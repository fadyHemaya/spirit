package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/spirit/pkg/table"
)

// logExistingLocks queries performance_schema to log what locks currently exist on the tables
func logExistingLocks(ctx context.Context, db *sql.DB, tables []*table.TableInfo, logger *slog.Logger) {
	// Build table names for the query
	tableNames := make([]string, 0, len(tables)*2)
	for _, tbl := range tables {
		tableNames = append(tableNames, tbl.TableName)
		tableNames = append(tableNames, "_"+tbl.TableName+"_new")
	}

	// Query metadata locks
	query := `
		SELECT 
			ml.OBJECT_NAME,
			ml.LOCK_TYPE,
			ml.LOCK_STATUS,
			ml.LOCK_DURATION,
			t.PROCESSLIST_ID,
			t.PROCESSLIST_USER,
			t.PROCESSLIST_TIME,
			LEFT(t.PROCESSLIST_INFO, 100) as query_snippet,
			t.NAME as thread_name,
			t.TYPE as thread_type
		FROM performance_schema.metadata_locks ml
		LEFT JOIN performance_schema.threads t ON ml.owner_thread_id = t.thread_id
		WHERE ml.OBJECT_SCHEMA = DATABASE()
		ORDER BY t.PROCESSLIST_TIME DESC
		LIMIT 50
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		logger.Warn("failed to query existing locks", "error", err)
		return
	}
	defer rows.Close()

	lockCount := 0
	for rows.Next() {
		var objectName, lockType, lockStatus, lockDuration sql.NullString
		var processlistID sql.NullInt64
		var processlistUser, processlistTime, querySnippet, threadName, threadType sql.NullString

		if err := rows.Scan(
			&objectName, &lockType, &lockStatus, &lockDuration,
			&processlistID, &processlistUser, &processlistTime, &querySnippet,
			&threadName, &threadType,
		); err != nil {
			logger.Warn("failed to scan lock row", "error", err)
			continue
		}

		// Check if this lock is on one of our tables
		isRelevant := false
		for _, name := range tableNames {
			if objectName.Valid && objectName.String == name {
				isRelevant = true
				break
			}
		}

		if isRelevant {
			lockCount++
			logger.Info("existing lock on table",
				"object_name", objectName.String,
				"lock_type", lockType.String,
				"lock_status", lockStatus.String,
				"lock_duration", lockDuration.String,
				"processlist_id", fmt.Sprintf("%v", processlistID),
				"user", processlistUser.String,
				"time", processlistTime.String,
				"query", querySnippet.String,
				"thread_name", threadName.String,
				"thread_type", threadType.String,
			)
		}
	}
	logger.Info("finished logging existing locks", "relevant_lock_count", lockCount)
}

type TableLock struct {
	tables  []*table.TableInfo
	lockTxn *sql.Tx
	logger  *slog.Logger
}

// NewTableLock creates a new server wide lock on multiple tables.
// i.e. LOCK TABLES .. WRITE.
// It uses a short timeout and *does not retry*. The caller is expected to retry,
// which gives it a chance to first do things like catch up on replication apply
// before it does the next attempt.
//
// Setting config.ForceKill=true is recommended, since it will more or less ensure
// that the lock acquisition is successful by killing long-running queries that are
// blocking our lock acquisition after we have waited for 90% of our configured
// LockWaitTimeout.
func NewTableLock(ctx context.Context, db *sql.DB, tables []*table.TableInfo, config *DBConfig, logger *slog.Logger) (*TableLock, error) {
	var err error
	var lockTxn *sql.Tx
	var lockStmt = "LOCK TABLES "
	// Build the LOCK TABLES statement
	for idx, tbl := range tables {
		if idx > 0 {
			lockStmt += ", "
		}
		lockStmt += "`" + tbl.TableName + "` WRITE"
	}

	// Log current database connection stats before attempting lock
	stats := db.Stats()
	logger.Info("database connection stats before lock attempt",
		"open_connections", stats.OpenConnections,
		"in_use", stats.InUse,
		"idle", stats.Idle,
		"wait_count", stats.WaitCount,
		"max_open", stats.MaxOpenConnections,
	)

	// Try and acquire the lock. No retries are permitted here.
	lockTxn, pid, err := BeginStandardTrx(ctx, db, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Before we return an error, we need to now ensure that
		// we rollback the transaction if it was opened,
		// this helps prevent a connection leak.
		if err != nil {
			_ = lockTxn.Rollback()
		}
	}()

	// Log what locks currently exist on the tables we're trying to lock
	logger.Info("checking existing locks before acquiring table lock")
	logExistingLocks(ctx, db, tables, logger)

	if config.ForceKill {
		// If ForceKill is true, we will wait for 90% of the configured LockWaitTimeout
		threshold := time.Duration(float64(config.LockWaitTimeout)*lockWaitTimeoutForceKillMultiplier) * time.Second
		logger.Info("force-kill enabled, will attempt to kill blockers after threshold",
			"threshold_seconds", threshold.Seconds(),
			"lock_wait_timeout", config.LockWaitTimeout,
		)
		timer := time.AfterFunc(threshold, func() {
			logger.Warn("threshold reached, attempting to kill locking transactions")
			logExistingLocks(ctx, db, tables, logger)
			err := KillLockingTransactions(ctx, db, tables, config, logger, []int{pid})
			if err != nil {
				logger.Error("failed to kill locking transactions", "error", err)
			}
		})
		defer timer.Stop()
	}

	// We need to lock all the tables we intend to write to while we have the lock.
	// For each table, we need to lock both the main table and its _new table.
	logger.Warn("trying to acquire table locks", "timeout", config.LockWaitTimeout, "statement", lockStmt)
	_, err = lockTxn.ExecContext(ctx, lockStmt)
	if err != nil {
		logger.Error("failed to acquire table lock(s)",
			"error", err,
			"statement", lockStmt,
		)
		// Log what's blocking us
		logger.Error("logging locks that may have blocked us")
		logExistingLocks(ctx, db, tables, logger)
		return nil, err
	}

	// Otherwise we are successful, we still log because
	// it's a critical function.
	logger.Warn("table lock(s) acquired")
	return &TableLock{
		tables:  tables,
		lockTxn: lockTxn,
		logger:  logger,
	}, nil
}

// ExecUnderLock executes a set of statements under a table lock.
func (s *TableLock) ExecUnderLock(ctx context.Context, stmts ...string) error {
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		_, err := s.lockTxn.ExecContext(ctx, stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes the table lock
func (s *TableLock) Close() error {
	_, err := s.lockTxn.Exec("UNLOCK TABLES")
	if err != nil {
		return err
	}
	err = s.lockTxn.Rollback()
	if err != nil {
		return err
	}
	s.logger.Warn("table lock released")
	return nil
}
