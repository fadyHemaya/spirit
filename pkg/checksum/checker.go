// Package checksum provides online checksum functionality.
// Two tables on the same MySQL server can be compared with only an initial lock.
// It is not in the row/ package because it requires a replClient to be passed in,
// which would cause a circular dependency.
package checksum

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type SingleChecker struct {
	sync.Mutex
	concurrency      int
	feed             *repl.Client
	db               *sql.DB
	trxPool          *dbconn.TrxPool // reader trx pool
	isInvalid        bool
	chunker          table.Chunker
	startTime        time.Time
	execTime         time.Duration
	dbConfig         *dbconn.DBConfig
	logger           *slog.Logger
	fixDifferences   bool
	differencesFound atomic.Uint64
	recopyLock       sync.Mutex
	maxRetries       int
	sampleRate       int    // Sample every Nth chunk (0 = disabled, check all chunks)
	watermarkDate    string // Only checksum rows with created_at >= this date (YYYY-MM-DD)
	watermarkID      string // Only checksum rows with id >= this value
}

var _ Checker = (*SingleChecker)(nil)

func (c *SingleChecker) ChecksumChunk(ctx context.Context, trxPool *dbconn.TrxPool, chunk *table.Chunk) error {
	startTime := time.Now()
	trx, err := trxPool.Get()
	if err != nil {
		return err
	}
	defer trxPool.Put(trx)

	// If watermark ID is set, check if this chunk should be skipped (fast - no query)
	if c.watermarkID != "" {
		shouldSkip := c.shouldSkipChunkByID(chunk)
		if shouldSkip {
			c.logger.Debug("skipping chunk (all rows before watermark ID)", "chunk", chunk.String())
			c.chunker.Feedback(chunk, 0, 0)
			return nil
		}
	}

	// If watermark date is set, check if this chunk should be skipped (slower - requires query)
	if c.watermarkDate != "" {
		shouldSkip, err := c.shouldSkipChunkByDate(trx, chunk)
		if err != nil {
			c.logger.Warn("error checking watermark date, processing chunk anyway", "error", err)
		} else if shouldSkip {
			c.logger.Debug("skipping chunk (all rows before watermark date)", "chunk", chunk.String())
			c.chunker.Feedback(chunk, 0, 0)
			return nil
		}
	}

	c.logger.Debug("checksumming chunk", "chunk", chunk.String())
	source := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
		c.intersectColumns(chunk),
		chunk.Table.QuotedName,
		chunk.String(),
	)
	target := fmt.Sprintf("SELECT BIT_XOR(CRC32(CONCAT(%s))) as checksum, count(*) as c FROM %s WHERE %s",
		c.intersectColumns(chunk),
		chunk.NewTable.QuotedName,
		chunk.String(),
	)
	var sourceChecksum, targetChecksum int64
	var sourceCount, targetCount uint64
	err = trx.QueryRow(source).Scan(&sourceChecksum, &sourceCount)
	if err != nil {
		return err
	}
	err = trx.QueryRow(target).Scan(&targetChecksum, &targetCount)
	if err != nil {
		return err
	}
	if sourceChecksum != targetChecksum {
		// The checksums do not match, so we first need
		// to inspect closely and report on the differences.
		c.differencesFound.Add(1)
		c.logger.Warn("checksum mismatch for chunk", "chunk", chunk.String(), "sourceChecksum", sourceChecksum, "targetChecksum", targetChecksum, "sourceCount", sourceCount, "targetCount", targetCount)
		if err := c.inspectDifferences(trx, chunk); err != nil {
			return err
		}
		// Are we allowed to fix the differences? If not, return an error.
		// This is mostly used by the test-suite.
		if !c.fixDifferences {
			return errors.New("checksum mismatch")
		}
		// Since we can fix differences, replace the chunk.
		if err = c.replaceChunk(ctx, chunk); err != nil {
			return err
		}
	}
	// When we give feedback, we need to say how many rows were in the chunk.
	c.chunker.Feedback(chunk, time.Since(startTime), targetCount)
	return nil
}

// GetProgress returns the progress of the checker
// this is really just a proxy to the chunker progress.
func (c *SingleChecker) GetProgress() string {
	rowsProcessed, _, totalRows := c.chunker.Progress()
	pct := float64(0)
	if totalRows > 0 {
		pct = float64(rowsProcessed) / float64(totalRows) * 100
	}
	return fmt.Sprintf("%d/%d %.2f%%", rowsProcessed, totalRows, pct)
}

// inspectDifferences looks at the chunk and tries to find differences.
// For cross-database scenarios, it queries each database separately and compares in memory.
func (c *SingleChecker) inspectDifferences(trx *sql.Tx, chunk *table.Chunk) error {
	c.logger.Info("inspecting differences for chunk", "chunk", chunk.String())

	sourceRows, err := trx.Query(fmt.Sprintf(queryTemplate,
		c.intersectColumns(chunk),
		strings.Join(chunk.Table.KeyColumns, ", "),
		chunk.Table.QuotedName,
		chunk.String(),
	))
	if err != nil {
		return fmt.Errorf("failed to query source rows: %w", err)
	}
	defer sourceRows.Close()

	// Build map of source checksums
	sourceChecksums := make(map[string]string) // pk -> checksum
	for sourceRows.Next() {
		var checksum, pk string
		if err := sourceRows.Scan(&checksum, &pk); err != nil {
			return fmt.Errorf("failed to scan source row: %w", err)
		}
		sourceChecksums[pk] = checksum
	}
	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("error iterating source rows: %w", err)
	}

	targetRows, err := trx.Query(fmt.Sprintf(queryTemplate,
		c.intersectColumns(chunk),
		strings.Join(chunk.NewTable.KeyColumns, ", "),
		chunk.NewTable.QuotedName,
		chunk.String(),
	))
	if err != nil {
		return fmt.Errorf("failed to query target rows: %w", err)
	}
	defer targetRows.Close()

	// Build map of target checksums and compare
	targetChecksums := make(map[string]string) // pk -> checksum
	for targetRows.Next() {
		var checksum, pk string
		if err := targetRows.Scan(&checksum, &pk); err != nil {
			return fmt.Errorf("failed to scan target row: %w", err)
		}
		targetChecksums[pk] = checksum

		// Check if this row exists in source and has different checksum
		if sourceChecksum, exists := sourceChecksums[pk]; exists {
			if sourceChecksum != checksum {
				c.logger.Warn("inspection revealed row checksum mismatch", "pk", pk, "sourceChecksum", sourceChecksum, "targetChecksum", checksum)
			}
		} else {
			c.logger.Warn("inspection revealed row does not exist in source", "pk", pk)
		}
	}
	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("error iterating target rows: %w", err)
	}

	// Check for rows that exist in source but not in target
	for pk, sourceChecksum := range sourceChecksums {
		if _, exists := targetChecksums[pk]; !exists {
			c.logger.Warn("inspection revealed row does not exist in target", "pk", pk, "sourceChecksum", sourceChecksum)
		}
	}

	return nil // managed to inspect differences
}

// replaceChunk recopies the data from table to newTable for a given chunk.
// For cross-database operations, this reads the data from the source and writes it to the target.
// Note that the chunk is dynamically sized based on the target-time that it took
// to *read* data in the checksum. This could be substantially longer than the time
// that it takes to copy the data. Maybe in future we could consider splitting
// the chunk here, but this is expected to be a very rare situation, so a small
// stall from an XL sized chunk is considered acceptable.
func (c *SingleChecker) replaceChunk(ctx context.Context, chunk *table.Chunk) error {
	c.logger.Warn("recopying chunk", "chunk", chunk.String())

	// We further prevent the chance of deadlocks from the recopying process by only re-copying one chunk at a time.
	// We may revisit this in future, but since conflicts are expected to be low, it should be fine for now.
	c.recopyLock.Lock()
	defer c.recopyLock.Unlock()

	// Construct a delete statement to remove existing rows in the target chunk
	deleteStmt := "DELETE FROM " + chunk.NewTable.QuotedName + " WHERE " + chunk.String()

	// Within the same database we use a REPLACE INTO .. SELECT approach.
	// Within database we also support intersecting columns (i.e.
	// there might be a schema change/transformation that applies).
	//
	// Note: historically this process has caused deadlocks between the DELETE statement
	// in one replaceChunk and the REPLACE statement of another chunk. Inspection of
	// SHOW ENGINE INNODB STATUS shows that this is not caused by locks on the PRIMARY KEY,
	// but a unique secondary key (in our case an idempotence key):
	//
	// ------------------------
	// LATEST DETECTED DEADLOCK
	// ------------------------
	// 2024-06-11 18:34:21 70676106989440
	// *** (1) TRANSACTION:
	// TRANSACTION 15106308424, ACTIVE 4 sec updating or deleting
	// mysql tables in use 1, locked 1
	// LOCK WAIT 620 lock struct(s), heap size 73848, 49663 row lock(s), undo log entries 49661
	// MySQL thread id 540806, OS thread handle 70369444421504, query id 409280999 10.137.84.232 <snip> updating
	// DELETE FROM `<snip>`.`_<snip>_new` WHERE `id` >= 1108588365 AND `id` < 1108688365
	//
	// *** (1) HOLDS THE LOCK(S):
	// RECORD LOCKS space id 1802 page no 26277057 n bits 232 index idempotence_key_idx of table `<snip>`.`_<snip>_new` trx id 15106308424 lock_mode X locks rec but not gap
	// Record lock, heap no 163 PHYSICAL RECORD: n_fields 2; compact format; info bits 32
	// 0: len 30; hex <snip>; asc <snip>; (total 62 bytes);
	// 1: len 8; hex <snip>; asc     <snip>;;
	//
	//
	// *** (1) WAITING FOR THIS LOCK TO BE GRANTED:
	// RECORD LOCKS space id 1802 page no 1945840 n bits 280 index idempotence_key_idx of table `<snip>`.`_<snip>_new` trx id 15106308424 lock_mode X locks rec but not gap waiting
	// Record lock, heap no 75 PHYSICAL RECORD: n_fields 2; compact format; info bits 0
	// 0: len 30; hex <snip>; asc <snip>; (total 62 bytes);
	// 1: len 8; hex <snip>; asc     <snip>;;
	//
	//
	// *** (2) TRANSACTION:
	// TRANSACTION 15106301192, ACTIVE 58 sec inserting
	// mysql tables in use 2, locked 1
	// LOCK WAIT 220020 lock struct(s), heap size 27680888, 409429 row lock(s), undo log entries 162834
	// MySQL thread id 540264, OS thread handle 70369485823872, query id 409127061 10.137.84.232 <snip> executing
	// REPLACE INTO `<snip>`.`_<snip>_new` (`id`, <snip> FROM `<snip>`.`<snip>` WHERE `id` >= 1106488365 AND `id` < 1106588365
	//
	// *** (2) HOLDS THE LOCK(S):
	// RECORD LOCKS space id 1802 page no 1945840 n bits 280 index idempotence_key_idx of table `<snip>`.`_<snip>_new` trx id 15106301192 lock_mode X
	// Record lock, heap no 75 PHYSICAL RECORD: n_fields 2; compact format; info bits 0
	// 0: len 30; hex <snip>; asc <snip>; (total 62 bytes);
	// 1: len 8; hex <snip>; asc     B yI;;
	//
	//
	// *** (2) WAITING FOR THIS LOCK TO BE GRANTED:
	// RECORD LOCKS space id 1802 page no 26277057 n bits 232 index idempotence_key_idx of table `<snip>`.`_<snip>_new` trx id 15106301192 lock_mode X waiting
	// Record lock, heap no 163 PHYSICAL RECORD: n_fields 2; compact format; info bits 32
	// 0: len 30; hex <snip>; asc <snip>; (total 62 bytes);
	// 1: len 8; hex <snip>; asc     <snip>;;
	//
	// *** WE ROLL BACK TRANSACTION (1)
	//
	// We don't need this to be an atomic transaction. We just need to delete from the _new table
	// first so that any since-deleted rows (which wouldn't get removed by replace) are removed first.
	// By doing this as two transactions we should be able to remove
	// the opportunity for deadlocks.
	replaceStmt := fmt.Sprintf("REPLACE INTO %s (%s) SELECT %s FROM %s WHERE %s",
		chunk.NewTable.QuotedName,
		utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable),
		utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable),
		chunk.Table.QuotedName,
		chunk.String(),
	)
	if _, err := dbconn.RetryableTransaction(ctx, c.db, false, c.dbConfig, deleteStmt); err != nil {
		return fmt.Errorf("failed to delete existing rows: %w", err)
	}
	if _, err := dbconn.RetryableTransaction(ctx, c.db, false, c.dbConfig, replaceStmt); err != nil {
		return fmt.Errorf("failed to replace chunk data: %w", err)
	}
	return nil
}

func (c *SingleChecker) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid
}

func (c *SingleChecker) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *SingleChecker) ExecTime() time.Duration {
	c.Lock()
	defer c.Unlock()
	return c.execTime
}

func (c *SingleChecker) setInvalid(newVal bool) {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = newVal
}

func (c *SingleChecker) initConnPool(ctx context.Context) error {
	// Try and catch up before we apply a table lock,
	// since we will need to catch up again with the lock held
	// and we want to minimize that.
	if err := c.feed.Flush(ctx); err != nil {
		return err
	}
	
	// Wait for all database connections to become idle before acquiring the lock.
	// This prevents our own flush operations from blocking our lock acquisition.
	c.logger.Info("waiting for all connections to become idle before acquiring table lock")
	if err := c.waitForIdleConnections(ctx, 60*time.Second); err != nil {
		c.logger.Warn("timeout waiting for idle connections, proceeding anyway", "error", err)
	}
	
	// Lock the source and target table in a trx
	// so the connection is not used by others
	c.logger.Info("starting checksum operation, this will require a table lock")

	// Always acquire lock on the read database
	tableLock, err := dbconn.NewTableLock(ctx, c.db, c.chunker.Tables(), c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer tableLock.Close()
	// We only have a reader, so flush the read connection.
	if err := c.feed.FlushUnderTableLock(ctx, tableLock); err != nil {
		return err
	}

	// Assert that the change set is empty. This should always
	// be the case because we are under a lock.
	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed")
	}
	// Create a set of connections which can be used to checksum
	// The table. They MUST be created before the lock is released
	// with REPEATABLE-READ and a consistent snapshot (or dummy read)
	// to initialize the read-view.
	c.trxPool, err = dbconn.NewTrxPool(ctx, c.db, c.concurrency, c.dbConfig)
	if err != nil {
		return err
	}

	return nil
}

func (c *SingleChecker) Run(ctx context.Context) error {
	// Set startTime under lock to prevent race with StartTime() method
	c.Lock()
	c.startTime = time.Now()
	startTime := c.startTime // capture for defer
	c.Unlock()

	defer func() {
		c.execTime = time.Since(startTime)
	}()

	// Try the checksum up to n times if differences are found and we can fix them
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		if attempt > 1 {
			c.logger.Error("checksum failed, retrying", "attempt", attempt, "maxRetries", c.maxRetries)
			// Reset the chunker to start from the beginning
			if err := c.chunker.Reset(); err != nil {
				return fmt.Errorf("failed to reset chunker for retry: %w", err)
			}
			// Reset differences found counter
			c.differencesFound.Store(0)
		}

		// Run the actual checksum
		if err := c.runChecksum(ctx); err != nil {
			// This is really not expected to fail, since if there are differences
			// it will run the resolver and report the differences in DifferencesFound().
			return err
		}

		// If we are here, the checksum passed.
		// But we don't know if differences were found and chunks were recopied.
		// We want to know it passed without finding differences.
		if c.differencesFound.Load() == 0 {
			c.logger.Info("checksum passed")
			return nil
		}
	}

	// Retries exhausted:
	// This used to say "checksum failed, this should never happen" but that's not entirely true.
	// If the user attempts a lossy schema change such as adding a UNIQUE INDEX to non-unique data,
	// then the checksum will fail. This is entirely expected, and not considered a bug. We should
	// do our best-case to differentiate that we believe this ALTER statement is lossy, and
	// customize the returned error based on it.
	return fmt.Errorf("checksum failed after %d attempts. This likely indicates either a bug in Spirit, or a manual modification to the _new table outside of Spirit. Please report @ github.com/block/spirit", c.maxRetries)
}

func (c *SingleChecker) runChecksum(ctx context.Context) error {
	// initConnPool initialize the connection pool.
	// This is done under a table lock which is acquired in this func.
	// It is released as the func is returned.
	if err := c.initConnPool(ctx); err != nil {
		return err
	}
	c.logger.Info("table unlocked, starting checksum")

	// Start the periodic flush again *just* for the duration of the checksum.
	// If the checksum is long running, it could block flushing for too long:
	// - If we need to resume from checkpoint, the binlogs may not be there.
	// - If they are there, they will take a huge amount of time to flush
	// - The memory requirements for 1MM deltas seems reasonable, but for a multi-day
	//   checksum it is reasonable to assume it may exceed this.
	go c.feed.StartPeriodicFlush(ctx, repl.DefaultFlushInterval)
	defer c.feed.StopPeriodicFlush()

	g, errGrpCtx := errgroup.WithContext(ctx)
	g.SetLimit(c.concurrency)
	chunkCounter := 0
	totalChunks := 0
	sampledChunks := 0
	skippedChunks := 0

	for !c.chunker.IsRead() && c.isHealthy(errGrpCtx) {
		chunk, err := c.chunker.Next()
		if err != nil {
			if err == table.ErrTableIsRead {
				break
			}
			c.setInvalid(true)
			return err
		}

		totalChunks++

		// If sampling is enabled and this chunk should be skipped
		if c.sampleRate > 0 && chunkCounter%c.sampleRate != 0 {
			// Skip this chunk - just provide feedback to mark it as processed
			c.chunker.Feedback(chunk, 0, 0)
			chunkCounter++
			skippedChunks++
			continue
		}

		chunkCounter++
		sampledChunks++

		// Process the chunk (either sampling is disabled or this chunk is sampled)
		g.Go(func() error {
			if err := c.ChecksumChunk(errGrpCtx, c.trxPool, chunk); err != nil {
				c.setInvalid(true)
				return err
			}
			return nil
		})
	}

	if c.sampleRate > 0 {
		c.logger.Info("checksum sampling complete",
			"total_chunks", totalChunks,
			"sampled_chunks", sampledChunks,
			"skipped_chunks", skippedChunks,
			"sample_rate", fmt.Sprintf("1/%d (%.1f%%)", c.sampleRate, 100.0/float64(c.sampleRate)),
		)
	}
	if c.watermarkID != "" {
		c.logger.Info("checksum watermark ID filtering applied",
			"watermark_id", c.watermarkID,
			"total_chunks", totalChunks,
			"note", "chunks with id < watermark skipped (no query needed)",
		)
	}
	if c.watermarkDate != "" {
		c.logger.Info("checksum watermark date filtering applied",
			"watermark_date", c.watermarkDate,
			"total_chunks", totalChunks,
			"note", "old chunks skipped (requires MAX(created_at) query per chunk)",
		)
	}
	// wait for all work to finish
	err1 := g.Wait()
	// Regardless of err state, we should attempt to rollback the transaction
	// in checksumTxns. They are likely holding metadata locks, which will block
	// further operations like cleanup or cut-over.
	if err := c.trxPool.Close(); err != nil {
		return err
	}
	if err1 != nil {
		c.logger.Error("checksum failed")
		return err1
	}
	return nil
}

// waitForIdleConnections waits until all database connections are idle
// or until the timeout is reached.
func (c *SingleChecker) waitForIdleConnections(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	checkInterval := 500 * time.Millisecond
	
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		
		stats := c.db.Stats()
		c.logger.Info("connection stats",
			"in_use", stats.InUse,
			"idle", stats.Idle,
			"open", stats.OpenConnections,
		)
		
		if stats.InUse == 0 {
			c.logger.Info("all connections are idle, ready to acquire lock")
			return nil
		}
		
		time.Sleep(checkInterval)
	}
	
	stats := c.db.Stats()
	return fmt.Errorf("timeout waiting for idle connections, still have %d in use", stats.InUse)
}

// shouldSkipChunkByID checks if all rows in a chunk are before the watermark ID.
// This is MUCH faster than shouldSkipChunkByDate because it doesn't require a database query.
// It just compares the chunk's upper bound with the watermark ID.
func (c *SingleChecker) shouldSkipChunkByID(chunk *table.Chunk) bool {
	// Parse the watermark ID
	watermarkID, err := strconv.ParseInt(c.watermarkID, 10, 64)
	if err != nil {
		c.logger.Warn("invalid watermark ID, ignoring", "watermark_id", c.watermarkID, "error", err)
		return false
	}

	// Get the chunk's upper bound for the first key (id)
	// Chunk format: id >= X AND id < Y
	if chunk.UpperBound != nil && len(chunk.UpperBound.Value) > 0 {
		upperBoundStr := fmt.Sprintf("%v", chunk.UpperBound.Value[0])
		upperBoundID, err := strconv.ParseInt(upperBoundStr, 10, 64)
		if err != nil {
			// Can't parse, don't skip
			return false
		}

		// If the chunk's highest ID is below the watermark, skip it
		if upperBoundID <= watermarkID {
			return true
		}
	}

	return false
}

// shouldSkipChunkByDate checks if all rows in a chunk are before the watermark date.
// Returns true if the chunk should be skipped (all rows are old), false if it should be checked.
func (c *SingleChecker) shouldSkipChunkByDate(trx *sql.Tx, chunk *table.Chunk) (bool, error) {
	// Query to check the max(created_at) in this chunk
	// If max(created_at) < watermark date, skip the entire chunk
	query := fmt.Sprintf("SELECT MAX(created_at) FROM %s WHERE %s",
		chunk.Table.QuotedName,
		chunk.String(),
	)

	var maxCreatedAtBytes []byte
	err := trx.QueryRow(query).Scan(&maxCreatedAtBytes)
	if err != nil {
		return false, err
	}

	// If there are no rows or all created_at are NULL, don't skip
	if maxCreatedAtBytes == nil || len(maxCreatedAtBytes) == 0 {
		return false, nil
	}

	// Parse the timestamp from bytes
	// MySQL returns timestamps in format: "2006-01-02 15:04:05" or "2006-01-02"
	maxCreatedAtStr := string(maxCreatedAtBytes)
	var maxCreatedAt time.Time

	// Try parsing with datetime format first
	maxCreatedAt, err = time.Parse("2006-01-02 15:04:05", maxCreatedAtStr)
	if err != nil {
		// Try parsing with date-only format
		maxCreatedAt, err = time.Parse("2006-01-02", maxCreatedAtStr)
		if err != nil {
			return false, fmt.Errorf("failed to parse created_at value '%s': %w", maxCreatedAtStr, err)
		}
	}

	// Parse the watermark date
	watermarkTime, err := time.Parse("2006-01-02", c.watermarkDate)
	if err != nil {
		return false, fmt.Errorf("invalid watermark date format (expected YYYY-MM-DD): %w", err)
	}

	// If the newest row in this chunk is before the watermark, skip it
	if maxCreatedAt.Before(watermarkTime) {
		return true, nil
	}

	return false, nil
}

// intersectColumns is similar to utils.IntersectColumns, but it
// wraps an IFNULL(), ISNULL() and cast operation around the columns.
// The cast is to c.newTable type.
func (c *SingleChecker) intersectColumns(chunk *table.Chunk) string {
	var intersection []string
	for _, col := range chunk.Table.NonGeneratedColumns {
		for _, col2 := range chunk.NewTable.NonGeneratedColumns {
			if col == col2 {
				// Column exists in both, so we add intersection wrapped in
				// IFNULL, ISNULL and CAST.
				intersection = append(intersection, "IFNULL("+chunk.NewTable.WrapCastType(col)+",''), ISNULL(`"+col+"`)")
			}
		}
	}
	return strings.Join(intersection, ", ")
}
