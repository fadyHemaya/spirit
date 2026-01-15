package copier

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

// The buffered copier implements a producer/consumer pattern
// where multiple reader goroutines read chunks from the source table,
// and then send them to an Applier which breaks them into chunklets.
// It closely matches the DBLog algorithm:
// https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b

type buffered struct {
	sync.Mutex

	db               *sql.DB
	applier          applier.Applier
	chunker          table.Chunker
	concurrency      int
	rowsPerSecond    uint64
	isInvalid        bool
	startTime        time.Time
	throttler        throttler.Throttler
	dbConfig         *dbconn.DBConfig
	logger           *slog.Logger
	metricsSink      metrics.Sink
	copierEtaHistory *copierEtaHistory
}

// Assert that buffered implements the Copier interface
var _ Copier = (*buffered)(nil)

// readChunkData reads all rows from a chunk into memory
func (c *buffered) readChunkData(ctx context.Context, chunk *table.Chunk) ([][]any, error) {
	// Build the SELECT query to read full row data
	columnList := utils.IntersectNonGeneratedColumns(chunk.Table, chunk.NewTable)
	query := fmt.Sprintf("SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE %s",
		columnList,
		chunk.Table.QuotedName,
		chunk.String(),
	)

	c.logger.Debug("reading chunk data", "chunk", chunk.String(), "query", query)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query chunk data: %w", err)
	}
	defer rows.Close()

	// Get column count for scanning
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var rowDataList [][]any
	for rows.Next() {
		// Create slice to hold the row values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowDataList = append(rowDataList, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	c.logger.Debug("read rows from chunk", "rowCount", len(rowDataList), "chunk", chunk.String())
	return rowDataList, nil
}

func (c *buffered) isHealthy(ctx context.Context) bool {
	c.Lock()
	defer c.Unlock()
	if ctx.Err() != nil {
		return false
	}
	return !c.isInvalid
}

func (c *buffered) StartTime() time.Time {
	c.Lock()
	defer c.Unlock()
	return c.startTime
}

func (c *buffered) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.startTime = time.Now()
	// Note: throttler.Open() is called by the runner, not the copier,
	// so the socket is available throughout the entire migration lifecycle.
	go c.estimateRowsPerSecondLoop(ctx) // estimate rows while copying

	// Start the applier
	if err := c.applier.Start(ctx); err != nil {
		return fmt.Errorf("failed to start applier: %w", err)
	}

	// Start read workers
	g, errGrpCtx := errgroup.WithContext(ctx)
	c.logger.Info("starting read workers", "count", c.concurrency)
	for range c.concurrency {
		g.Go(func() error {
			return c.readWorker(errGrpCtx)
		})
	}

	// Wait for all read workers to finish
	err := g.Wait()

	// Wait for the applier to finish processing all pending work
	// This ensures all callbacks have been invoked before we return
	if waitErr := c.applier.Wait(ctx); waitErr != nil && err == nil {
		err = waitErr
	}

	// "Stop" the applier. This will free up the gouroutines that
	// were used for copying, but it won't close the DB connections
	if closeErr := c.applier.Stop(); closeErr != nil && err == nil {
		err = closeErr
	}

	return err
}

// readWorker reads chunks and sends them to the applier
func (c *buffered) readWorker(ctx context.Context) error {
	c.logger.Debug("readWorker started", "isRead", c.chunker.IsRead())

	for !c.chunker.IsRead() && c.isHealthy(ctx) {
		c.throttler.BlockWait()

		c.logger.Debug("readWorker calling chunker.Next()")
		chunk, err := c.chunker.Next()
		if err != nil {
			if err == table.ErrTableIsRead {
				c.logger.Debug("readWorker table is read, exiting")
				return nil
			}
			c.logger.Error("readWorker got error from chunker", "error", err)
			c.setInvalid()
			return err
		}
		c.logger.Debug("readWorker got chunk", "chunk", chunk.String())

		readStart := time.Now()
		rows, err := c.readChunkData(ctx, chunk)
		if err != nil {
			c.setInvalid()
			return fmt.Errorf("failed to read chunk data: %w", err)
		}
		readTime := time.Since(readStart)

		// Handle empty chunks immediately
		if len(rows) == 0 {
			c.logger.Debug("readWorker chunk is empty, sending immediate feedback", "chunk", chunk.String())
			c.chunker.Feedback(chunk, readTime, 0)

			// Send metrics for empty chunk
			err := c.sendMetrics(ctx, readTime, chunk.ChunkSize, 0)
			if err != nil {
				c.logger.Error("error sending metrics for empty chunk", "error", err)
			}
			continue
		}

		c.logger.Debug("readWorker sending rows to applier", "chunk", chunk.String(), "rowCount", len(rows))

		// Send rows to applier with callback
		// The callback will be invoked when all rows are safely flushed
		callback := func(affectedRows int64, err error) {
			if err != nil {
				c.logger.Error("applier callback received error", "chunk", chunk.String(), "error", err)
				c.setInvalid()
				return
			}

			c.logger.Debug("applier callback invoked", "chunk", chunk.String(), "affectedRows", affectedRows)

			// Send feedback to chunker
			c.chunker.Feedback(chunk, readTime, uint64(affectedRows))

			// Send metrics
			metricsErr := c.sendMetrics(ctx, readTime, chunk.ChunkSize, uint64(affectedRows))
			if metricsErr != nil {
				c.logger.Error("error sending metrics from copier", "error", metricsErr)
			}
		}

		// Apply the rows
		if err := c.applier.Apply(ctx, chunk, rows, callback); err != nil {
			c.setInvalid()
			return fmt.Errorf("failed to apply rows: %w", err)
		}
	}

	c.logger.Info("readWorker exiting main loop")
	return nil
}

func (c *buffered) setInvalid() {
	c.Lock()
	defer c.Unlock()
	c.isInvalid = true
}

func (c *buffered) SetThrottler(throttler throttler.Throttler) {
	c.Lock()
	defer c.Unlock()
	c.throttler = throttler
}

func (c *buffered) getCopyStats() (uint64, uint64, float64) {
	// Get progress from the chunker instead of calculating it ourselves
	rowsProcessed, _, totalRows := c.chunker.Progress()

	// Calculate percentage
	pct := float64(0)
	if totalRows > 0 {
		pct = float64(rowsProcessed) / float64(totalRows) * 100
	}

	return rowsProcessed, totalRows, pct
}

// GetProgress returns the progress of the copier
func (c *buffered) GetProgress() string {
	c.Lock()
	defer c.Unlock()
	copied, total, pct := c.getCopyStats()
	return fmt.Sprintf("%d/%d %.2f%%", copied, total, pct)
}

func (c *buffered) GetETA() string {
	c.Lock()
	defer c.Unlock()
	copiedRows, totalRows, pct := c.getCopyStats()
	rowsPerSecond := atomic.LoadUint64(&c.rowsPerSecond)
	if pct > 99.99 {
		return "DUE"
	}
	if rowsPerSecond == 0 || time.Since(c.startTime) < copyETAInitialWaitTime {
		return "TBD"
	}
	// divide the remaining rows by how many rows we copied in the last interval per second
	// "remainingRows" might be the actual rows or the logical rows since
	// c.getCopyStats() and rowsPerSecond change estimation method when the PK is auto-inc.
	remainingRows := totalRows - copiedRows
	remainingSeconds := math.Floor(float64(remainingRows) / float64(rowsPerSecond))

	estimate := time.Duration(remainingSeconds * float64(time.Second))
	comparison := c.copierEtaHistory.addCurrentEstimateAndCompare(estimate)
	if comparison != "" {
		return fmt.Sprintf("%s (%s)", estimate.String(), comparison)
	}
	return estimate.String()
}

func (c *buffered) estimateRowsPerSecondLoop(ctx context.Context) {
	// We take >10 second averages because with parallel copy it bounces around a lot.
	// Get progress from chunker since we no longer track rows locally
	prevRowsCount, _, _ := c.chunker.Progress()
	ticker := time.NewTicker(copyEstimateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.isHealthy(ctx) {
				return
			}
			newRowsCount, _, _ := c.chunker.Progress()
			rowsPerInterval := float64(newRowsCount - prevRowsCount)
			intervalsDivisor := float64(copyEstimateInterval / time.Second) // should be something like 10 for 10 seconds
			rowsPerSecond := uint64(rowsPerInterval / intervalsDivisor)
			atomic.StoreUint64(&c.rowsPerSecond, rowsPerSecond)
			prevRowsCount = newRowsCount
		}
	}
}

func (c *buffered) sendMetrics(ctx context.Context, processingTime time.Duration, logicalRowsCount uint64, affectedRowsCount uint64) error {
	m := &metrics.Metrics{
		Values: []metrics.MetricValue{
			{
				Name:  metrics.ChunkProcessingTimeMetricName,
				Type:  metrics.GAUGE,
				Value: float64(processingTime.Milliseconds()), // in milliseconds
			},
			{
				Name:  metrics.ChunkLogicalRowsCountMetricName,
				Type:  metrics.COUNTER,
				Value: float64(logicalRowsCount),
			},
			{
				Name:  metrics.ChunkAffectedRowsCountMetricName,
				Type:  metrics.COUNTER,
				Value: float64(affectedRowsCount),
			},
		},
	}

	contextWithTimeout, cancel := context.WithTimeout(ctx, metrics.SinkTimeout)
	defer cancel()

	return c.metricsSink.Send(contextWithTimeout, m)
}

// GetChunker returns the chunker for accessing progress information
func (c *buffered) GetChunker() table.Chunker {
	return c.chunker
}

func (c *buffered) GetThrottler() throttler.Throttler {
	c.Lock()
	defer c.Unlock()
	return c.throttler
}
