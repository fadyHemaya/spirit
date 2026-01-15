package throttler

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// DeltaLenGetter is an interface for getting the current binlog delta length.
// This is typically implemented by the replication client.
type DeltaLenGetter interface {
	GetDeltaLen() int
}

// Flusher is an interface for triggering a binlog flush.
type Flusher interface {
	Flush(ctx context.Context) error
}

// BinlogFlusher combines both interfaces - typically the replication client implements both.
type BinlogFlusher interface {
	DeltaLenGetter
	Flusher
}

// BinlogThrottler throttles the copier when binlog deltas exceed a threshold.
// When throttled, it pauses the copier and actively flushes binlog changes.
type BinlogThrottler struct {
	replClient     BinlogFlusher
	highWatermark  int           // Pause copier when deltas exceed this
	lowWatermark   int           // Resume copier when deltas fall below this
	checkInterval  time.Duration // How often to check delta count
	logger         *slog.Logger

	mu         sync.RWMutex
	throttled  bool
	flushing   bool
	ctx        context.Context
	cancelFunc context.CancelFunc
}

var _ Throttler = &BinlogThrottler{}

// NewBinlogThrottler creates a new BinlogThrottler.
// highWatermark: pause copier when deltas exceed this (e.g., 500000)
// lowWatermark: resume copier when deltas fall below this (e.g., 100000)
func NewBinlogThrottler(replClient BinlogFlusher, highWatermark, lowWatermark int, logger *slog.Logger) *BinlogThrottler {
	return &BinlogThrottler{
		replClient:    replClient,
		highWatermark: highWatermark,
		lowWatermark:  lowWatermark,
		checkInterval: 1 * time.Second,
		logger:        logger,
	}
}

func (t *BinlogThrottler) Open(ctx context.Context) error {
	t.ctx, t.cancelFunc = context.WithCancel(ctx)
	go t.monitor()
	return nil
}

func (t *BinlogThrottler) Close() error {
	if t.cancelFunc != nil {
		t.cancelFunc()
	}
	return nil
}

func (t *BinlogThrottler) IsThrottled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.throttled
}

// BlockWait blocks until the throttle is released (deltas below low watermark).
func (t *BinlogThrottler) BlockWait() {
	for t.IsThrottled() {
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
			// Check again
		}
	}
}

func (t *BinlogThrottler) UpdateLag() error {
	// Not used for binlog throttling
	return nil
}

// monitor runs in the background and updates the throttled state.
func (t *BinlogThrottler) monitor() {
	ticker := time.NewTicker(t.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.updateThrottleState()
		}
	}
}

func (t *BinlogThrottler) updateThrottleState() {
	deltaLen := t.replClient.GetDeltaLen()

	t.mu.Lock()
	wasThrottled := t.throttled
	isFlushing := t.flushing

	if !t.throttled && deltaLen > t.highWatermark {
		t.throttled = true
		t.logger.Warn("binlog deltas exceeded high watermark, pausing copier to flush",
			"deltas", deltaLen,
			"high_watermark", t.highWatermark,
		)
	} else if t.throttled && deltaLen < t.lowWatermark {
		t.throttled = false
		t.flushing = false
		t.logger.Info("binlog deltas below low watermark, resuming copier",
			"deltas", deltaLen,
			"low_watermark", t.lowWatermark,
		)
	}

	// If we just became throttled or are throttled and not flushing, start a flush
	shouldFlush := t.throttled && !isFlushing
	if shouldFlush {
		t.flushing = true
	}
	t.mu.Unlock()

	// Trigger flush outside the lock to avoid deadlock
	if shouldFlush {
		go t.doFlush()
	}

	// Log periodic status when throttled
	if t.throttled && wasThrottled {
		t.logger.Info("copier paused, flushing binlog",
			"deltas", deltaLen,
			"low_watermark", t.lowWatermark,
		)
	}
}

// doFlush performs the actual flush operation.
func (t *BinlogThrottler) doFlush() {
	t.logger.Info("starting binlog flush while copier is paused")
	startTime := time.Now()
	deltasBefore := t.replClient.GetDeltaLen()

	if err := t.replClient.Flush(t.ctx); err != nil {
		t.logger.Error("error during binlog flush", "error", err)
	}

	deltasAfter := t.replClient.GetDeltaLen()
	t.logger.Info("binlog flush completed",
		"duration", time.Since(startTime),
		"deltas_before", deltasBefore,
		"deltas_after", deltasAfter,
		"deltas_flushed", deltasBefore-deltasAfter,
	)

	t.mu.Lock()
	t.flushing = false
	t.mu.Unlock()
}

