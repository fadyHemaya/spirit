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

// BinlogThrottler throttles the copier when binlog deltas exceed a threshold.
// When throttled, it waits for the deltas to be flushed before allowing the copier to continue.
type BinlogThrottler struct {
	deltaGetter    DeltaLenGetter
	highWatermark  int           // Pause copier when deltas exceed this
	lowWatermark   int           // Resume copier when deltas fall below this
	checkInterval  time.Duration // How often to check delta count
	logger         *slog.Logger

	mu         sync.RWMutex
	throttled  bool
	ctx        context.Context
	cancelFunc context.CancelFunc
}

var _ Throttler = &BinlogThrottler{}

// NewBinlogThrottler creates a new BinlogThrottler.
// highWatermark: pause copier when deltas exceed this (e.g., 500000)
// lowWatermark: resume copier when deltas fall below this (e.g., 100000)
func NewBinlogThrottler(deltaGetter DeltaLenGetter, highWatermark, lowWatermark int, logger *slog.Logger) *BinlogThrottler {
	return &BinlogThrottler{
		deltaGetter:   deltaGetter,
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
	deltaLen := t.deltaGetter.GetDeltaLen()

	t.mu.Lock()
	defer t.mu.Unlock()

	wasThrottled := t.throttled

	if !t.throttled && deltaLen > t.highWatermark {
		t.throttled = true
		t.logger.Warn("binlog deltas exceeded high watermark, pausing copier",
			"deltas", deltaLen,
			"high_watermark", t.highWatermark,
		)
	} else if t.throttled && deltaLen < t.lowWatermark {
		t.throttled = false
		t.logger.Info("binlog deltas below low watermark, resuming copier",
			"deltas", deltaLen,
			"low_watermark", t.lowWatermark,
		)
	}

	// Log periodic status when throttled
	if t.throttled && wasThrottled {
		t.logger.Info("copier still paused waiting for binlog flush",
			"deltas", deltaLen,
			"low_watermark", t.lowWatermark,
		)
	}
}

