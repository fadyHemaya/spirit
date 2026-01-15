package throttler

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SocketThrottler allows manual control of the copier via a Unix socket.
// Commands:
//   - "throttle" or "pause": Pause the copier
//   - "no-throttle" or "resume": Resume the copier
//   - "status": Get current status
//   - "flush": Pause copier and trigger a flush
type SocketThrottler struct {
	socketPath string
	logger     *slog.Logger
	flusher    Flusher

	mu         sync.RWMutex
	throttled  bool
	listener   net.Listener
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// Flusher is an interface for triggering a binlog flush.
type Flusher interface {
	Flush(ctx context.Context) error
	GetDeltaLen() int
}

var _ Throttler = &SocketThrottler{}

// NewSocketThrottler creates a new SocketThrottler.
// socketPath: path to the Unix socket (e.g., /tmp/spirit.ebdb.states.sock)
func NewSocketThrottler(socketPath string, flusher Flusher, logger *slog.Logger) *SocketThrottler {
	return &SocketThrottler{
		socketPath: socketPath,
		flusher:    flusher,
		logger:     logger,
	}
}

// SocketPathForTable returns the default socket path for a given database and table.
func SocketPathForTable(database, table string) string {
	return filepath.Join("/tmp", fmt.Sprintf("spirit.%s.%s.sock", database, table))
}

func (t *SocketThrottler) Open(ctx context.Context) error {
	t.ctx, t.cancelFunc = context.WithCancel(ctx)

	// Remove existing socket file if it exists
	if err := os.Remove(t.socketPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket: %w", err)
	}

	var err error
	t.listener, err = net.Listen("unix", t.socketPath)
	if err != nil {
		return fmt.Errorf("failed to create socket: %w", err)
	}

	t.logger.Info("socket throttler listening",
		"socket", t.socketPath,
		"commands", "throttle|pause, no-throttle|resume, status, flush",
	)

	go t.acceptConnections()
	return nil
}

func (t *SocketThrottler) Close() error {
	if t.cancelFunc != nil {
		t.cancelFunc()
	}
	if t.listener != nil {
		t.listener.Close()
	}
	// Clean up socket file
	os.Remove(t.socketPath)
	return nil
}

func (t *SocketThrottler) IsThrottled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.throttled
}

// BlockWait blocks until the throttle is released.
func (t *SocketThrottler) BlockWait() {
	for t.IsThrottled() {
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
			// Check again
		}
	}
}

func (t *SocketThrottler) UpdateLag() error {
	return nil
}

func (t *SocketThrottler) acceptConnections() {
	for {
		select {
		case <-t.ctx.Done():
			return
		default:
		}

		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.ctx.Done():
				return
			default:
				t.logger.Error("error accepting connection", "error", err)
				continue
			}
		}

		go t.handleConnection(conn)
	}
}

func (t *SocketThrottler) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		// Try reading without newline
		line = strings.TrimSpace(string(line))
		if line == "" {
			return
		}
	}

	command := strings.TrimSpace(strings.ToLower(line))
	response := t.handleCommand(command)
	conn.Write([]byte(response + "\n"))
}

func (t *SocketThrottler) handleCommand(command string) string {
	switch command {
	case "throttle", "pause":
		t.mu.Lock()
		t.throttled = true
		t.mu.Unlock()
		t.logger.Warn("copier PAUSED via socket command")
		return "OK: copier paused"

	case "no-throttle", "resume":
		t.mu.Lock()
		t.throttled = false
		t.mu.Unlock()
		t.logger.Info("copier RESUMED via socket command")
		return "OK: copier resumed"

	case "status":
		t.mu.RLock()
		throttled := t.throttled
		t.mu.RUnlock()
		deltaLen := 0
		if t.flusher != nil {
			deltaLen = t.flusher.GetDeltaLen()
		}
		status := "running"
		if throttled {
			status = "paused"
		}
		return fmt.Sprintf("OK: copier=%s binlog_deltas=%d", status, deltaLen)

	case "flush":
		t.mu.Lock()
		t.throttled = true
		t.mu.Unlock()
		t.logger.Warn("copier PAUSED for flush via socket command")

		if t.flusher != nil {
			deltasBefore := t.flusher.GetDeltaLen()
			t.logger.Info("starting flush", "deltas_before", deltasBefore)
			if err := t.flusher.Flush(t.ctx); err != nil {
				t.logger.Error("flush error", "error", err)
				return fmt.Sprintf("ERROR: flush failed: %v", err)
			}
			deltasAfter := t.flusher.GetDeltaLen()
			t.logger.Info("flush completed",
				"deltas_before", deltasBefore,
				"deltas_after", deltasAfter,
				"deltas_flushed", deltasBefore-deltasAfter,
			)
			return fmt.Sprintf("OK: flushed %d deltas (before=%d, after=%d). Copier still paused - send 'resume' to continue",
				deltasBefore-deltasAfter, deltasBefore, deltasAfter)
		}
		return "OK: copier paused (no flusher configured)"

	default:
		return fmt.Sprintf("ERROR: unknown command '%s'. Valid commands: throttle|pause, no-throttle|resume, status, flush", command)
	}
}

