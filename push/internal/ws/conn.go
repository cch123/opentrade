package ws

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/push/internal/hub"
)

// Conn wraps a WebSocket connection. It owns a single read goroutine and a
// single write goroutine; Hub enqueues outbound messages via TrySend or
// TrySendCoalesce.
type Conn struct {
	id     string
	userID string
	ws     *websocket.Conn
	send   chan []byte
	logger *zap.Logger
	hub    *hub.Hub

	writeTimeout time.Duration
	rate         *tokenBucket

	// Coalesce path (ADR-0037). For "replaceable" streams like KlineUpdate
	// we keep a single latest payload per streamKey; when the write loop is
	// under quota it flushes the map to the wire. This turns a flood of
	// rapid updates into one or two sends carrying the freshest state,
	// instead of backing up the send channel until messages get dropped.
	coalMu  sync.Mutex
	coalMap map[string][]byte
	coalSig chan struct{}

	closeOnce sync.Once
	done      chan struct{}
	closed    atomic.Bool
}

// Config tunes per-connection runtime bounds.
type Config struct {
	SendBuffer   int           // outbound queue depth; defaults to 256
	WriteTimeout time.Duration // per-write deadline; defaults to 10s

	// Per-connection outbound rate limit (messages / second, with burst).
	// <= 0 disables the limiter — the default keeps the rate high enough
	// that well-behaved clients never trip it.
	MessageRate  float64
	MessageBurst float64

	// Sticky routing (ADR-0033): when TotalInstances > 1 the HTTP handler
	// verifies shard.Index(userID, TotalInstances) == InstanceOrdinal
	// before accepting the upgrade. <=1 disables the check.
	InstanceOrdinal int
	TotalInstances  int
}

// NewConn wraps ws into a ready-but-not-running Conn. The caller must call
// Start(ctx) to spin up the read/write goroutines.
func NewConn(id, userID string, wsConn *websocket.Conn, h *hub.Hub, cfg Config, logger *zap.Logger) *Conn {
	if cfg.SendBuffer <= 0 {
		cfg.SendBuffer = 256
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	return &Conn{
		id:           id,
		userID:       userID,
		ws:           wsConn,
		send:         make(chan []byte, cfg.SendBuffer),
		logger:       logger,
		hub:          h,
		writeTimeout: cfg.WriteTimeout,
		rate:         newTokenBucket(cfg.MessageRate, cfg.MessageBurst),
		coalMap:      make(map[string][]byte),
		coalSig:      make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
}

// ID implements hub.Sink.
func (c *Conn) ID() string { return c.id }

// UserID implements hub.Sink.
func (c *Conn) UserID() string { return c.userID }

// TrySend implements hub.Sink — non-blocking enqueue; returns false if the
// outbound queue is full (hub will treat as a drop).
func (c *Conn) TrySend(payload []byte) bool {
	if c.closed.Load() {
		return false
	}
	select {
	case c.send <- payload:
		return true
	default:
		return false
	}
}

// TrySendCoalesce implements hub.Sink — stash payload as the "latest for
// coalesceKey" and wake the write loop. Always returns true (unless the
// conn is already closed) because the map is an unbounded latest-wins
// store: older payloads for the same key are discarded, never queued.
// ADR-0037.
func (c *Conn) TrySendCoalesce(coalesceKey string, payload []byte) bool {
	if c.closed.Load() {
		return false
	}
	c.coalMu.Lock()
	c.coalMap[coalesceKey] = payload
	c.coalMu.Unlock()
	select {
	case c.coalSig <- struct{}{}:
	default:
	}
	return true
}

// Start runs the read and write loops. Blocks until either side exits, then
// drains and removes the connection from the Hub. Safe to call once.
func (c *Conn) Start(ctx context.Context) {
	c.hub.Register(c)
	// Every authenticated connection is implicitly on the private user stream.
	if c.userID != "" {
		c.hub.Subscribe(c.id, []string{StreamUser})
	}

	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.readLoop(loopCtx)
		cancel() // unblock write loop on reader exit
	}()
	go func() {
		defer wg.Done()
		c.writeLoop(loopCtx)
		cancel() // unblock read loop on writer exit
	}()
	wg.Wait()
	c.close()
}

// Close triggers orderly shutdown. Safe to call from any goroutine.
func (c *Conn) close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.done)
		c.hub.Unregister(c.id)
		_ = c.ws.Close(websocket.StatusNormalClosure, "bye")
	})
}

func (c *Conn) readLoop(ctx context.Context) {
	for {
		_, data, err := c.ws.Read(ctx)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				c.logger.Debug("ws read closed",
					zap.String("conn", c.id), zap.Error(err))
			}
			return
		}
		var msg ClientMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			c.sendControl(ControlMsg{Op: OpError, Message: "invalid json"})
			continue
		}
		c.handleClientMsg(&msg)
	}
}

func (c *Conn) writeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-c.send:
			if !ok {
				return
			}
			if !c.rate.allow() {
				c.logger.Warn("ws rate-limit drop",
					zap.String("conn", c.id),
					zap.String("path", "send"))
				continue
			}
			if err := c.writeFrame(ctx, payload); err != nil {
				return
			}
		case <-c.coalSig:
			if !c.flushCoalesce(ctx) {
				return
			}
		}
	}
}

// flushCoalesce drains the coalesce map (snapshot under the lock so new
// overwrites after we leave don't block us) and writes each entry. Rate
// limit applies per-entry: rate-limited entries are put back in the map so
// the next signal picks them up (or a newer overwrite replaces them).
// Returns false on a terminal write error (caller should exit the loop).
func (c *Conn) flushCoalesce(ctx context.Context) bool {
	c.coalMu.Lock()
	pending := c.coalMap
	c.coalMap = make(map[string][]byte, len(pending))
	c.coalMu.Unlock()
	if len(pending) == 0 {
		return true
	}
	var reque map[string][]byte
	for key, payload := range pending {
		if !c.rate.allow() {
			c.logger.Warn("ws rate-limit drop",
				zap.String("conn", c.id),
				zap.String("path", "coalesce"),
				zap.String("stream", key))
			if reque == nil {
				reque = make(map[string][]byte)
			}
			reque[key] = payload
			continue
		}
		if err := c.writeFrame(ctx, payload); err != nil {
			return false
		}
	}
	if len(reque) > 0 {
		c.coalMu.Lock()
		for k, v := range reque {
			if _, overwritten := c.coalMap[k]; !overwritten {
				c.coalMap[k] = v
			}
		}
		c.coalMu.Unlock()
		select {
		case c.coalSig <- struct{}{}:
		default:
		}
	}
	return true
}

// writeFrame pushes one payload through the underlying WS. Returns an error
// only when the caller should exit (ctx dead; socket broken).
func (c *Conn) writeFrame(ctx context.Context, payload []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	err := c.ws.Write(writeCtx, websocket.MessageText, payload)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			c.logger.Debug("ws write failed",
				zap.String("conn", c.id), zap.Error(err))
		}
		return err
	}
	return nil
}

func (c *Conn) handleClientMsg(msg *ClientMsg) {
	switch msg.Op {
	case OpSubscribe:
		added := c.hub.Subscribe(c.id, msg.Streams)
		c.sendControl(ControlMsg{Op: OpAck, Streams: added})
	case OpUnsubscribe:
		removed := c.hub.Unsubscribe(c.id, msg.Streams)
		c.sendControl(ControlMsg{Op: OpAck, Streams: removed})
	case OpPing:
		c.sendControl(ControlMsg{Op: OpPong})
	default:
		c.sendControl(ControlMsg{Op: OpError, Message: "unknown op"})
	}
}

func (c *Conn) sendControl(m ControlMsg) {
	payload, err := EncodeControl(m)
	if err != nil {
		c.logger.Error("encode control", zap.Error(err))
		return
	}
	// Fire-and-forget: if the queue is full we drop, same as any broadcast.
	_ = c.TrySend(payload)
}
