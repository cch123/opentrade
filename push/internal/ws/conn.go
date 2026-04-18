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
// single write goroutine; Hub enqueues outbound messages via TrySend.
type Conn struct {
	id     string
	userID string
	ws     *websocket.Conn
	send   chan []byte
	logger *zap.Logger
	hub    *hub.Hub

	writeTimeout time.Duration

	closeOnce sync.Once
	done      chan struct{}
	closed    atomic.Bool
}

// Config tunes per-connection runtime bounds.
type Config struct {
	SendBuffer   int           // outbound queue depth; defaults to 256
	WriteTimeout time.Duration // per-write deadline; defaults to 10s

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
			writeCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
			err := c.ws.Write(writeCtx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					c.logger.Debug("ws write failed",
						zap.String("conn", c.id), zap.Error(err))
				}
				return
			}
		}
	}
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
