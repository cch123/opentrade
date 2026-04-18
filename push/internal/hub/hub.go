// Package hub maintains the in-process registry of active WebSocket
// connections and their subscriptions, and fans out broadcast / per-user
// sends to the owning goroutines.
//
// The Hub itself never touches the wire — it just dereferences connection ids
// to a buffered send channel. Slow consumers get their messages dropped (see
// Send) rather than blocking other subscribers, per ADR-0022 §广播队列.
package hub

import (
	"sync"

	"go.uber.org/zap"
)

// Sink is what a Conn exposes to Hub for delivery. In production this is the
// per-connection write goroutine's inbox channel; tests can use a fake.
type Sink interface {
	// TrySend attempts a non-blocking enqueue. Returns false if the inbox is
	// full (the message is dropped).
	TrySend(payload []byte) bool
	// ID is the connection id used as the map key.
	ID() string
	// UserID is the authenticated user id; may be empty for anonymous.
	UserID() string
}

// Hub is safe for concurrent use by the WS accept goroutine, read loops (for
// Subscribe / Unsubscribe), and Kafka consumer goroutines (for broadcast).
type Hub struct {
	logger *zap.Logger

	mu       sync.RWMutex
	conns    map[string]Sink
	byUser   map[string]map[string]struct{} // userID → set<connID>
	byStream map[string]map[string]struct{} // streamKey → set<connID>
	connSubs map[string]map[string]struct{} // connID → set<streamKey> (for cleanup)
}

// New returns an empty Hub.
func New(logger *zap.Logger) *Hub {
	return &Hub{
		logger:   logger,
		conns:    make(map[string]Sink),
		byUser:   make(map[string]map[string]struct{}),
		byStream: make(map[string]map[string]struct{}),
		connSubs: make(map[string]map[string]struct{}),
	}
}

// Register adds a connection to the indexes. A connection with an empty
// userID is registered for public streams only.
func (h *Hub) Register(c Sink) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[c.ID()] = c
	h.connSubs[c.ID()] = make(map[string]struct{})
	if uid := c.UserID(); uid != "" {
		set, ok := h.byUser[uid]
		if !ok {
			set = make(map[string]struct{})
			h.byUser[uid] = set
		}
		set[c.ID()] = struct{}{}
	}
}

// Unregister removes a connection from every index it appears in.
func (h *Hub) Unregister(connID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c, ok := h.conns[connID]
	if !ok {
		return
	}
	delete(h.conns, connID)

	if uid := c.UserID(); uid != "" {
		if set, ok := h.byUser[uid]; ok {
			delete(set, connID)
			if len(set) == 0 {
				delete(h.byUser, uid)
			}
		}
	}
	if subs, ok := h.connSubs[connID]; ok {
		for stream := range subs {
			if set, ok := h.byStream[stream]; ok {
				delete(set, connID)
				if len(set) == 0 {
					delete(h.byStream, stream)
				}
			}
		}
		delete(h.connSubs, connID)
	}
}

// Subscribe adds streamKeys to a connection's subscription set. Returns the
// streams that were newly added (for caller ack); unknown connections are a
// no-op (returns nil).
func (h *Hub) Subscribe(connID string, streams []string) []string {
	if len(streams) == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	conn, ok := h.connSubs[connID]
	if !ok {
		return nil
	}
	added := make([]string, 0, len(streams))
	for _, s := range streams {
		if s == "" {
			continue
		}
		if _, already := conn[s]; already {
			continue
		}
		conn[s] = struct{}{}
		set, ok := h.byStream[s]
		if !ok {
			set = make(map[string]struct{})
			h.byStream[s] = set
		}
		set[connID] = struct{}{}
		added = append(added, s)
	}
	return added
}

// Unsubscribe removes streamKeys. Returns the streams that were actually
// removed (caller may ack those).
func (h *Hub) Unsubscribe(connID string, streams []string) []string {
	if len(streams) == 0 {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	conn, ok := h.connSubs[connID]
	if !ok {
		return nil
	}
	removed := make([]string, 0, len(streams))
	for _, s := range streams {
		if _, subscribed := conn[s]; !subscribed {
			continue
		}
		delete(conn, s)
		if set, ok := h.byStream[s]; ok {
			delete(set, connID)
			if len(set) == 0 {
				delete(h.byStream, s)
			}
		}
		removed = append(removed, s)
	}
	return removed
}

// BroadcastStream delivers payload to every connection subscribed to
// streamKey. Returns the number of successful deliveries (slow consumers are
// logged and counted as drops in the second return value).
func (h *Hub) BroadcastStream(streamKey string, payload []byte) (sent, dropped int) {
	h.mu.RLock()
	set, ok := h.byStream[streamKey]
	if !ok {
		h.mu.RUnlock()
		return 0, 0
	}
	targets := make([]Sink, 0, len(set))
	for cid := range set {
		if c, ok := h.conns[cid]; ok {
			targets = append(targets, c)
		}
	}
	h.mu.RUnlock()

	for _, c := range targets {
		if c.TrySend(payload) {
			sent++
			continue
		}
		dropped++
		h.logger.Warn("dropped message for slow consumer",
			zap.String("conn", c.ID()),
			zap.String("stream", streamKey))
	}
	return sent, dropped
}

// SendUser delivers payload to every connection owned by userID. Same slow
// consumer policy as BroadcastStream.
func (h *Hub) SendUser(userID string, payload []byte) (sent, dropped int) {
	if userID == "" {
		return 0, 0
	}
	h.mu.RLock()
	set, ok := h.byUser[userID]
	if !ok {
		h.mu.RUnlock()
		return 0, 0
	}
	targets := make([]Sink, 0, len(set))
	for cid := range set {
		if c, ok := h.conns[cid]; ok {
			targets = append(targets, c)
		}
	}
	h.mu.RUnlock()

	for _, c := range targets {
		if c.TrySend(payload) {
			sent++
			continue
		}
		dropped++
		h.logger.Warn("dropped private message for slow consumer",
			zap.String("conn", c.ID()),
			zap.String("user", userID))
	}
	return sent, dropped
}

// ConnCount returns the number of active connections (for metrics/tests).
func (h *Hub) ConnCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.conns)
}

// StreamSubscribers returns the number of conns currently subscribed to the
// given stream (for tests/metrics).
func (h *Hub) StreamSubscribers(stream string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.byStream[stream])
}
