// Package marketcache stores BFF's per-symbol view of market-data so that
// REST clients can resync after a disconnect without Push having to replay
// history (ADR-0038).
//
// Scope:
//   - Latest DepthSnapshot per symbol (overwritten by every incoming
//     DepthSnapshot event from Quote).
//   - Ring buffer of the last N KlineClosed per (symbol, interval). Defaults
//     to 500 per bucket.
//
// Out of scope for MVP: PublicTrade history (go to MySQL trades table),
// DepthUpdate accumulation (clients should use DepthSnapshot + live stream).
package marketcache

import (
	"sync"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// Config tunes the cache.
type Config struct {
	// KlineBuffer caps the number of KlineClosed events kept per
	// (symbol, interval). Older entries fall off the front.
	KlineBuffer int
}

// Cache is safe for concurrent use. It keeps everything in memory; restart
// repopulates from market-data tail like any other Quote consumer.
type Cache struct {
	cfg Config

	mu     sync.RWMutex
	depth  map[string]*eventpb.DepthSnapshot
	klines map[klineKey]*ringBuffer
}

type klineKey struct {
	Symbol   string
	Interval eventpb.KlineInterval
}

// New returns an empty cache. A zero/negative KlineBuffer falls back to 500.
func New(cfg Config) *Cache {
	if cfg.KlineBuffer <= 0 {
		cfg.KlineBuffer = 500
	}
	return &Cache{
		cfg:    cfg,
		depth:  make(map[string]*eventpb.DepthSnapshot),
		klines: make(map[klineKey]*ringBuffer),
	}
}

// PutDepthSnapshot replaces the stored snapshot for snap.Symbol.
func (c *Cache) PutDepthSnapshot(snap *eventpb.DepthSnapshot) {
	if snap == nil || snap.Symbol == "" {
		return
	}
	c.mu.Lock()
	c.depth[snap.Symbol] = snap
	c.mu.Unlock()
}

// DepthSnapshot returns the latest stored snapshot for symbol, or nil if we
// have not seen one yet. Caller must treat the returned pointer as
// read-only (no copy).
func (c *Cache) DepthSnapshot(symbol string) *eventpb.DepthSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.depth[symbol]
}

// AppendKlineClosed records a closed bar into the ring buffer for its
// (symbol, interval). Out-of-order events are appended verbatim; dedup +
// ordering are the emitter's responsibility.
func (c *Cache) AppendKlineClosed(symbol string, closed *eventpb.KlineClosed) {
	if closed == nil || closed.Kline == nil || symbol == "" {
		return
	}
	k := klineKey{Symbol: symbol, Interval: closed.Kline.Interval}
	c.mu.Lock()
	rb, ok := c.klines[k]
	if !ok {
		rb = newRingBuffer(c.cfg.KlineBuffer)
		c.klines[k] = rb
	}
	rb.push(closed.Kline)
	c.mu.Unlock()
}

// RecentKlines returns up to limit of the most recently closed bars for
// (symbol, interval), oldest first. Missing keys yield nil.
func (c *Cache) RecentKlines(symbol string, interval eventpb.KlineInterval, limit int) []*eventpb.Kline {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rb, ok := c.klines[klineKey{Symbol: symbol, Interval: interval}]
	if !ok {
		return nil
	}
	return rb.tail(limit)
}

// -----------------------------------------------------------------------------
// ring buffer of *eventpb.Kline
// -----------------------------------------------------------------------------

type ringBuffer struct {
	cap  int
	data []*eventpb.Kline
	head int // index of oldest; next write goes at (head+len(data))%cap if not full
	size int
}

func newRingBuffer(cap int) *ringBuffer {
	return &ringBuffer{cap: cap, data: make([]*eventpb.Kline, cap)}
}

func (r *ringBuffer) push(k *eventpb.Kline) {
	if r.size < r.cap {
		r.data[(r.head+r.size)%r.cap] = k
		r.size++
		return
	}
	// Full — overwrite oldest and advance head.
	r.data[r.head] = k
	r.head = (r.head + 1) % r.cap
}

func (r *ringBuffer) tail(limit int) []*eventpb.Kline {
	if limit <= 0 || limit > r.size {
		limit = r.size
	}
	out := make([]*eventpb.Kline, limit)
	start := r.head + (r.size - limit)
	for i := 0; i < limit; i++ {
		out[i] = r.data[(start+i)%r.cap]
	}
	return out
}
