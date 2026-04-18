// Package ratelimit provides per-key sliding-window rate limiting used by
// BFF REST handlers.
//
// Design: simple in-memory map[key] -> sorted slice of request timestamps
// within the current window. Strictly single-node; cluster-wide limiting is
// out of MVP scope (ADR-0015/0018/0020 touch business correctness, not rate
// limiting).
package ratelimit

import (
	"sync"
	"time"
)

// SlidingWindow is a thread-safe sliding-window limiter. Allow returns true
// if the request fits within the per-key quota, false if it should be
// rejected (HTTP 429).
type SlidingWindow struct {
	limit  int
	window time.Duration

	mu      sync.Mutex
	buckets map[string]*bucket

	nowFunc func() time.Time // override in tests
}

type bucket struct {
	stamps []time.Time
}

// New builds a SlidingWindow allowing at most limit requests per window.
func New(limit int, window time.Duration) *SlidingWindow {
	if limit <= 0 {
		limit = 1
	}
	if window <= 0 {
		window = time.Second
	}
	return &SlidingWindow{
		limit:   limit,
		window:  window,
		buckets: make(map[string]*bucket),
		nowFunc: time.Now,
	}
}

// Allow records and checks a request for key. It prunes stamps older than
// the window as a side effect.
func (s *SlidingWindow) Allow(key string) bool {
	if key == "" {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowFunc()
	cutoff := now.Add(-s.window)

	b, ok := s.buckets[key]
	if !ok {
		b = &bucket{}
		s.buckets[key] = b
	}
	// Drop stamps older than the window.
	i := 0
	for i < len(b.stamps) && b.stamps[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		b.stamps = b.stamps[i:]
	}
	if len(b.stamps) >= s.limit {
		return false
	}
	b.stamps = append(b.stamps, now)
	return true
}

// Sweep removes empty buckets whose most recent stamp is older than the
// window. Call periodically from a background goroutine to bound memory.
func (s *SlidingWindow) Sweep() {
	now := s.nowFunc()
	cutoff := now.Add(-s.window)
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, b := range s.buckets {
		if len(b.stamps) == 0 || b.stamps[len(b.stamps)-1].Before(cutoff) {
			delete(s.buckets, k)
		}
	}
}

// SetNowFunc overrides the clock for tests.
func (s *SlidingWindow) SetNowFunc(f func() time.Time) { s.nowFunc = f }
