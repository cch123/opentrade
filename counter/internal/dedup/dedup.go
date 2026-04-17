// Package dedup implements the transfer_id idempotency table for Counter's
// Transfer interface (ADR-0011).
//
// Scope: only transfer_id dedup lives here. clientOrderId dedup (ADR-0015) is
// modeled as an index on the active-order map and lives in the engine package.
//
// Storage: in-memory. Eviction is lazy (on Get of an expired entry) plus an
// optional periodic Sweep loop. Durability-across-restart is provided by
// snapshotting the table alongside the account state.
package dedup

import (
	"sync"
	"time"
)

// Default TTL per ADR-0011.
const DefaultTTL = 24 * time.Hour

// Record is a cached Transfer response keyed by transfer_id.
type Record struct {
	Value     any       // caller-defined response payload (TransferResult)
	ExpiresAt time.Time // exclusive
}

// Table is a concurrent TTL map. Safe for concurrent Get/Set across many
// goroutines (Counter's UserSequencer does not provide global serialization).
type Table struct {
	mu  sync.RWMutex
	m   map[string]Record
	ttl time.Duration

	nowFunc func() time.Time // override in tests
}

// New constructs an empty Table with the given TTL (defaults to DefaultTTL
// if <= 0).
func New(ttl time.Duration) *Table {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Table{
		m:       make(map[string]Record),
		ttl:     ttl,
		nowFunc: time.Now,
	}
}

// Get returns the stored value for key, or (nil, false) if absent or expired.
// Expired entries are removed as a side-effect.
func (t *Table) Get(key string) (any, bool) {
	if key == "" {
		return nil, false
	}
	t.mu.RLock()
	rec, ok := t.m[key]
	t.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if t.nowFunc().After(rec.ExpiresAt) {
		t.mu.Lock()
		// Re-check under write lock to avoid racing with a concurrent Set
		// that refreshed the entry.
		if cur, ok := t.m[key]; ok && t.nowFunc().After(cur.ExpiresAt) {
			delete(t.m, key)
		}
		t.mu.Unlock()
		return nil, false
	}
	return rec.Value, true
}

// Set stores value under key with the Table's TTL.
func (t *Table) Set(key string, value any) {
	if key == "" {
		return
	}
	t.mu.Lock()
	t.m[key] = Record{Value: value, ExpiresAt: t.nowFunc().Add(t.ttl)}
	t.mu.Unlock()
}

// Delete removes the entry for key (no-op if absent).
func (t *Table) Delete(key string) {
	t.mu.Lock()
	delete(t.m, key)
	t.mu.Unlock()
}

// Len returns the current entry count (including any that may be expired but
// not yet evicted).
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.m)
}

// Sweep evicts all expired entries in one pass.
func (t *Table) Sweep() {
	now := t.nowFunc()
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, r := range t.m {
		if now.After(r.ExpiresAt) {
			delete(t.m, k)
		}
	}
}

// Entry is an exported view of a (key, record) pair for snapshot use.
type Entry struct {
	Key       string
	Value     any
	ExpiresAt time.Time
}

// Snapshot returns a copy of every unexpired entry.
func (t *Table) Snapshot() []Entry {
	now := t.nowFunc()
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Entry, 0, len(t.m))
	for k, r := range t.m {
		if now.After(r.ExpiresAt) {
			continue
		}
		out = append(out, Entry{Key: k, Value: r.Value, ExpiresAt: r.ExpiresAt})
	}
	return out
}

// Restore replaces the table contents with entries (useful on start-up from
// a snapshot). Pre-expired entries are skipped.
func (t *Table) Restore(entries []Entry) {
	now := t.nowFunc()
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m = make(map[string]Record, len(entries))
	for _, e := range entries {
		if now.After(e.ExpiresAt) {
			continue
		}
		t.m[e.Key] = Record{Value: e.Value, ExpiresAt: e.ExpiresAt}
	}
}

// SetNowFunc overrides the clock. Tests only.
func (t *Table) SetNowFunc(f func() time.Time) { t.nowFunc = f }
