package engine

// ADR-0062: per-user ring buffer for recently evicted terminal orders.
//
// Rationale: once the evictor goroutine removes a terminal order from
// OrderStore.byID, the existing CancelOrder RPC idempotency guard
// (`Get(order_id) + Status.IsTerminal()`) breaks — `Get` returns nil
// and the client sees ErrOrderNotFound on retry. To keep CancelOrder
// idempotent across evict, each Account keeps a fixed-capacity ring
// of (order_id, final_status, ...) for recent evictions. CancelOrder
// checks byID first; on miss, queries the ring; on miss again, returns
// ErrOrderNotFound for real.
//
// The ring is structurally analogous to `recentTransferIDs` (ADR-0057)
// but stores structured entries rather than bare ids. Slot storage
// carries only order_id (for FIFO eviction bookkeeping); payload lives
// in a companion map keyed by order_id.

// TerminatedOrderEntry is the per-user cached record of a terminal
// order that has since been evicted from OrderStore.byID. Kept small:
// no balances, no fill details — CancelOrder only needs enough to
// construct an idempotent terminal response.
type TerminatedOrderEntry struct {
	OrderID       uint64
	FinalStatus   OrderStatus
	TerminatedAt  int64 // ms timestamp, mirrors Order.TerminatedAt
	ClientOrderID string
	Symbol        string
}

// LookupTerminated returns the cached entry for orderID if the ring
// still holds it, together with ok=true. Returns ok=false when the
// order was never evicted (CancelOrder should look in OrderStore.byID
// first) or has already been displaced by FIFO overflow.
//
// orderID == 0 returns ok=false (reserved sentinel).
func (a *Account) LookupTerminated(orderID uint64) (TerminatedOrderEntry, bool) {
	if orderID == 0 {
		return TerminatedOrderEntry{}, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	entry, ok := a.recentTerminatedSet[orderID]
	return entry, ok
}

// RememberTerminated inserts entry into the ring. If the ring is full,
// the oldest entry is evicted (both the slot and its companion map
// record are overwritten). Duplicate order_ids are overwritten with
// the new entry (idempotent for the evictor's "crash-and-retry" path
// — a second attempt carries the same payload so the overwrite is a
// no-op in practice, but the slot's position in FIFO order resets,
// which is harmless).
//
// entry.OrderID == 0 is a no-op.
func (a *Account) RememberTerminated(entry TerminatedOrderEntry) {
	if entry.OrderID == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	cap := a.recentTerminatedCap
	if cap <= 0 {
		cap = TerminatedRingCapacityDefault
		a.recentTerminatedCap = cap
	}
	if a.recentTerminatedSet == nil {
		a.recentTerminatedIDs = make([]uint64, cap)
		a.recentTerminatedSet = make(map[uint64]TerminatedOrderEntry, cap)
	}
	if _, exists := a.recentTerminatedSet[entry.OrderID]; exists {
		// Already in ring; refresh payload without touching FIFO position.
		a.recentTerminatedSet[entry.OrderID] = entry
		return
	}
	if a.recentTerminatedSize < cap {
		a.recentTerminatedIDs[a.recentTerminatedSize] = entry.OrderID
		a.recentTerminatedSize++
	} else {
		oldID := a.recentTerminatedIDs[a.recentTerminatedHead]
		delete(a.recentTerminatedSet, oldID)
		a.recentTerminatedIDs[a.recentTerminatedHead] = entry.OrderID
		a.recentTerminatedHead = (a.recentTerminatedHead + 1) % cap
	}
	a.recentTerminatedSet[entry.OrderID] = entry
}

// RecentTerminatedSnapshot returns the ring contents in insertion order
// (oldest → newest) for snapshot serialisation. Used by snapshot.Capture
// so restart restores the full dedup window. Returns nil when empty.
func (a *Account) RecentTerminatedSnapshot() []TerminatedOrderEntry {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.recentTerminatedSize == 0 {
		return nil
	}
	cap := a.recentTerminatedCap
	out := make([]TerminatedOrderEntry, 0, a.recentTerminatedSize)
	if a.recentTerminatedSize < cap {
		// Not yet wrapped: slots 0..size-1 are oldest → newest.
		for i := 0; i < a.recentTerminatedSize; i++ {
			id := a.recentTerminatedIDs[i]
			if entry, ok := a.recentTerminatedSet[id]; ok {
				out = append(out, entry)
			}
		}
	} else {
		// Full: start at head (oldest slot), wrap around.
		for i := 0; i < cap; i++ {
			id := a.recentTerminatedIDs[(a.recentTerminatedHead+i)%cap]
			if entry, ok := a.recentTerminatedSet[id]; ok {
				out = append(out, entry)
			}
		}
	}
	return out
}

// RestoreRecentTerminated rebuilds the ring from an ordered slice
// (oldest → newest). If entries exceeds the account's capacity, only
// the tail (most recent) is kept. Restore-only; same contract as
// PutForRestore — must run before the shard starts serving traffic.
//
// Entries with OrderID == 0 are skipped defensively.
func (a *Account) RestoreRecentTerminated(entries []TerminatedOrderEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	cap := a.recentTerminatedCap
	if cap <= 0 {
		cap = TerminatedRingCapacityDefault
		a.recentTerminatedCap = cap
	}
	if len(entries) == 0 {
		a.recentTerminatedIDs = nil
		a.recentTerminatedSet = nil
		a.recentTerminatedHead = 0
		a.recentTerminatedSize = 0
		return
	}
	if len(entries) > cap {
		entries = entries[len(entries)-cap:]
	}
	n := len(entries)
	a.recentTerminatedIDs = make([]uint64, cap)
	a.recentTerminatedSet = make(map[uint64]TerminatedOrderEntry, n)
	slot := 0
	for _, e := range entries {
		if e.OrderID == 0 {
			continue
		}
		a.recentTerminatedIDs[slot] = e.OrderID
		a.recentTerminatedSet[e.OrderID] = e
		slot++
	}
	a.recentTerminatedSize = slot
	a.recentTerminatedHead = 0
	if slot == cap {
		// Ring is full; head is the oldest slot which, after a restore
		// that packs oldest → newest into slots 0..cap-1, sits at index 0.
		a.recentTerminatedHead = 0
	}
}

// TerminatedRingCapacity reports the configured capacity of this
// account's terminated-orders ring. Used by tests / introspection;
// production code should not need to read this.
func (a *Account) TerminatedRingCapacity() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.recentTerminatedCap <= 0 {
		return TerminatedRingCapacityDefault
	}
	return a.recentTerminatedCap
}
