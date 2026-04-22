package worker

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// pendingList tracks trade-event offsets that have been dispatched to
// per-user drains but whose fn set has not yet fully completed. The
// ADR-0060 ordered advancer reads the head of this list to decide when
// a watermark can be advanced (= TECheckpointEvent published).
//
// Ordering invariants:
//   - Enqueue appends at the list tail. The consumer loop is a single
//     goroutine (VShardWorker main), so offsets are naturally
//     ascending.
//   - popConsecutiveDone walks from the head and returns the largest
//     offset such that every entry at or before it has pending==0.
//   - Concurrent pending-counter decrements from per-user drain
//     goroutines do NOT hold pendingList.mu; the advancer is the sole
//     pop-path reader and takes mu only for the pop loop.
//
// Memory bound: enqueue ∝ trade-event consumption rate; pop ∝ advancer
// runtime. Under normal conditions list length ≈ in-flight TEs (expect
// < 100). If it grows unboundedly the evictor of this structure is
// whatever is wedging fn completion — Publish 5s-panic (ADR-0060 §3)
// is the upstream fail-fast. Length is exposed via Len() for
// monitoring (ADR-0060 §G6).
type pendingList struct {
	mu    sync.Mutex
	items *list.List            // *inFlightTE, ordered by offset ascending
	index map[int64]*list.Element // offset → element, O(1) lookups for tests
}

// inFlightTE is the per-entry book-keeping record. Allocation rate is
// per trade-event; keep it small.
type inFlightTE struct {
	partition int32
	offset    int64
	pending   atomic.Int32 // decremented by fn-done callbacks
}

// newPendingList constructs an empty pendingList.
func newPendingList() *pendingList {
	return &pendingList{
		items: list.New(),
		index: make(map[int64]*list.Element),
	}
}

// Enqueue registers a trade-event with the expected number of per-user
// fn completions (0, 1, or 2 in ADR-0058 dual-emit single-vshard flow).
// Returns the inFlightTE so per-user fn callbacks can reach it without
// another map lookup.
//
// expectedFnCount == 0 is legal (all sides are foreign users and the
// trade-event consumed no work): the entry is immediately eligible for
// the advancer and does not require any MarkFnDone call.
//
// expectedFnCount < 0 panics — programmer error.
func (p *pendingList) Enqueue(partition int32, offset int64, expectedFnCount int32) *inFlightTE {
	if expectedFnCount < 0 {
		panic("pendingList: negative expectedFnCount")
	}
	infl := &inFlightTE{partition: partition, offset: offset}
	infl.pending.Store(expectedFnCount)
	p.mu.Lock()
	e := p.items.PushBack(infl)
	p.index[offset] = e
	p.mu.Unlock()
	return infl
}

// MarkFnDone decrements infl.pending and reports whether the TE as a
// whole is now eligible for watermark advancement (pending dropped to
// zero on this call).
//
// Safe to call concurrently from multiple drain goroutines.
func (p *pendingList) MarkFnDone(infl *inFlightTE) (readyForAdvance bool) {
	remaining := infl.pending.Add(-1)
	if remaining < 0 {
		panic("pendingList: MarkFnDone called more times than Enqueue's expectedFnCount")
	}
	return remaining == 0
}

// PopConsecutiveDone removes the contiguous prefix of list entries whose
// pending is zero, and returns (partition, maxOffset, true) for the
// largest offset popped. Returns (_, _, false) if the head entry is not
// yet done (nothing to advance) or the list is empty.
//
// Called exclusively by the advancer goroutine — serialisation with
// Enqueue / MarkFnDone is via pendingList.mu plus the pending atomic.
func (p *pendingList) PopConsecutiveDone() (partition int32, maxOffset int64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		e := p.items.Front()
		if e == nil {
			return partition, maxOffset, ok
		}
		infl := e.Value.(*inFlightTE)
		if infl.pending.Load() != 0 {
			return partition, maxOffset, ok
		}
		p.items.Remove(e)
		delete(p.index, infl.offset)
		partition = infl.partition
		maxOffset = infl.offset
		ok = true
	}
}

// Len reports the current number of pending entries. Safe to call
// concurrently; used by ADR-0060 §G6 monitoring to flag wedged fn
// completions.
func (p *pendingList) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.items.Len()
}

// HeadOffset returns the offset of the oldest pending entry, or
// (0, false) if empty. Useful for lag metrics and tests.
func (p *pendingList) HeadOffset() (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.items.Front()
	if e == nil {
		return 0, false
	}
	return e.Value.(*inFlightTE).offset, true
}
