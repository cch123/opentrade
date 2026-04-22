package engine

import (
	"fmt"
	"sync"
	"testing"
)

// mkEntry is a test helper producing a TerminatedOrderEntry with
// deterministic fields derived from id.
func mkEntry(id uint64) TerminatedOrderEntry {
	return TerminatedOrderEntry{
		OrderID:       id,
		FinalStatus:   OrderStatusCanceled,
		TerminatedAt:  int64(id) * 1000,
		ClientOrderID: fmt.Sprintf("coid-%d", id),
		Symbol:        "BTCUSDT",
	}
}

// TestTerminatedRing_FillBelowCap inserts fewer than capacity entries
// and verifies every one remains addressable via Lookup.
func TestTerminatedRing_FillBelowCap(t *testing.T) {
	acc := newAccount("u1")
	for i := uint64(1); i <= 10; i++ {
		acc.RememberTerminated(mkEntry(i))
	}
	for i := uint64(1); i <= 10; i++ {
		entry, ok := acc.LookupTerminated(i)
		if !ok {
			t.Fatalf("id %d not found after fill", i)
		}
		if entry.OrderID != i || entry.ClientOrderID != fmt.Sprintf("coid-%d", i) {
			t.Fatalf("unexpected entry for id %d: %+v", i, entry)
		}
	}
	if _, ok := acc.LookupTerminated(9999); ok {
		t.Fatalf("never-inserted id 9999 should not be found")
	}
	if _, ok := acc.LookupTerminated(0); ok {
		t.Fatalf("sentinel id 0 should always return ok=false")
	}
}

// TestTerminatedRing_OverflowEvictsOldest fills past the default capacity
// (1024) and verifies the earliest-inserted ids are displaced.
func TestTerminatedRing_OverflowEvictsOldest(t *testing.T) {
	acc := newAccount("u1")
	cap := acc.TerminatedRingCapacity()
	if cap != TerminatedRingCapacityDefault {
		t.Fatalf("default cap = %d, want %d", cap, TerminatedRingCapacityDefault)
	}
	total := uint64(cap + 5)
	for i := uint64(1); i <= total; i++ {
		acc.RememberTerminated(mkEntry(i))
	}
	// First 5 should be evicted.
	for i := uint64(1); i <= 5; i++ {
		if _, ok := acc.LookupTerminated(i); ok {
			t.Fatalf("id %d should have been evicted", i)
		}
	}
	// Remaining 1024 should be present.
	for i := uint64(6); i <= total; i++ {
		if _, ok := acc.LookupTerminated(i); !ok {
			t.Fatalf("id %d should still be present", i)
		}
	}
}

// TestTerminatedRing_DuplicateRememberRefreshesPayload verifies that a
// second RememberTerminated for the same order_id updates the stored
// payload (simulates the evictor's crash-and-retry producing the same
// entry again) without changing FIFO ordering.
func TestTerminatedRing_DuplicateRememberRefreshesPayload(t *testing.T) {
	acc := newAccount("u1")
	acc.RememberTerminated(mkEntry(1))
	// Fill most of the ring; id=1 should still be around.
	cap := acc.TerminatedRingCapacity()
	for i := uint64(2); i <= uint64(cap-1); i++ {
		acc.RememberTerminated(mkEntry(i))
	}
	// Remember id=1 again with different payload.
	refreshed := TerminatedOrderEntry{
		OrderID:       1,
		FinalStatus:   OrderStatusFilled, // differs from mkEntry
		TerminatedAt:  42,
		ClientOrderID: "coid-refreshed",
		Symbol:        "ETHUSDT",
	}
	acc.RememberTerminated(refreshed)
	// Lookup should return the refreshed payload.
	got, ok := acc.LookupTerminated(1)
	if !ok {
		t.Fatalf("id 1 should still be present after duplicate remember")
	}
	if got.FinalStatus != OrderStatusFilled || got.ClientOrderID != "coid-refreshed" {
		t.Fatalf("payload not refreshed: %+v", got)
	}
	// Now push enough new ids to force an eviction round; id=1 should
	// still survive because duplicate-remember did NOT reset its FIFO
	// position to "newest" — it stays where it was (near the oldest).
	// Push exactly 1 additional id to trigger overflow of the oldest slot.
	acc.RememberTerminated(mkEntry(uint64(cap) + 10))
	if _, ok := acc.LookupTerminated(1); !ok {
		t.Fatalf("id 1 should still be present after single overflow (FIFO position was near middle)")
	}
	// Force full wrap-around: now id=1 must be evicted eventually.
	for i := uint64(cap) + 11; i < uint64(cap)*2+20; i++ {
		acc.RememberTerminated(mkEntry(i))
	}
	if _, ok := acc.LookupTerminated(1); ok {
		t.Fatalf("id 1 should have been evicted after full wrap-around")
	}
}

// TestTerminatedRing_SnapshotRoundTrip verifies RecentTerminatedSnapshot
// followed by RestoreRecentTerminated on a fresh account reproduces the
// same lookup behaviour.
func TestTerminatedRing_SnapshotRoundTrip(t *testing.T) {
	src := newAccount("u1")
	cap := src.TerminatedRingCapacity()
	// Fill past cap to exercise wrap-around.
	total := uint64(cap + 3)
	for i := uint64(1); i <= total; i++ {
		src.RememberTerminated(mkEntry(i))
	}
	snap := src.RecentTerminatedSnapshot()
	if len(snap) != cap {
		t.Fatalf("snapshot length = %d, want %d", len(snap), cap)
	}
	// Snapshot ordered oldest → newest: first element should be id=4
	// (1,2,3 evicted when 1025,1026,1027 arrived — cap=1024).
	wantFirst := uint64(total - uint64(cap) + 1)
	if snap[0].OrderID != wantFirst {
		t.Fatalf("snapshot[0] = %d, want %d", snap[0].OrderID, wantFirst)
	}
	if snap[len(snap)-1].OrderID != total {
		t.Fatalf("snapshot[last] = %d, want %d", snap[len(snap)-1].OrderID, total)
	}

	dst := newAccount("u1")
	dst.RestoreRecentTerminated(snap)
	for _, e := range snap {
		got, ok := dst.LookupTerminated(e.OrderID)
		if !ok {
			t.Fatalf("id %d missing after restore", e.OrderID)
		}
		if got != e {
			t.Fatalf("id %d payload diverged after restore: got %+v want %+v", e.OrderID, got, e)
		}
	}
	// Ids that were evicted from src should not appear in dst.
	for i := uint64(1); i < wantFirst; i++ {
		if _, ok := dst.LookupTerminated(i); ok {
			t.Fatalf("id %d should not exist after restore from truncated snapshot", i)
		}
	}
	// Further inserts on dst should evict dst's oldest (wantFirst) first.
	dst.RememberTerminated(mkEntry(9999))
	if _, ok := dst.LookupTerminated(wantFirst); ok {
		t.Fatalf("oldest id %d should have been evicted by the post-restore insert", wantFirst)
	}
}

// TestTerminatedRing_RestoreOversizedTruncates feeds a slice larger than
// the account's capacity and verifies only the most-recent `cap` entries
// survive.
func TestTerminatedRing_RestoreOversizedTruncates(t *testing.T) {
	acc := newAccountWithCap("u1", 8)
	entries := make([]TerminatedOrderEntry, 0, 20)
	for i := uint64(1); i <= 20; i++ {
		entries = append(entries, mkEntry(i))
	}
	acc.RestoreRecentTerminated(entries)
	// Only ids 13..20 should survive.
	for i := uint64(1); i <= 12; i++ {
		if _, ok := acc.LookupTerminated(i); ok {
			t.Fatalf("id %d should have been truncated", i)
		}
	}
	for i := uint64(13); i <= 20; i++ {
		if _, ok := acc.LookupTerminated(i); !ok {
			t.Fatalf("id %d should have been kept", i)
		}
	}
}

// TestTerminatedRing_RestoreEmptyClearsRing verifies an empty slice
// resets ring state (useful for restore from pre-ADR-0062 snapshots).
func TestTerminatedRing_RestoreEmptyClearsRing(t *testing.T) {
	acc := newAccount("u1")
	acc.RememberTerminated(mkEntry(1))
	acc.RememberTerminated(mkEntry(2))
	acc.RestoreRecentTerminated(nil)
	if _, ok := acc.LookupTerminated(1); ok {
		t.Fatalf("id 1 should have been cleared by empty restore")
	}
	if _, ok := acc.LookupTerminated(2); ok {
		t.Fatalf("id 2 should have been cleared by empty restore")
	}
	// Re-inserts should work afresh.
	acc.RememberTerminated(mkEntry(42))
	if _, ok := acc.LookupTerminated(42); !ok {
		t.Fatalf("post-clear insert 42 not found")
	}
}

// TestTerminatedRing_PerUserIndependent verifies two accounts' rings
// are isolated.
func TestTerminatedRing_PerUserIndependent(t *testing.T) {
	a := newAccount("u1")
	b := newAccount("u2")
	a.RememberTerminated(mkEntry(1))
	b.RememberTerminated(mkEntry(2))
	if _, ok := a.LookupTerminated(2); ok {
		t.Fatalf("u1 should not see u2's id 2")
	}
	if _, ok := b.LookupTerminated(1); ok {
		t.Fatalf("u2 should not see u1's id 1")
	}
}

// TestTerminatedRing_MarketMakerCapacity verifies the larger market-
// maker cap takes effect when set at construction.
func TestTerminatedRing_MarketMakerCapacity(t *testing.T) {
	mm := newAccountWithCap("mm", TerminatedRingCapacityMarketMaker)
	if got := mm.TerminatedRingCapacity(); got != TerminatedRingCapacityMarketMaker {
		t.Fatalf("MM cap = %d, want %d", got, TerminatedRingCapacityMarketMaker)
	}
	// Fill just under MM cap but well over default cap.
	for i := uint64(1); i <= uint64(TerminatedRingCapacityDefault+100); i++ {
		mm.RememberTerminated(mkEntry(i))
	}
	// Oldest (id=1) should still be present because we haven't exceeded MM cap.
	if _, ok := mm.LookupTerminated(1); !ok {
		t.Fatalf("MM account prematurely evicted id 1 — cap not honoured")
	}
}

// TestTerminatedRing_ZeroCapFallsBackToDefault verifies the
// newAccountWithCap defensive path for cap <= 0.
func TestTerminatedRing_ZeroCapFallsBackToDefault(t *testing.T) {
	acc := newAccountWithCap("u1", 0)
	if got := acc.TerminatedRingCapacity(); got != TerminatedRingCapacityDefault {
		t.Fatalf("cap=0 should fall back to default, got %d", got)
	}
	acc.RememberTerminated(mkEntry(1))
	if _, ok := acc.LookupTerminated(1); !ok {
		t.Fatalf("remember+lookup should work with fallback cap")
	}
}

// TestTerminatedRing_ConcurrentRemember exercises mu under 8-way parallel
// writers pushing non-overlapping id ranges. Correctness assertion: total
// count equals min(sum, cap), and no panic / data race under `go test -race`.
func TestTerminatedRing_ConcurrentRemember(t *testing.T) {
	acc := newAccount("u1")
	cap := acc.TerminatedRingCapacity()
	const workers = 8
	const perWorker = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < perWorker; i++ {
				acc.RememberTerminated(mkEntry(base*uint64(perWorker) + i + 1))
			}
		}(uint64(w))
	}
	wg.Wait()
	snap := acc.RecentTerminatedSnapshot()
	total := workers * perWorker
	want := total
	if total > cap {
		want = cap
	}
	if len(snap) != want {
		t.Fatalf("concurrent fill produced %d entries, want %d", len(snap), want)
	}
}

// TestTerminatedRing_LookupSafeDuringWrite exercises the RLock/Lock path:
// many concurrent readers together with a writer; no assertion beyond
// "no race / no panic" (the -race detector does the work).
func TestTerminatedRing_LookupSafeDuringWrite(t *testing.T) {
	acc := newAccount("u1")
	const readers = 8
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = acc.LookupTerminated(42)
				}
			}
		}()
	}
	for i := uint64(1); i <= 500; i++ {
		acc.RememberTerminated(mkEntry(i))
	}
	close(stop)
	wg.Wait()
}
