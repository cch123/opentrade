package counterstate

import (
	"fmt"
	"testing"
)

// TestTransferRing_FillBelowCap inserts fewer than the ring capacity; all
// ids must be remembered.
func TestTransferRing_FillBelowCap(t *testing.T) {
	acc := newAccount("u1")
	for i := 0; i < 10; i++ {
		acc.RememberTransfer(fmt.Sprintf("t-%d", i))
	}
	for i := 0; i < 10; i++ {
		if !acc.TransferSeen(fmt.Sprintf("t-%d", i)) {
			t.Errorf("t-%d not seen", i)
		}
	}
	if acc.TransferSeen("t-42") {
		t.Error("unknown id surfaced as seen")
	}
}

// TestTransferRing_OverflowEvictsOldest fills past cap and verifies the
// oldest ids fall out in insertion order.
func TestTransferRing_OverflowEvictsOldest(t *testing.T) {
	acc := newAccount("u1")
	total := TransferRingCapacity + 5
	for i := 0; i < total; i++ {
		acc.RememberTransfer(fmt.Sprintf("t-%d", i))
	}
	// First 5 should have been evicted.
	for i := 0; i < 5; i++ {
		if acc.TransferSeen(fmt.Sprintf("t-%d", i)) {
			t.Errorf("t-%d should have been evicted", i)
		}
	}
	// The remaining cap entries (5..total-1) are still there.
	for i := 5; i < total; i++ {
		if !acc.TransferSeen(fmt.Sprintf("t-%d", i)) {
			t.Errorf("t-%d should still be in the ring", i)
		}
	}
}

// TestTransferRing_DuplicateRememberIsNoop ensures remembering the same id
// twice does not double-occupy a slot.
func TestTransferRing_DuplicateRememberIsNoop(t *testing.T) {
	acc := newAccount("u1")
	acc.RememberTransfer("t-1")
	acc.RememberTransfer("t-1")
	// Fill the ring to cap-1 with unique ids.
	for i := 0; i < TransferRingCapacity-1; i++ {
		acc.RememberTransfer(fmt.Sprintf("x-%d", i))
	}
	if !acc.TransferSeen("t-1") {
		t.Fatal("t-1 should still be present — remembering a duplicate must not inflate size")
	}
}

// TestTransferRing_SnapshotRoundTrip verifies RecentTransferIDsSnapshot →
// RestoreRecentTransferIDs preserves membership order and behaviour.
func TestTransferRing_SnapshotRoundTrip(t *testing.T) {
	acc := newAccount("u1")
	// Full ring plus 3 extras to exercise wrap-around snapshot output.
	total := TransferRingCapacity + 3
	for i := 0; i < total; i++ {
		acc.RememberTransfer(fmt.Sprintf("t-%d", i))
	}
	snap := acc.RecentTransferIDsSnapshot()
	if len(snap) != TransferRingCapacity {
		t.Fatalf("snapshot length = %d, want %d", len(snap), TransferRingCapacity)
	}
	// Oldest remaining id is t-3 (0..2 evicted); newest is t-(total-1).
	if snap[0] != "t-3" {
		t.Errorf("snapshot[0] = %q, want t-3", snap[0])
	}
	if snap[len(snap)-1] != fmt.Sprintf("t-%d", total-1) {
		t.Errorf("snapshot[last] = %q, want t-%d", snap[len(snap)-1], total-1)
	}

	// Restore into a fresh account and verify membership + eviction order.
	other := newAccount("u1")
	other.RestoreRecentTransferIDs(snap)
	for i := 3; i < total; i++ {
		if !other.TransferSeen(fmt.Sprintf("t-%d", i)) {
			t.Errorf("restored: t-%d missing", i)
		}
	}
	// Adding one more id should evict the oldest restored (t-3).
	other.RememberTransfer("post-restore")
	if other.TransferSeen("t-3") {
		t.Error("post-restore insert should have evicted t-3")
	}
	if !other.TransferSeen("post-restore") {
		t.Error("post-restore insert lost")
	}
}

// TestTransferRing_PerUserIndependent verifies two users' rings don't
// cross-contaminate.
func TestTransferRing_PerUserIndependent(t *testing.T) {
	state := NewShardState(0)
	state.Account("u1").RememberTransfer("shared-id")
	if state.Account("u2").TransferSeen("shared-id") {
		t.Error("u2 should not see u1's id")
	}
}
