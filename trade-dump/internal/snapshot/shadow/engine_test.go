package shadow

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/engine"
)

// TestShadow_ApplyFreezeAndCheckpointAdvancesWatermarks drives the
// engine through the two event shapes that bump engine-local
// bookkeeping (counterSeq, teWatermark, nextJournalOffset) plus
// ShardState (via the engine.ApplyCounterJournalEvent path).
func TestShadow_ApplyFreezeAndCheckpointAdvancesWatermarks(t *testing.T) {
	sh := New(42)

	// Apply a Freeze at offset 100 — should bump counterSeq,
	// journalOffset, eventsSinceLastSnapshot; insert an order.
	freeze := &eventpb.CounterJournalEvent{
		CounterSeqId: 7,
		Payload: &eventpb.CounterJournalEvent_Freeze{
			Freeze: &eventpb.FreezeEvent{
				UserId: "u1", OrderId: 1, Symbol: "BTC-USDT",
				Side:         eventpb.Side_SIDE_BUY,
				OrderType:    eventpb.OrderType_ORDER_TYPE_LIMIT,
				Price:        "10", Qty: "1",
				FreezeAsset:  "USDT",
				FreezeAmount: "10",
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId: "u1", Asset: "USDT",
					Available: "90", Frozen: "10", Version: 1,
				},
			},
		},
	}
	if err := sh.Apply(freeze, 100); err != nil {
		t.Fatalf("Apply freeze: %v", err)
	}
	if sh.CounterSeq() != 7 {
		t.Fatalf("counterSeq = %d, want 7", sh.CounterSeq())
	}
	if sh.NextJournalOffset() != 101 {
		t.Fatalf("nextJournalOffset = %d, want 101", sh.NextJournalOffset())
	}
	if sh.EventsSinceLastSnapshot() != 1 {
		t.Fatalf("eventsSinceLastSnapshot = %d, want 1", sh.EventsSinceLastSnapshot())
	}
	if sh.State().Orders().Get(1) == nil {
		t.Fatal("order 1 should have been inserted by Freeze apply")
	}

	// Checkpoint at offset 101 bumps teWatermark but no ShardState
	// mutation. counterSeq also bumped since the checkpoint carries
	// its own CounterSeqId.
	checkpoint := &eventpb.CounterJournalEvent{
		CounterSeqId: 9,
		Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
			TeCheckpoint: &eventpb.TECheckpointEvent{
				TePartition: 42, TeOffset: 555,
			},
		},
	}
	if err := sh.Apply(checkpoint, 101); err != nil {
		t.Fatalf("Apply checkpoint: %v", err)
	}
	if sh.CounterSeq() != 9 {
		t.Fatalf("counterSeq after checkpoint = %d, want 9", sh.CounterSeq())
	}
	p, off := sh.TeWatermark()
	if p != 42 || off != 555 {
		t.Fatalf("teWatermark = (%d, %d), want (42, 555)", p, off)
	}
	if sh.NextJournalOffset() != 102 {
		t.Fatalf("nextJournalOffset after checkpoint = %d, want 102", sh.NextJournalOffset())
	}
	if sh.EventsSinceLastSnapshot() != 2 {
		t.Fatalf("eventsSinceLastSnapshot = %d, want 2", sh.EventsSinceLastSnapshot())
	}
}

// TestShadow_CheckpointMonotonic verifies an out-of-order checkpoint
// (lower offset than seen) does NOT rewind the watermark. In
// practice counter-journal delivers in partition order so this
// shouldn't happen, but the guard is cheap and protects against
// surprise.
func TestShadow_CheckpointMonotonic(t *testing.T) {
	sh := New(5)
	cp1 := &eventpb.CounterJournalEvent{
		Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
			TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: 5, TeOffset: 1000},
		},
	}
	cp2 := &eventpb.CounterJournalEvent{
		Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
			TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: 5, TeOffset: 500},
		},
	}
	if err := sh.Apply(cp1, 1); err != nil {
		t.Fatalf("apply cp1: %v", err)
	}
	if err := sh.Apply(cp2, 2); err != nil {
		t.Fatalf("apply cp2: %v", err)
	}
	_, off := sh.TeWatermark()
	if off != 1000 {
		t.Fatalf("watermark regressed: got %d, want 1000 (monotonic)", off)
	}
}

// TestShadow_CounterSeqMonotonic verifies an older event's CounterSeqId
// does not regress engine.counterSeq.
func TestShadow_CounterSeqMonotonic(t *testing.T) {
	sh := New(0)
	hi := &eventpb.CounterJournalEvent{CounterSeqId: 100}
	lo := &eventpb.CounterJournalEvent{CounterSeqId: 50}
	_ = sh.Apply(hi, 0)
	_ = sh.Apply(lo, 1)
	if sh.CounterSeq() != 100 {
		t.Fatalf("counterSeq = %d, want 100", sh.CounterSeq())
	}
}

// TestShadow_CaptureProducesFullState exercises the
// Apply → Capture handoff end-to-end. After a few journal events
// the captured snapshot must carry the right counterSeq, the right
// te_watermark in Offsets, and the expected account + order rows.
func TestShadow_CaptureProducesFullState(t *testing.T) {
	sh := New(7)

	events := []struct {
		evt    *eventpb.CounterJournalEvent
		offset int64
	}{
		{
			evt: &eventpb.CounterJournalEvent{
				CounterSeqId: 1,
				Payload: &eventpb.CounterJournalEvent_Transfer{
					Transfer: &eventpb.TransferEvent{
						UserId: "u1", TransferId: "tx-1",
						Asset: "USDT", Amount: "1000",
						Type: eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
						BalanceAfter: &eventpb.BalanceSnapshot{
							UserId: "u1", Asset: "USDT",
							Available: "1000", Frozen: "0", Version: 1,
						},
					},
				},
			},
			offset: 200,
		},
		{
			evt: &eventpb.CounterJournalEvent{
				CounterSeqId: 2,
				Payload: &eventpb.CounterJournalEvent_Freeze{
					Freeze: &eventpb.FreezeEvent{
						UserId: "u1", OrderId: 10, Symbol: "BTC-USDT",
						Side:         eventpb.Side_SIDE_BUY,
						OrderType:    eventpb.OrderType_ORDER_TYPE_LIMIT,
						Price:        "50000", Qty: "0.1",
						FreezeAsset:  "USDT", FreezeAmount: "5000",
						BalanceAfter: &eventpb.BalanceSnapshot{
							UserId: "u1", Asset: "USDT",
							Available: "-4000", Frozen: "5000", Version: 2,
						},
					},
				},
			},
			offset: 201,
		},
		{
			evt: &eventpb.CounterJournalEvent{
				CounterSeqId: 3,
				Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
					TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: 7, TeOffset: 9999},
				},
			},
			offset: 202,
		},
	}
	for _, e := range events {
		if err := sh.Apply(e.evt, e.offset); err != nil {
			t.Fatalf("Apply @ %d: %v", e.offset, err)
		}
	}

	snap := sh.Capture(12345)
	if snap == nil {
		t.Fatal("Capture returned nil")
	}
	if snap.ShardID != 7 {
		t.Fatalf("ShardID = %d, want 7", snap.ShardID)
	}
	if snap.CounterSeq != 3 {
		t.Fatalf("CounterSeq = %d, want 3", snap.CounterSeq)
	}
	if snap.TimestampMS != 12345 {
		t.Fatalf("TimestampMS = %d, want 12345", snap.TimestampMS)
	}
	if snap.JournalOffset != 203 {
		t.Fatalf("JournalOffset = %d, want 203 (last offset 202 + 1)", snap.JournalOffset)
	}
	if len(snap.Offsets) != 1 || snap.Offsets[0].Partition != 7 || snap.Offsets[0].Offset != 9999 {
		t.Fatalf("Offsets = %+v, want [{7, 9999}]", snap.Offsets)
	}
	if len(snap.Accounts) != 1 || snap.Accounts[0].UserID != "u1" {
		t.Fatalf("Accounts = %+v, want single u1", snap.Accounts)
	}
	if !containsTransferID(snap.Accounts[0].RecentTransferIDs, "tx-1") {
		t.Fatalf("u1.RecentTransferIDs missing tx-1: %+v", snap.Accounts[0].RecentTransferIDs)
	}
	if len(snap.Orders) != 1 || snap.Orders[0].ID != 10 {
		t.Fatalf("Orders = %+v, want single order 10", snap.Orders)
	}
	if len(snap.Dedup) != 0 {
		t.Fatalf("Dedup = %+v, want empty (shadow never populates legacy dedup)", snap.Dedup)
	}
}

// TestShadow_CaptureBeforeAnyApplyProducesEmptySnapshot verifies
// Capture on a fresh engine is valid (no panics, empty projection)
// — relevant for the pipeline starting at cold restart before any
// record has arrived.
func TestShadow_CaptureBeforeAnyApplyProducesEmptySnapshot(t *testing.T) {
	sh := New(0)
	snap := sh.Capture(1)
	if snap == nil {
		t.Fatal("Capture on fresh engine returned nil")
	}
	if snap.CounterSeq != 0 || snap.JournalOffset != 0 {
		t.Fatalf("fresh capture = (seq=%d, jo=%d), want (0, 0)", snap.CounterSeq, snap.JournalOffset)
	}
	if len(snap.Accounts) != 0 || len(snap.Orders) != 0 {
		t.Fatalf("fresh capture not empty: accounts=%d orders=%d", len(snap.Accounts), len(snap.Orders))
	}
	if len(snap.Offsets) != 0 {
		t.Fatalf("fresh capture had offsets: %+v", snap.Offsets)
	}
}

// TestShadow_ClearEventsSinceLastSnapshot exercises the pipeline
// contract: after a successful Capture, the pipeline calls
// ClearEventsSinceLastSnapshot so the event-count trigger starts
// counting fresh.
func TestShadow_ClearEventsSinceLastSnapshot(t *testing.T) {
	sh := New(0)
	evt := &eventpb.CounterJournalEvent{
		CounterSeqId: 1,
		Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
			TeCheckpoint: &eventpb.TECheckpointEvent{TeOffset: 10},
		},
	}
	_ = sh.Apply(evt, 0)
	_ = sh.Apply(evt, 1)
	if sh.EventsSinceLastSnapshot() != 2 {
		t.Fatalf("before clear = %d, want 2", sh.EventsSinceLastSnapshot())
	}
	sh.ClearEventsSinceLastSnapshot()
	if sh.EventsSinceLastSnapshot() != 0 {
		t.Fatalf("after clear = %d, want 0", sh.EventsSinceLastSnapshot())
	}
	_ = sh.Apply(evt, 2)
	if sh.EventsSinceLastSnapshot() != 1 {
		t.Fatalf("post-clear re-count = %d, want 1", sh.EventsSinceLastSnapshot())
	}
}

// TestShadow_ApplyRejectsNilEventGracefully guards the fast path:
// a nil event should be a no-op, not a panic, so pipeline code
// that fans out unrelated errors doesn't poison the engine.
func TestShadow_ApplyRejectsNilEventGracefully(t *testing.T) {
	sh := New(0)
	if err := sh.Apply(nil, 0); err != nil {
		t.Fatalf("nil Apply: %v", err)
	}
	if sh.EventsSinceLastSnapshot() != 0 {
		t.Fatalf("events bumped on nil apply: %d", sh.EventsSinceLastSnapshot())
	}
}

// Helper: scan recent-transfer-IDs for presence. Avoids
// testify just for a contains check.
func containsTransferID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestShadow_StateTypeCompatibility is a compile-time-ish guard:
// if the shadow engine's State() return type drifts from
// engine.ShardState, this test body stops compiling. Cheaper than
// wiring `var _ = ...` at package scope since the latter executes
// at init time and nil-derefs.
func TestShadow_StateTypeCompatibility(t *testing.T) {
	var s *engine.ShardState = New(0).State()
	if s == nil {
		t.Fatal("State() must be non-nil on a fresh engine")
	}
}
