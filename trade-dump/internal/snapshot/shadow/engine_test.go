package shadow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
				Side:      eventpb.Side_SIDE_BUY,
				OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
				Price:     "10", Qty: "1",
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
						Side:      eventpb.Side_SIDE_BUY,
						OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
						Price:     "50000", Qty: "0.1",
						FreezeAsset: "USDT", FreezeAmount: "5000",
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

// TestShadow_RestoreFromSnapshotRoundTrip exercises the
// Apply → Capture → RestoreFromSnapshot → Apply cycle that
// pipeline restarts go through. The post-restore engine must
// resume from the captured watermark and continue to produce
// identical state for subsequent events.
func TestShadow_RestoreFromSnapshotRoundTrip(t *testing.T) {
	a := New(3)
	// Seed a few events.
	deposit := &eventpb.CounterJournalEvent{
		CounterSeqId: 1,
		Payload: &eventpb.CounterJournalEvent_Transfer{
			Transfer: &eventpb.TransferEvent{
				UserId: "u1", TransferId: "tx-1", Asset: "USDT", Amount: "100",
				Type: eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId: "u1", Asset: "USDT",
					Available: "100", Frozen: "0", Version: 1,
				},
			},
		},
	}
	checkpoint := &eventpb.CounterJournalEvent{
		CounterSeqId: 2,
		Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
			TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: 3, TeOffset: 777},
		},
	}
	if err := a.Apply(deposit, 10); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(checkpoint, 11); err != nil {
		t.Fatal(err)
	}
	snap := a.Capture(1)
	a.ClearEventsSinceLastSnapshot()

	// New engine restores.
	b := New(3)
	if err := b.RestoreFromSnapshot(snap); err != nil {
		t.Fatalf("RestoreFromSnapshot: %v", err)
	}
	if b.CounterSeq() != 2 {
		t.Fatalf("restored counterSeq = %d, want 2", b.CounterSeq())
	}
	if b.NextJournalOffset() != 12 {
		t.Fatalf("restored NextJournalOffset = %d, want 12", b.NextJournalOffset())
	}
	_, off := b.TeWatermark()
	if off != 777 {
		t.Fatalf("restored teWatermark = %d, want 777", off)
	}
	if b.EventsSinceLastSnapshot() != 0 {
		t.Fatalf("restored events counter = %d, want 0", b.EventsSinceLastSnapshot())
	}
	if bal := b.State().Balance("u1", "USDT"); bal.Available.String() != "100" {
		t.Fatalf("restored u1 USDT available = %s, want 100", bal.Available)
	}

	// Apply a further event — engine b should behave identically
	// to engine a continuing from the same point.
	freeze := &eventpb.CounterJournalEvent{
		CounterSeqId: 3,
		Payload: &eventpb.CounterJournalEvent_Freeze{
			Freeze: &eventpb.FreezeEvent{
				UserId: "u1", OrderId: 42, Symbol: "BTC-USDT",
				Side:      eventpb.Side_SIDE_BUY,
				OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
				Price:     "50000", Qty: "0.001",
				FreezeAsset: "USDT", FreezeAmount: "50",
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId: "u1", Asset: "USDT",
					Available: "50", Frozen: "50", Version: 2,
				},
			},
		},
	}
	if err := b.Apply(freeze, 12); err != nil {
		t.Fatal(err)
	}
	// Apply the same event on engine a (for comparison).
	if err := a.Apply(freeze, 12); err != nil {
		t.Fatal(err)
	}
	if b.CounterSeq() != a.CounterSeq() {
		t.Fatalf("post-restore counterSeq diverged: a=%d b=%d", a.CounterSeq(), b.CounterSeq())
	}
	if b.NextJournalOffset() != a.NextJournalOffset() {
		t.Fatalf("post-restore NextJournalOffset diverged: a=%d b=%d", a.NextJournalOffset(), b.NextJournalOffset())
	}
	if b.State().Balance("u1", "USDT").Available.String() != "50" {
		t.Fatal("post-restore balance diverged from a")
	}
	if b.State().Orders().Get(42) == nil {
		t.Fatal("post-restore order 42 missing")
	}
}

// TestShadow_RestoreFromSnapshotRejectsNonEmpty guards the
// invariant: RestoreFromSnapshot must run on a fresh engine
// (RestoreState errors on non-empty state). This prevents a
// subtle bug where a pipeline accidentally restores an already-
// active engine.
func TestShadow_RestoreFromSnapshotRejectsNonEmpty(t *testing.T) {
	e := New(0)
	deposit := &eventpb.CounterJournalEvent{
		CounterSeqId: 1,
		Payload: &eventpb.CounterJournalEvent_Transfer{
			Transfer: &eventpb.TransferEvent{
				UserId: "u1", TransferId: "tx-1", Asset: "USDT", Amount: "1",
				Type: eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId: "u1", Asset: "USDT", Available: "1",
				},
			},
		},
	}
	_ = e.Apply(deposit, 1)
	snap := e.Capture(1)
	if err := e.RestoreFromSnapshot(snap); err == nil {
		t.Fatal("expected RestoreFromSnapshot to reject non-empty state")
	}
}

// TestShadow_RestoreFromSnapshotNilRejected guards the nil case
// explicitly — pipeline shouldn't silently start fresh just
// because Load returned nil.
func TestShadow_RestoreFromSnapshotNilRejected(t *testing.T) {
	e := New(0)
	if err := e.RestoreFromSnapshot(nil); err == nil {
		t.Fatal("expected RestoreFromSnapshot(nil) to error")
	}
}

// TestShadow_ApplyStartupFenceIsNoOp pins ADR-0064 §1.2 + §3.1 on the
// shadow-engine side: a StartupFence sentinel advances the journal
// cursor (nextJournalOffset) and the event counter
// (eventsSinceLastSnapshot) just like any other record, but DOES NOT
// touch ShardState, counterSeq, or teWatermark. Its whole point is
// its physical presence on the partition (fence + LEO stability,
// §3.1 of the ADR).
//
// If this test fails because the shadow Apply started mutating state
// on StartupFence, the on-demand snapshot contract is violated — a
// startup-triggered sentinel must not poison the snapshot content.
func TestShadow_ApplyStartupFenceIsNoOp(t *testing.T) {
	sh := New(42)

	// Seed some state so "unchanged" is observable.
	seed := &eventpb.CounterJournalEvent{
		CounterSeqId: 5,
		Payload: &eventpb.CounterJournalEvent_Freeze{
			Freeze: &eventpb.FreezeEvent{
				UserId: "u1", OrderId: 1, Symbol: "BTC-USDT",
				Side:      eventpb.Side_SIDE_BUY,
				OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
				Price:     "10", Qty: "1",
				FreezeAsset:  "USDT",
				FreezeAmount: "10",
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId: "u1", Asset: "USDT",
					Available: "90", Frozen: "10", Version: 1,
				},
			},
		},
	}
	if err := sh.Apply(seed, 100); err != nil {
		t.Fatalf("seed apply: %v", err)
	}
	csBefore := sh.CounterSeq()
	teP, teO := sh.TeWatermark()
	balBefore := sh.State().Balance("u1", "USDT")
	// Snapshot order fields as VALUES, not the *Order pointer. The
	// store returns a live pointer; keeping only the pointer would
	// alias any in-place mutation by Apply and make the post-check
	// tautological (codex review catch).
	orderPtr := sh.State().Orders().Get(1)
	if orderPtr == nil {
		t.Fatal("seed order missing")
	}
	orderStatusBefore := orderPtr.Status
	orderFilledBefore := orderPtr.FilledQty
	orderFrozenSpentBefore := orderPtr.FrozenSpent

	// Apply a StartupFence at offset 101. CounterSeqId is 0 per
	// ADR-0064 §1.2 convention — sentinels do not allocate a
	// counter_seq. Shadow engine must:
	//   - advance nextJournalOffset to 102
	//   - advance eventsSinceLastSnapshot by 1
	//   - NOT advance counterSeq (0 < current 5)
	//   - NOT touch teWatermark
	//   - NOT touch ShardState (balance, orders)
	fence := &eventpb.CounterJournalEvent{
		CounterSeqId: 0,
		Payload: &eventpb.CounterJournalEvent_StartupFence{
			StartupFence: &eventpb.StartupFenceEvent{
				NodeId: "counter-node-B", Epoch: 7,
				TsMs: 1_700_000_000_000,
			},
		},
	}
	if err := sh.Apply(fence, 101); err != nil {
		t.Fatalf("apply StartupFence: %v", err)
	}

	// Journal cursor + event count advance.
	if sh.NextJournalOffset() != 102 {
		t.Fatalf("nextJournalOffset = %d, want 102", sh.NextJournalOffset())
	}
	if sh.EventsSinceLastSnapshot() != 2 {
		t.Fatalf("eventsSinceLastSnapshot = %d, want 2", sh.EventsSinceLastSnapshot())
	}

	// counterSeq unchanged (fence carried 0 which < existing 5).
	if sh.CounterSeq() != csBefore {
		t.Fatalf("counterSeq mutated by StartupFence: %d → %d", csBefore, sh.CounterSeq())
	}
	// teWatermark unchanged.
	teP2, teO2 := sh.TeWatermark()
	if teP2 != teP || teO2 != teO {
		t.Fatalf("teWatermark mutated by StartupFence: (%d,%d) → (%d,%d)", teP, teO, teP2, teO2)
	}
	// ShardState untouched.
	balAfter := sh.State().Balance("u1", "USDT")
	if balAfter.Available.Cmp(balBefore.Available) != 0 ||
		balAfter.Frozen.Cmp(balBefore.Frozen) != 0 ||
		balAfter.Version != balBefore.Version {
		t.Fatalf("balance mutated by StartupFence: %+v → %+v", balBefore, balAfter)
	}
	orderAfter := sh.State().Orders().Get(1)
	if orderAfter == nil {
		t.Fatal("order disappeared after StartupFence apply")
	}
	if orderAfter.Status != orderStatusBefore {
		t.Fatalf("order.Status mutated by StartupFence: %v → %v", orderStatusBefore, orderAfter.Status)
	}
	if orderAfter.FilledQty.Cmp(orderFilledBefore) != 0 {
		t.Fatalf("order.FilledQty mutated by StartupFence: %s → %s", orderFilledBefore.String(), orderAfter.FilledQty.String())
	}
	if orderAfter.FrozenSpent.Cmp(orderFrozenSpentBefore) != 0 {
		t.Fatalf("order.FrozenSpent mutated by StartupFence: %s → %s", orderFrozenSpentBefore.String(), orderAfter.FrozenSpent.String())
	}
}

// TestShadow_ApplyUnknownPayloadIsNoOp locks ADR-0064 F10 forward-compat
// on the shadow side. If trade-dump upgrades lag counter deployments
// (or vice versa) and sees a journal event with an unknown oneof
// variant, the shadow Apply must not panic, must not error, and must
// still advance the cursor so consumption doesn't stall forever on
// that record.
func TestShadow_ApplyUnknownPayloadIsNoOp(t *testing.T) {
	sh := New(7)

	// Envelope with Payload deliberately unset. This is the
	// programmatic stand-in for "a variant this binary doesn't
	// compile yet".
	evt := &eventpb.CounterJournalEvent{CounterSeqId: 3}
	if err := sh.Apply(evt, 42); err != nil {
		t.Fatalf("Apply(unknown payload): %v", err)
	}

	if sh.NextJournalOffset() != 43 {
		t.Fatalf("nextJournalOffset = %d, want 43 (cursor must advance past unknown)", sh.NextJournalOffset())
	}
	if sh.EventsSinceLastSnapshot() != 1 {
		t.Fatalf("eventsSinceLastSnapshot = %d, want 1", sh.EventsSinceLastSnapshot())
	}
	// counterSeq still advances (envelope-level field, not payload).
	if sh.CounterSeq() != 3 {
		t.Fatalf("counterSeq = %d, want 3", sh.CounterSeq())
	}
}

// TestShadow_ApplyNilRecord guards the cheapest degenerate input —
// a nil CounterJournalEvent. Historical behaviour: return nil and do
// not advance the cursor (the record "didn't happen" from shadow's
// viewpoint). Locking this down prevents accidental regression if a
// future change starts treating nil as "unknown → advance cursor".
func TestShadow_ApplyNilRecord(t *testing.T) {
	sh := New(0)
	if err := sh.Apply(nil, 500); err != nil {
		t.Fatalf("Apply(nil): %v", err)
	}
	if sh.NextJournalOffset() != 0 {
		t.Fatalf("nextJournalOffset = %d, want 0 (nil record must not advance)", sh.NextJournalOffset())
	}
}

// -----------------------------------------------------------------------------
// ADR-0064 M1c-α concurrency infrastructure tests
// -----------------------------------------------------------------------------

// settlementAt is a test helper producing a well-formed Settlement
// journal event whose balance_after is internally consistent enough
// to pass the engine apply path without error. Used to drive Apply
// in concurrency stress tests where we want a non-trivial state
// mutation (not just no-op events) so a racing Capture has an actual
// walk to perform.
func settlementAt(user string, seq uint64, orderID uint64, balVer uint64) *eventpb.CounterJournalEvent {
	return &eventpb.CounterJournalEvent{
		CounterSeqId: seq,
		Payload: &eventpb.CounterJournalEvent_Settlement{
			Settlement: &eventpb.SettlementEvent{
				UserId:     user,
				OrderId:    orderID,
				Symbol:     "BTC-USDT",
				Side:       eventpb.Side_SIDE_BUY,
				Price:      "10",
				Qty:        "0",
				DeltaBase:  "0",
				DeltaQuote: "0",
				BaseBalanceAfter: &eventpb.BalanceSnapshot{
					UserId: user, Asset: "BTC",
					Available: "1", Frozen: "0", Version: balVer,
				},
				QuoteBalanceAfter: &eventpb.BalanceSnapshot{
					UserId: user, Asset: "USDT",
					Available: "1000", Frozen: "0", Version: balVer,
				},
			},
		},
	}
}

// TestShadow_PublishedOffsetTracksApply pins ADR-0064 §2.7 contract:
// every successful Apply must Store the advanced cursor to the atomic
// mirror before returning, so a reader using PublishedOffset() sees
// a value consistent with what NextJournalOffset() would report on
// the Run goroutine. Failed Apply (malformed event) must NOT advance
// publishedOffset even if nextJournalOffset already moved — keeping
// the atomic mirror's invariant "observed here ⇒ committed to
// state".
func TestShadow_PublishedOffsetTracksApply(t *testing.T) {
	sh := New(0)
	if sh.PublishedOffset() != 0 {
		t.Fatalf("initial publishedOffset = %d, want 0", sh.PublishedOffset())
	}

	// Successful apply: cursor should advance on both fields.
	ok := &eventpb.CounterJournalEvent{
		CounterSeqId: 1,
		Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
			TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: 0, TeOffset: 10},
		},
	}
	if err := sh.Apply(ok, 50); err != nil {
		t.Fatalf("Apply(ok): %v", err)
	}
	if got, want := sh.PublishedOffset(), int64(51); got != want {
		t.Fatalf("publishedOffset after ok Apply = %d, want %d", got, want)
	}
	if got, want := sh.NextJournalOffset(), int64(51); got != want {
		t.Fatalf("nextJournalOffset after ok Apply = %d, want %d", got, want)
	}

	// StartupFence (ADR-0064 no-op apply) still advances publishedOffset
	// — its whole purpose is to mark a cursor, so PublishedOffset must
	// follow.
	fence := &eventpb.CounterJournalEvent{
		CounterSeqId: 0,
		Payload: &eventpb.CounterJournalEvent_StartupFence{
			StartupFence: &eventpb.StartupFenceEvent{
				NodeId: "n", Epoch: 1, TsMs: 1,
			},
		},
	}
	if err := sh.Apply(fence, 51); err != nil {
		t.Fatalf("Apply(fence): %v", err)
	}
	if got, want := sh.PublishedOffset(), int64(52); got != want {
		t.Fatalf("publishedOffset after fence = %d, want %d", got, want)
	}
}

// TestShadow_PublishedOffsetNotAdvancedOnApplyError locks the
// contract ADR-0064 WaitAppliedTo relies on: PublishedOffset() only
// moves forward after a fully successful Apply. A malformed event
// that fails inside engine.ApplyCounterJournalEvent must leave the
// atomic mirror un-advanced even though nextJournalOffset has been
// pre-set for restart purposes. Without this invariant a waiter
// would falsely conclude a corrupt record was processed.
func TestShadow_PublishedOffsetNotAdvancedOnApplyError(t *testing.T) {
	sh := New(0)
	// Prime with one good apply so publishedOffset is > 0.
	if err := sh.Apply(settlementAt("u1", 1, 0, 1), 100); err != nil {
		t.Fatalf("seed apply: %v", err)
	}
	seedPublished := sh.PublishedOffset()
	if seedPublished != 101 {
		t.Fatalf("seed publishedOffset = %d, want 101", seedPublished)
	}

	// Malformed event: BalanceSnapshot with unparseable Available
	// → applyBalanceSnapshot → dec.Parse error. nextJournalOffset
	// WILL have been pre-advanced inside Apply; publishedOffset
	// must NOT.
	bad := &eventpb.CounterJournalEvent{
		CounterSeqId: 2,
		Payload: &eventpb.CounterJournalEvent_Settlement{
			Settlement: &eventpb.SettlementEvent{
				UserId: "u1", OrderId: 1, Symbol: "BTC-USDT",
				BaseBalanceAfter: &eventpb.BalanceSnapshot{
					UserId: "u1", Asset: "BTC",
					Available: "not-a-decimal", // force parse error
					Frozen:    "0",
					Version:   2,
				},
			},
		},
	}
	err := sh.Apply(bad, 101)
	if err == nil {
		t.Fatal("expected error for malformed event")
	}
	if sh.PublishedOffset() != seedPublished {
		t.Fatalf("publishedOffset advanced on failed Apply: %d → %d",
			seedPublished, sh.PublishedOffset())
	}
}

// TestShadow_WaitAppliedTo_FastPath — target already reached at call
// time returns nil without starting a ticker. No mocks needed; the
// behaviour is observable as a sub-millisecond return.
func TestShadow_WaitAppliedTo_FastPath(t *testing.T) {
	sh := New(0)
	if err := sh.Apply(settlementAt("u1", 1, 0, 1), 100); err != nil {
		t.Fatalf("apply: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := sh.WaitAppliedTo(ctx, 101); err != nil {
		t.Fatalf("WaitAppliedTo fast path: %v", err)
	}
	if dur := time.Since(start); dur > 5*time.Millisecond {
		t.Fatalf("fast path unexpectedly slow: %v", dur)
	}
}

// TestShadow_WaitAppliedTo_WakesOnApply proves the poll loop picks
// up a concurrent Apply's cursor advance within the 5ms poll
// interval. The waiter starts with target > publishedOffset, a
// helper goroutine drives one Apply after a short delay, and
// WaitAppliedTo must return nil (not ctx error).
func TestShadow_WaitAppliedTo_WakesOnApply(t *testing.T) {
	sh := New(0)
	// Seed one apply so subsequent concurrent Apply isn't the
	// very first (exercises the steady-state Apply path, not the
	// zero-init edge case).
	if err := sh.Apply(settlementAt("u1", 1, 0, 1), 100); err != nil {
		t.Fatalf("seed: %v", err)
	}

	target := int64(200) // requires Apply at offset 199 to reach 200
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		done <- sh.WaitAppliedTo(ctx, target)
	}()

	// Give the waiter a chance to enter the poll loop, then Apply
	// enough records to advance publishedOffset to ≥ target.
	time.Sleep(15 * time.Millisecond)
	if err := sh.Apply(settlementAt("u1", 2, 0, 2), 199); err != nil {
		t.Fatalf("advance apply: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitAppliedTo returned err despite Apply advance: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("WaitAppliedTo did not return within 1s of Apply advancing cursor")
	}
}

// TestShadow_WaitAppliedTo_CtxCancel verifies ctx.Done drives
// termination when publishedOffset never reaches target. ADR-0064
// §2.5 relies on this to enforce the 2s handler-side WaitApply
// budget and return DeadlineExceeded so Counter falls back.
func TestShadow_WaitAppliedTo_CtxCancel(t *testing.T) {
	sh := New(0)
	// publishedOffset stays at 0 — target 100 will never arrive.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := sh.WaitAppliedTo(ctx, 100)
	if err == nil {
		t.Fatal("expected ctx error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

// TestShadow_RestoreFromSnapshotSeedsPublishedOffset pins ADR-0064
// M1c-α correctness after a pipeline restart: RestoreFromSnapshot
// MUST seed the publishedOffset atomic mirror from snap.JournalOffset,
// otherwise WaitAppliedTo on an idle vshard (no post-restore Apply
// yet) would block until timeout and needlessly drive Counter into
// the legacy fallback path even though the shadow is already caught
// up to the restored cursor. (codex review catch)
func TestShadow_RestoreFromSnapshotSeedsPublishedOffset(t *testing.T) {
	sh := New(0)
	// Craft a minimal snapshot. JournalOffset is the key field we
	// care about; CaptureFromState over an empty state returns
	// zeroed account/order slices but the cursor is preserved.
	seeded := sh.Capture(time.Now().UnixMilli())
	seeded.JournalOffset = 12345
	seeded.CounterSeq = 42

	// Fresh engine, seed via Restore.
	sh2 := New(0)
	if err := sh2.RestoreFromSnapshot(seeded); err != nil {
		t.Fatalf("RestoreFromSnapshot: %v", err)
	}
	if got, want := sh2.PublishedOffset(), int64(12345); got != want {
		t.Fatalf("publishedOffset after Restore = %d, want %d", got, want)
	}
	// WaitAppliedTo must succeed immediately for the restored
	// target — this is the scenario that used to time out.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := sh2.WaitAppliedTo(ctx, 12345); err != nil {
		t.Fatalf("WaitAppliedTo on restored cursor: %v", err)
	}
}

// TestShadow_WaitAppliedTo_ReachesTargetAtDeadline closes the
// codex-caught select race: when publishedOffset reaches target in
// the same scheduling moment the deadline expires, WaitAppliedTo
// must still report success rather than DeadlineExceeded. We drive
// this by pre-setting publishedOffset before WaitAppliedTo is called
// with an already-expired context — without the post-Done re-check,
// Go's random select between a ready tk.C and ready ctx.Done would
// flake roughly 50% toward ctx.Err.
func TestShadow_WaitAppliedTo_ReachesTargetAtDeadline(t *testing.T) {
	sh := New(0)
	if err := sh.Apply(settlementAt("u1", 1, 0, 1), 99); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// publishedOffset is now 100, target is 100. Context is
	// already cancelled (expired deadline). The post-Done re-check
	// must see publishedOffset >= target and return nil.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // ctx.Done() fires immediately

	if err := sh.WaitAppliedTo(ctx, 100); err != nil {
		t.Fatalf("WaitAppliedTo should report success when target already met: %v", err)
	}
}

// TestShadow_ApplyCaptureConcurrent is the M1c-α correctness
// linchpin: the pipeline's Run goroutine runs Apply in a tight loop
// while the on-demand RPC handler's Capture fires repeatedly from a
// separate goroutine. With -race the shadow mutex must prevent any
// torn state read during CaptureFromState's deep-copy walk.
//
// Without the mutex the race detector would flag concurrent Apply
// mutations + Capture reads of ShardState / counterSeq /
// nextJournalOffset / teWatermark. With it, every Capture observes
// a snapshot taken at a well-defined point between two Applies.
//
// Invariants asserted after the concurrent run:
//
//   - Every captured snapshot had JournalOffset ≤ the final
//     cursor (no future reads).
//   - CounterSeq on each snapshot was monotone non-decreasing
//     across the capture sequence (no rewinds).
//   - Final publishedOffset matches NextJournalOffset.
func TestShadow_ApplyCaptureConcurrent(t *testing.T) {
	sh := New(0)

	const (
		applies  = 2000
		captures = 50
	)

	var wg sync.WaitGroup

	// Apply goroutine (mimics pipeline Run): drives offsets 0..applies-1
	// with real Settlement events so CaptureFromState has a non-trivial
	// walk each time.
	applyDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(applyDone)
		for i := int64(0); i < applies; i++ {
			evt := settlementAt("u1", uint64(i+1), 0, uint64(i+1))
			if err := sh.Apply(evt, i); err != nil {
				t.Errorf("apply %d: %v", i, err)
				return
			}
		}
	}()

	// Capture goroutine (mimics on-demand RPC handler).
	var captureCount atomic.Int64
	var lastSeq atomic.Uint64
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(200 * time.Microsecond)
		defer ticker.Stop()
		maxCap := int64(captures)
		for {
			select {
			case <-applyDone:
				// Take one last capture post-Apply so we always observe
				// the final state in our monotonicity check.
				snap := sh.Capture(time.Now().UnixMilli())
				if snap.CounterSeq < lastSeq.Load() {
					t.Errorf("final snapshot counter_seq %d < previously observed %d",
						snap.CounterSeq, lastSeq.Load())
				}
				if snap.JournalOffset > applies {
					t.Errorf("final snapshot journal_offset %d > total applies %d",
						snap.JournalOffset, applies)
				}
				return
			case <-ticker.C:
				if captureCount.Load() >= maxCap {
					// Keep polling applyDone to exit cleanly.
					continue
				}
				snap := sh.Capture(time.Now().UnixMilli())
				prev := lastSeq.Load()
				if snap.CounterSeq < prev {
					t.Errorf("capture counter_seq went backwards: %d < %d",
						snap.CounterSeq, prev)
				}
				for !lastSeq.CompareAndSwap(prev, snap.CounterSeq) {
					prev = lastSeq.Load()
					if snap.CounterSeq < prev {
						break // another goroutine overtook us
					}
				}
				if snap.JournalOffset > applies {
					t.Errorf("capture journal_offset %d > total applies %d",
						snap.JournalOffset, applies)
				}
				captureCount.Add(1)
			}
		}
	}()

	wg.Wait()

	if got := sh.PublishedOffset(); got != applies {
		t.Fatalf("final publishedOffset = %d, want %d", got, applies)
	}
	if sh.NextJournalOffset() != applies {
		t.Fatalf("final nextJournalOffset = %d, want %d", sh.NextJournalOffset(), applies)
	}
}
