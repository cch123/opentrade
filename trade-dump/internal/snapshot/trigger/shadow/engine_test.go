package shadow

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
)

func update(id, seq uint64, status eventpb.TriggerEventStatus) *eventpb.TriggerUpdate {
	return &eventpb.TriggerUpdate{
		Id:           id,
		UserId:       "u1",
		Symbol:       "BTC-USDT",
		Status:       status,
		StopPrice:    "100",
		Qty:          "1",
		TriggerSeqId: seq,
	}
}

func TestApply_PendingUpsert(t *testing.T) {
	e := New(0)
	if err := e.ApplyTriggerUpdate(update(1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING), 0, 100); err != nil {
		t.Fatal(err)
	}
	snap := e.Capture(0, false)
	if len(snap.Pending) != 1 || snap.Pending[0].Id != 1 {
		t.Fatalf("pending = %+v, want [{id=1}]", snap.Pending)
	}
	if len(snap.Terminals) != 0 {
		t.Fatalf("terminals = %+v, want empty", snap.Terminals)
	}
	if got := snap.TriggerEventOffsets[0]; got != 101 {
		t.Errorf("partition 0 cursor = %d, want 101", got)
	}
}

func TestApply_TerminalMovesToRing(t *testing.T) {
	e := New(0)
	_ = e.ApplyTriggerUpdate(update(1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING), 0, 0)
	_ = e.ApplyTriggerUpdate(update(1, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED), 0, 1)
	snap := e.Capture(0, false)
	if len(snap.Pending) != 0 {
		t.Fatalf("pending = %+v, want empty after terminal", snap.Pending)
	}
	if len(snap.Terminals) != 1 || snap.Terminals[0].Id != 1 {
		t.Fatalf("terminals = %+v, want [{id=1}]", snap.Terminals)
	}
	if got := uint32(eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED); snap.Terminals[0].Status != got {
		t.Errorf("terminal status = %d, want %d", snap.Terminals[0].Status, got)
	}
}

// Successive applies on the same trigger_id with increasing offsets
// just overwrite the pending entry (no per-id LWW — Kafka assign-mode
// ordering is the only invariant the shadow relies on).
func TestApply_PendingOverwrite(t *testing.T) {
	e := New(0)
	first := update(1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING)
	first.StopPrice = "100"
	_ = e.ApplyTriggerUpdate(first, 0, 0)
	second := update(1, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING)
	second.StopPrice = "120"
	_ = e.ApplyTriggerUpdate(second, 0, 1)
	snap := e.Capture(0, false)
	if len(snap.Pending) != 1 {
		t.Fatalf("pending = %+v, want 1 entry", snap.Pending)
	}
	if snap.Pending[0].StopPrice != "120" {
		t.Errorf("pending stop_price = %q, want 120", snap.Pending[0].StopPrice)
	}
}

func TestApply_TerminalRingEvictionFIFO(t *testing.T) {
	e := New(2)
	for i := uint64(1); i <= 4; i++ {
		_ = e.ApplyTriggerUpdate(update(i, i, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED), 0, int64(i-1))
	}
	snap := e.Capture(0, false)
	if len(snap.Terminals) != 2 {
		t.Fatalf("terminals len = %d, want 2 (cap)", len(snap.Terminals))
	}
	// Oldest two (id=1, id=2) evicted; remaining must be id=3, id=4 in
	// that order (FIFO).
	if snap.Terminals[0].Id != 3 || snap.Terminals[1].Id != 4 {
		t.Fatalf("terminals = [%d, %d], want [3, 4]", snap.Terminals[0].Id, snap.Terminals[1].Id)
	}
}

// Per-partition cursor: applying on partition 0 doesn't move partition
// 1's cursor; per-partition advance is monotonic.
func TestApply_PerPartitionCursor(t *testing.T) {
	e := New(0)
	_ = e.ApplyTriggerUpdate(update(1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING), 0, 100)
	_ = e.ApplyTriggerUpdate(update(2, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING), 1, 200)
	if got := e.NextTriggerEventOffset(0); got != 101 {
		t.Errorf("partition 0 cursor = %d, want 101", got)
	}
	if got := e.NextTriggerEventOffset(1); got != 201 {
		t.Errorf("partition 1 cursor = %d, want 201", got)
	}
	// Apply at offset 50 on partition 0 must NOT roll cursor back.
	_ = e.ApplyTriggerUpdate(update(3, 3, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING), 0, 50)
	if got := e.NextTriggerEventOffset(0); got != 101 {
		t.Fatalf("partition 0 cursor rolled back: %d, want 101", got)
	}
}

func TestApply_MarketCheckpointAdvancesOffsets(t *testing.T) {
	e := New(0)
	c := &eventpb.TriggerMarketCheckpointEvent{
		MarketOffsets: map[int32]int64{0: 100, 2: 200},
		TsUnixMs:      1,
	}
	if err := e.ApplyMarketCheckpoint(c, 0, 7); err != nil {
		t.Fatal(err)
	}
	snap := e.Capture(0, false)
	if got := snap.Offsets[0]; got != 100 {
		t.Errorf("partition 0 = %d, want 100", got)
	}
	if got := snap.Offsets[2]; got != 200 {
		t.Errorf("partition 2 = %d, want 200", got)
	}
	if got := e.NextTriggerEventOffset(0); got != 8 {
		t.Errorf("partition 0 cursor = %d, want 8 (kafka offset 7 + 1)", got)
	}
}

// Per-partition LWW on market offsets — older checkpoint does not move
// the cursor backwards.
func TestApply_MarketCheckpointPerPartitionLWW(t *testing.T) {
	e := New(0)
	_ = e.ApplyMarketCheckpoint(&eventpb.TriggerMarketCheckpointEvent{
		MarketOffsets: map[int32]int64{0: 500},
	}, 0, 0)
	_ = e.ApplyMarketCheckpoint(&eventpb.TriggerMarketCheckpointEvent{
		MarketOffsets: map[int32]int64{0: 300, 1: 400}, // 0 is stale, 1 is fresh
	}, 0, 1)
	snap := e.Capture(0, false)
	if got := snap.Offsets[0]; got != 500 {
		t.Errorf("partition 0 went backwards: %d, want 500", got)
	}
	if got := snap.Offsets[1]; got != 400 {
		t.Errorf("partition 1 = %d, want 400", got)
	}
}

func TestCapture_ResetCounter(t *testing.T) {
	e := New(0)
	for i := uint64(1); i <= 3; i++ {
		_ = e.ApplyTriggerUpdate(update(i, i, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING), 0, int64(i-1))
	}
	if got := e.EventsSinceLastSnapshot(); got != 3 {
		t.Fatalf("events = %d, want 3", got)
	}
	_ = e.Capture(0, true)
	if got := e.EventsSinceLastSnapshot(); got != 0 {
		t.Fatalf("events after Capture(reset=true) = %d, want 0", got)
	}
}

func TestCapture_NoResetWhenFalse(t *testing.T) {
	e := New(0)
	_ = e.ApplyTriggerUpdate(update(1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING), 0, 0)
	_ = e.Capture(0, false)
	if got := e.EventsSinceLastSnapshot(); got != 1 {
		t.Errorf("events after Capture(reset=false) = %d, want 1", got)
	}
}

func TestRestore_RoundTrip(t *testing.T) {
	src := New(0)
	_ = src.ApplyTriggerUpdate(update(1, 1, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING), 0, 0)
	_ = src.ApplyTriggerUpdate(update(2, 2, eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED), 1, 5)
	_ = src.ApplyMarketCheckpoint(&eventpb.TriggerMarketCheckpointEvent{
		MarketOffsets: map[int32]int64{0: 50},
	}, 0, 2)
	snap := src.Capture(42, true)

	dst := New(0)
	if err := dst.RestoreFromSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if got := dst.NextTriggerEventOffset(0); got != 3 {
		t.Errorf("restored partition 0 cursor = %d, want 3", got)
	}
	if got := dst.NextTriggerEventOffset(1); got != 6 {
		t.Errorf("restored partition 1 cursor = %d, want 6", got)
	}
	got := dst.Capture(0, false)
	if len(got.Pending) != 1 || got.Pending[0].Id != 1 {
		t.Errorf("restored pending = %+v, want id=1", got.Pending)
	}
	if len(got.Terminals) != 1 || got.Terminals[0].Id != 2 {
		t.Errorf("restored terminals = %+v, want id=2", got.Terminals)
	}
	if got.Offsets[0] != 50 {
		t.Errorf("restored market offset = %d, want 50", got.Offsets[0])
	}
}

func TestRestore_NilSnapshot(t *testing.T) {
	e := New(0)
	if err := e.RestoreFromSnapshot(nil); err == nil {
		t.Fatal("expected error on nil snapshot")
	}
}

func TestApply_RejectsZeroId(t *testing.T) {
	e := New(0)
	if err := e.ApplyTriggerUpdate(&eventpb.TriggerUpdate{Id: 0, TriggerSeqId: 1}, 0, 0); err == nil {
		t.Fatal("expected error on Id == 0")
	}
}

func TestApply_RejectsNil(t *testing.T) {
	e := New(0)
	if err := e.ApplyTriggerUpdate(nil, 0, 0); err == nil {
		t.Fatal("expected error on nil TriggerUpdate")
	}
	if err := e.ApplyMarketCheckpoint(nil, 0, 0); err == nil {
		t.Fatal("expected error on nil checkpoint")
	}
}

func TestCapture_PreservesAllRecordFields(t *testing.T) {
	e := New(0)
	full := &eventpb.TriggerUpdate{
		Id:                42,
		ClientTriggerId:   "cli-1",
		UserId:            "u-1",
		Symbol:            "BTC-USDT",
		Side:              eventpb.Side_SIDE_SELL,
		Type:              eventpb.TriggerEventType_TRIGGER_EVENT_TYPE_STOP_LOSS_LIMIT,
		StopPrice:         "100",
		LimitPrice:        "99",
		Qty:               "0.5",
		QuoteQty:          "50",
		Tif:               eventpb.TimeInForce_TIME_IN_FORCE_GTC,
		Status:            eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING,
		CreatedAtUnixMs:   1700000000000,
		ExpiresAtUnixMs:   1700000060000,
		OcoGroupId:        "oco-99",
		TrailingDeltaBps:  50,
		ActivationPrice:   "101",
		TrailingWatermark: "102",
		TrailingActive:    true,
		TriggerSeqId:      1,
	}
	_ = e.ApplyTriggerUpdate(full, 0, 0)
	snap := e.Capture(0, false)
	if len(snap.Pending) != 1 {
		t.Fatalf("pending len = %d", len(snap.Pending))
	}
	r := snap.Pending[0]
	want := &snapshotpb.TriggerRecord{
		Id:                42,
		ClientTriggerId:   "cli-1",
		UserId:            "u-1",
		Symbol:            "BTC-USDT",
		Side:              uint32(eventpb.Side_SIDE_SELL),
		Type:              uint32(eventpb.TriggerEventType_TRIGGER_EVENT_TYPE_STOP_LOSS_LIMIT),
		StopPrice:         "100",
		LimitPrice:        "99",
		Qty:               "0.5",
		QuoteQty:          "50",
		Tif:               uint32(eventpb.TimeInForce_TIME_IN_FORCE_GTC),
		Status:            uint32(eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_PENDING),
		CreatedAtMs:       1700000000000,
		ExpiresAtMs:       1700000060000,
		OcoGroupId:        "oco-99",
		TrailingDeltaBps:  50,
		ActivationPrice:   "101",
		TrailingWatermark: "102",
		TrailingActive:    true,
	}
	if !recordsEqual(r, want) {
		t.Errorf("captured record = %+v\nwant %+v", r, want)
	}
}

func recordsEqual(a, b *snapshotpb.TriggerRecord) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Id == b.Id &&
		a.ClientTriggerId == b.ClientTriggerId &&
		a.UserId == b.UserId &&
		a.Symbol == b.Symbol &&
		a.Side == b.Side &&
		a.Type == b.Type &&
		a.StopPrice == b.StopPrice &&
		a.LimitPrice == b.LimitPrice &&
		a.Qty == b.Qty &&
		a.QuoteQty == b.QuoteQty &&
		a.Tif == b.Tif &&
		a.Status == b.Status &&
		a.CreatedAtMs == b.CreatedAtMs &&
		a.TriggeredAtMs == b.TriggeredAtMs &&
		a.PlacedOrderId == b.PlacedOrderId &&
		a.RejectReason == b.RejectReason &&
		a.ExpiresAtMs == b.ExpiresAtMs &&
		a.OcoGroupId == b.OcoGroupId &&
		a.TrailingDeltaBps == b.TrailingDeltaBps &&
		a.ActivationPrice == b.ActivationPrice &&
		a.TrailingWatermark == b.TrailingWatermark &&
		a.TrailingActive == b.TrailingActive
}
