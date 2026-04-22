package engine

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
)

// TestApplyFreezeEvent_InsertsOrderAndSetsBalance covers the cold
// case where catch-up sees an order before its snapshot-saved copy
// existed. Freeze replay must insert the order (from event fields)
// and write balance_after (via PutForRestore so Version matches
// event).
func TestApplyFreezeEvent_InsertsOrderAndSetsBalance(t *testing.T) {
	state := NewShardState(0)
	evt := &eventpb.FreezeEvent{
		UserId:        "u1",
		OrderId:       100,
		ClientOrderId: "coid-A",
		Symbol:        "BTC-USDT",
		Side:          eventpb.Side_SIDE_BUY,
		OrderType:     eventpb.OrderType_ORDER_TYPE_LIMIT,
		Tif:           eventpb.TimeInForce_TIME_IN_FORCE_GTC,
		Price:         "50000",
		Qty:           "0.1",
		FreezeAsset:   "USDT",
		FreezeAmount:  "5000",
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId:    "u1",
			Asset:     "USDT",
			Available: "15000",
			Frozen:    "5000",
			Version:   42,
		},
	}
	if err := applyFreezeEvent(state, evt); err != nil {
		t.Fatalf("applyFreezeEvent: %v", err)
	}
	o := state.Orders().Get(100)
	if o == nil {
		t.Fatal("order 100 not inserted")
	}
	if o.UserID != "u1" || o.Symbol != "BTC-USDT" || o.Status != OrderStatusPendingNew {
		t.Fatalf("order = %+v, want (u1/BTC-USDT/PendingNew)", o)
	}
	if o.Price.String() != "50000" || o.Qty.String() != "0.1" {
		t.Fatalf("order = %+v, want price=50000 qty=0.1", o)
	}
	if o.FrozenAsset != "USDT" || o.FrozenAmount.String() != "5000" {
		t.Fatalf("order = %+v, want frozen USDT 5000", o)
	}
	b := state.Balance("u1", "USDT")
	if b.Available.String() != "15000" || b.Frozen.String() != "5000" || b.Version != 42 {
		t.Fatalf("balance = %+v, want (15000/5000/v42)", b)
	}
}

// TestApplyFreezeEvent_IdempotentOnExistingOrder exercises the replay
// case: order already exists from an earlier event / snapshot-restore.
// The freeze event must NOT overwrite the order (it may have moved
// forward), but balance_after still writes verbatim.
func TestApplyFreezeEvent_IdempotentOnExistingOrder(t *testing.T) {
	state := NewShardState(0)
	state.Orders().RestoreInsert(&Order{
		ID:        100,
		UserID:    "u1",
		Symbol:    "BTC-USDT",
		Status:    OrderStatusPartiallyFilled,
		FilledQty: dec.New("0.05"),
	})
	evt := &eventpb.FreezeEvent{
		UserId:       "u1",
		OrderId:      100,
		Symbol:       "BTC-USDT",
		Side:         eventpb.Side_SIDE_BUY,
		OrderType:    eventpb.OrderType_ORDER_TYPE_LIMIT,
		Price:        "50000",
		Qty:          "0.1",
		FreezeAsset:  "USDT",
		FreezeAmount: "5000",
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId: "u1", Asset: "USDT",
			Available: "15000", Frozen: "5000", Version: 10,
		},
	}
	if err := applyFreezeEvent(state, evt); err != nil {
		t.Fatalf("applyFreezeEvent: %v", err)
	}
	o := state.Orders().Get(100)
	if o.Status != OrderStatusPartiallyFilled {
		t.Fatalf("status = %v, want preserved PartiallyFilled", o.Status)
	}
	if o.FilledQty.String() != "0.05" {
		t.Fatalf("filled_qty = %s, want preserved 0.05", o.FilledQty)
	}
	b := state.Balance("u1", "USDT")
	if b.Available.String() != "15000" {
		t.Fatalf("balance.available = %s, want 15000 (from event)", b.Available)
	}
}

// TestApplyUnfreezeEvent_SetsBalance covers the refund path —
// UnfreezeEvent's job during catch-up is purely balance reset;
// associated OrderStatusEvent (emitted in same transaction) will
// advance the order's terminal status in a separate apply.
func TestApplyUnfreezeEvent_SetsBalance(t *testing.T) {
	state := NewShardState(0)
	state.Account("u1").PutForRestore("USDT", Balance{
		Available: dec.New("10000"),
		Frozen:    dec.New("5000"),
		Version:   5,
	})
	evt := &eventpb.UnfreezeEvent{
		UserId: "u1", OrderId: 100, Asset: "USDT", Amount: "5000",
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId: "u1", Asset: "USDT",
			Available: "15000", Frozen: "0", Version: 6,
		},
	}
	if err := applyUnfreezeEvent(state, evt); err != nil {
		t.Fatalf("applyUnfreezeEvent: %v", err)
	}
	b := state.Balance("u1", "USDT")
	if b.Available.String() != "15000" || b.Frozen.String() != "0" || b.Version != 6 {
		t.Fatalf("balance = %+v, want (15000/0/v6)", b)
	}
}

// TestApplySettlementEvent_AdvancesFilledAndBalances tests the
// settlement replay path. Balances from base/quote_balance_after;
// FilledQty is monotonically advanced by evt.Qty.
func TestApplySettlementEvent_AdvancesFilledAndBalances(t *testing.T) {
	state := NewShardState(0)
	state.Orders().RestoreInsert(&Order{
		ID:           100,
		UserID:       "u1",
		Symbol:       "BTC-USDT",
		Side:         SideBid,
		Type:         OrderTypeLimit,
		Price:        dec.New("50000"),
		Qty:          dec.New("1"),
		FilledQty:    dec.New("0.2"),
		FrozenAsset:  "USDT",
		FrozenAmount: dec.New("50000"),
		FrozenSpent:  dec.New("10000"),
		Status:       OrderStatusPartiallyFilled,
	})
	evt := &eventpb.SettlementEvent{
		UserId: "u1", OrderId: 100, Symbol: "BTC-USDT",
		Side:       eventpb.Side_SIDE_BUY,
		Price:      "50000",
		Qty:        "0.1",
		DeltaBase:  "0.1",
		DeltaQuote: "-5000",
		BaseBalanceAfter: &eventpb.BalanceSnapshot{
			UserId: "u1", Asset: "BTC",
			Available: "0.3", Frozen: "0", Version: 3,
		},
		QuoteBalanceAfter: &eventpb.BalanceSnapshot{
			UserId: "u1", Asset: "USDT",
			Available: "25000", Frozen: "30000", Version: 8,
		},
	}
	if err := applySettlementEvent(state, evt); err != nil {
		t.Fatalf("applySettlementEvent: %v", err)
	}
	o := state.Orders().Get(100)
	if o.FilledQty.String() != "0.3" {
		t.Fatalf("filled_qty = %s, want 0.3 (0.2 + 0.1)", o.FilledQty)
	}
	// Buy-side LIMIT: frozen_spent += price * qty = 5000
	if o.FrozenSpent.String() != "15000" {
		t.Fatalf("frozen_spent = %s, want 15000 (10000 + 5000)", o.FrozenSpent)
	}
	b := state.Balance("u1", "BTC")
	if b.Available.String() != "0.3" {
		t.Fatalf("BTC balance = %+v, want available=0.3", b)
	}
}

// TestApplySettlementEvent_SequentialReplayAccumulates documents the
// accepted behavior: catch-up assumes sequential forward application
// of every journal event. Each replayed settlement adds its qty to
// FilledQty, so in-order replay from a snapshot converges.
func TestApplySettlementEvent_SequentialReplayAccumulates(t *testing.T) {
	state := NewShardState(0)
	state.Orders().RestoreInsert(&Order{
		ID: 100, UserID: "u1", Symbol: "BTC-USDT",
		Side: SideAsk, Type: OrderTypeLimit,
		Price: dec.New("50000"), Qty: dec.New("1"),
		FilledQty:   dec.New("0.5"),
		FrozenAsset: "BTC", FrozenAmount: dec.New("1"),
		Status: OrderStatusPartiallyFilled,
	})
	evt := &eventpb.SettlementEvent{
		UserId: "u1", OrderId: 100, Symbol: "BTC-USDT",
		Price: "50000", Qty: "0.1",
	}
	if err := applySettlementEvent(state, evt); err != nil {
		t.Fatalf("applySettlementEvent: %v", err)
	}
	o := state.Orders().Get(100)
	if o.FilledQty.String() != "0.6" {
		t.Fatalf("filled_qty = %s, want 0.6 (sequential replay accumulates)", o.FilledQty)
	}
}

// TestApplyTransferEvent_RememberInRing ensures the transfer_id is
// re-seated into recentTransferIDs so RPC replay after restart
// correctly dedups.
func TestApplyTransferEvent_RememberInRing(t *testing.T) {
	state := NewShardState(0)
	evt := &eventpb.TransferEvent{
		UserId:     "u1",
		TransferId: "tx-42",
		Asset:      "USDT",
		Amount:     "100",
		Type:       eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId: "u1", Asset: "USDT",
			Available: "100", Frozen: "0", Version: 1,
		},
	}
	if err := applyTransferEvent(state, evt); err != nil {
		t.Fatalf("applyTransferEvent: %v", err)
	}
	if !state.Account("u1").TransferSeen("tx-42") {
		t.Fatal("transfer_id not remembered in ring")
	}
	b := state.Balance("u1", "USDT")
	if b.Available.String() != "100" {
		t.Fatalf("balance = %+v, want 100", b)
	}
}

// TestApplyOrderStatusEvent_TransitionsToTerminal covers the primary
// use: an OrderStatusEvent for a known order drives Status forward
// and stamps TerminatedAt.
func TestApplyOrderStatusEvent_TransitionsToTerminal(t *testing.T) {
	state := NewShardState(0)
	state.Orders().RestoreInsert(&Order{
		ID: 100, UserID: "u1", Symbol: "BTC-USDT",
		Type: OrderTypeLimit, Status: OrderStatusNew,
	})
	evt := &eventpb.OrderStatusEvent{
		UserId: "u1", OrderId: 100,
		OldStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW,
		NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED,
		FilledQty: "0.5",
	}
	if err := applyOrderStatusEvent(state, evt); err != nil {
		t.Fatalf("applyOrderStatusEvent: %v", err)
	}
	o := state.Orders().Get(100)
	if o.Status != OrderStatusCanceled {
		t.Fatalf("status = %v, want Canceled", o.Status)
	}
	if o.TerminatedAt == 0 {
		t.Fatal("TerminatedAt not stamped on terminal transition")
	}
	if o.FilledQty.String() != "0.5" {
		t.Fatalf("filled_qty = %s, want 0.5", o.FilledQty)
	}
}

// TestApplyOrderStatusEvent_NoBackwardsFromTerminal: replay of a
// non-terminal status after the order is already terminal must
// NOT revert.
func TestApplyOrderStatusEvent_NoBackwardsFromTerminal(t *testing.T) {
	state := NewShardState(0)
	state.Orders().RestoreInsert(&Order{
		ID: 100, UserID: "u1", Symbol: "BTC-USDT",
		Type: OrderTypeLimit, Status: OrderStatusCanceled,
		TerminatedAt: 1000,
	})
	evt := &eventpb.OrderStatusEvent{
		UserId: "u1", OrderId: 100,
		NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW,
	}
	if err := applyOrderStatusEvent(state, evt); err != nil {
		t.Fatalf("applyOrderStatusEvent: %v", err)
	}
	o := state.Orders().Get(100)
	if o.Status != OrderStatusCanceled {
		t.Fatalf("status = %v, want Canceled (no revert)", o.Status)
	}
}

// TestApplyOrderStatusEvent_MissingOrderIsNoOp covers the foreign /
// evicted order case — status event arrives for an ID not in byID
// and must not error.
func TestApplyOrderStatusEvent_MissingOrderIsNoOp(t *testing.T) {
	state := NewShardState(0)
	evt := &eventpb.OrderStatusEvent{
		UserId: "u1", OrderId: 999,
		NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW,
	}
	if err := applyOrderStatusEvent(state, evt); err != nil {
		t.Fatalf("applyOrderStatusEvent: %v", err)
	}
	if state.Orders().Get(999) != nil {
		t.Fatal("order 999 should still be absent")
	}
}

// TestApplyOrderEvictedEvent_Deletes: byID loses the order after apply.
func TestApplyOrderEvictedEvent_Deletes(t *testing.T) {
	state := NewShardState(0)
	state.Orders().RestoreInsert(&Order{
		ID: 100, UserID: "u1", Symbol: "BTC-USDT",
		Type: OrderTypeLimit, Status: OrderStatusCanceled,
		TerminatedAt: 1000,
	})
	evt := &eventpb.OrderEvictedEvent{
		UserId: "u1", OrderId: 100, Symbol: "BTC-USDT",
		FinalStatus:  eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED,
		TerminatedAt: 1000,
	}
	if err := applyOrderEvictedEvent(state, evt); err != nil {
		t.Fatalf("applyOrderEvictedEvent: %v", err)
	}
	if state.Orders().Get(100) != nil {
		t.Fatal("order 100 should have been deleted")
	}
}

// TestApplyOrderEvictedEvent_Idempotent: replaying the same evict
// event over already-evicted state is a no-op (no error).
func TestApplyOrderEvictedEvent_Idempotent(t *testing.T) {
	state := NewShardState(0)
	evt := &eventpb.OrderEvictedEvent{
		UserId: "u1", OrderId: 100, Symbol: "BTC-USDT",
		FinalStatus:  eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED,
		TerminatedAt: 1000,
	}
	if err := applyOrderEvictedEvent(state, evt); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := applyOrderEvictedEvent(state, evt); err != nil {
		t.Fatalf("second apply: %v", err)
	}
}

// TestApplyCounterJournalEvent_DispatchesByPayload covers the
// top-level dispatcher: each oneof variant reaches its helper. Uses
// the exported entry point (what every external caller will hit).
func TestApplyCounterJournalEvent_DispatchesByPayload(t *testing.T) {
	state := NewShardState(0)
	cases := []struct {
		name  string
		evt   *eventpb.CounterJournalEvent
		check func(t *testing.T)
	}{
		{
			name: "freeze",
			evt: &eventpb.CounterJournalEvent{
				Payload: &eventpb.CounterJournalEvent_Freeze{
					Freeze: &eventpb.FreezeEvent{
						UserId: "u1", OrderId: 1, Symbol: "BTC-USDT",
						Side:      eventpb.Side_SIDE_BUY,
						OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
						Price:     "10", Qty: "1", FreezeAsset: "USDT", FreezeAmount: "10",
						BalanceAfter: &eventpb.BalanceSnapshot{
							UserId: "u1", Asset: "USDT", Available: "90", Frozen: "10",
						},
					},
				},
			},
			check: func(t *testing.T) {
				if state.Orders().Get(1) == nil {
					t.Fatal("freeze didn't insert order 1")
				}
			},
		},
		{
			name: "te_checkpoint_noop",
			evt: &eventpb.CounterJournalEvent{
				Payload: &eventpb.CounterJournalEvent_TeCheckpoint{
					TeCheckpoint: &eventpb.TECheckpointEvent{TePartition: 1, TeOffset: 42},
				},
			},
			check: func(t *testing.T) {
				// purely signalling — no effect.
			},
		},
		{
			name: "nil_payload",
			evt:  &eventpb.CounterJournalEvent{Payload: nil},
			check: func(t *testing.T) {
				// nil payload is a no-op and must not panic.
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ApplyCounterJournalEvent(state, tc.evt); err != nil {
				t.Fatalf("ApplyCounterJournalEvent: %v", err)
			}
			tc.check(t)
		})
	}
}

// TestOrderStatusFromProto_RoundTrip verifies the mapping table
// is complete — every internal status has a non-Unspecified entry,
// unknown proto values fall back to Unspecified.
func TestOrderStatusFromProto_RoundTrip(t *testing.T) {
	cases := []struct {
		proto eventpb.InternalOrderStatus
		want  OrderStatus
	}{
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW, OrderStatusPendingNew},
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW, OrderStatusNew},
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED, OrderStatusPartiallyFilled},
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED, OrderStatusFilled},
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL, OrderStatusPendingCancel},
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED, OrderStatusCanceled},
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED, OrderStatusRejected},
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED, OrderStatusExpired},
		{eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED, OrderStatusUnspecified},
	}
	for _, tc := range cases {
		if got := OrderStatusFromProto(tc.proto); got != tc.want {
			t.Errorf("OrderStatusFromProto(%v) = %v, want %v", tc.proto, got, tc.want)
		}
	}
}
