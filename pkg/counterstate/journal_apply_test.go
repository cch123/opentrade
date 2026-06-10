package counterstate

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
		UserId:        1001,
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
			UserId:    1001,
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
	if o.UserID != 1001 || o.Symbol != "BTC-USDT" || o.Status != OrderStatusPendingNew {
		t.Fatalf("order = %+v, want (u1/BTC-USDT/PendingNew)", o)
	}
	if o.Price.String() != "50000" || o.Qty.String() != "0.1" {
		t.Fatalf("order = %+v, want price=50000 qty=0.1", o)
	}
	if o.FrozenAsset != "USDT" || o.FrozenAmount.String() != "5000" {
		t.Fatalf("order = %+v, want frozen USDT 5000", o)
	}
	b := state.Balance(1001, "USDT")
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
		UserID:    1001,
		Symbol:    "BTC-USDT",
		Status:    OrderStatusPartiallyFilled,
		FilledQty: dec.New("0.05"),
	})
	evt := &eventpb.FreezeEvent{
		UserId:       1001,
		OrderId:      100,
		Symbol:       "BTC-USDT",
		Side:         eventpb.Side_SIDE_BUY,
		OrderType:    eventpb.OrderType_ORDER_TYPE_LIMIT,
		Price:        "50000",
		Qty:          "0.1",
		FreezeAsset:  "USDT",
		FreezeAmount: "5000",
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "USDT",
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
	b := state.Balance(1001, "USDT")
	if b.Available.String() != "15000" {
		t.Fatalf("balance.available = %s, want 15000 (from event)", b.Available)
	}
}

// TestApplyFreezeEvent_MarketBuyByQuoteThenLiveSettlement is the
// regression test for the snapshot/journal-catch-up boundary: an
// ADR-0035 market-buy-by-quote order placed after the snapshot is
// rebuilt from its FreezeEvent during catch-up, then LIVE trade events
// settle against the rebuilt order. Before FreezeEvent carried
// quote_qty, the rebuilt order had QuoteQty=0 ∧ Qty=0, so settleTaker
// took the limit-buy branch with Price=0: FrozenQuoteDelta stayed 0
// (frozen quote never consumed) and statusAfterFill flagged the first
// partial fill as FILLED (filledAfter ≥ Qty(0)).
func TestApplyFreezeEvent_MarketBuyByQuoteThenLiveSettlement(t *testing.T) {
	state := NewShardState(0)
	// Catch-up: rebuild the order from its FreezeEvent. User budget:
	// 5000 USDT (QuoteQty == FreezeAmount for the by-quote shape).
	evt := &eventpb.FreezeEvent{
		UserId:       1001,
		OrderId:      100,
		Symbol:       "BTC-USDT",
		Side:         eventpb.Side_SIDE_BUY,
		OrderType:    eventpb.OrderType_ORDER_TYPE_MARKET,
		Tif:          eventpb.TimeInForce_TIME_IN_FORCE_IOC,
		Price:        "0",
		Qty:          "0",
		QuoteQty:     "5000",
		FreezeAsset:  "USDT",
		FreezeAmount: "5000",
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "USDT",
			Available: "5000", Frozen: "5000", Version: 7,
		},
	}
	if err := applyFreezeEvent(state, evt); err != nil {
		t.Fatalf("applyFreezeEvent: %v", err)
	}
	o := state.Orders().Get(100)
	if o == nil {
		t.Fatal("order 100 not inserted")
	}
	if o.QuoteQty.String() != "5000" || !o.IsMarketBuyByQuote() {
		t.Fatalf("rebuilt order = %+v, want QuoteQty=5000 / IsMarketBuyByQuote", o)
	}

	// Catch-up done — a live trade event arrives. Match price 50000,
	// qty 0.05 → consumes 2500 of the 5000 quote budget.
	_, taker, err := ComputeSettlement(state, TradeInput{
		TradeID:             "t-1",
		Symbol:              "BTC-USDT",
		Price:               dec.New("50000"),
		Qty:                 dec.New("0.05"),
		MakerUserID:         2002,
		MakerOrderID:        999, // foreign — not in this state
		TakerUserID:         1001,
		TakerOrderID:        100,
		TakerSide:           SideBid,
		TakerFilledQtyAfter: dec.New("0.05"),
	})
	if err != nil {
		t.Fatalf("ComputeSettlement: %v", err)
	}
	if taker.FrozenQuoteDelta.String() != "-2500" {
		t.Fatalf("FrozenQuoteDelta = %s, want -2500 (match quote consumed from budget)", taker.FrozenQuoteDelta)
	}
	if taker.StatusAfter != OrderStatusPartiallyFilled {
		t.Fatalf("StatusAfter = %v, want PartiallyFilled (by-quote shape never self-FILLs)", taker.StatusAfter)
	}
	if err := state.ApplyPartySettlement("BTC-USDT", taker); err != nil {
		t.Fatalf("ApplyPartySettlement: %v", err)
	}
	if o.Status != OrderStatusPartiallyFilled {
		t.Fatalf("status = %v, want non-terminal PartiallyFilled", o.Status)
	}
	if o.FrozenSpent.String() != "2500" {
		t.Fatalf("frozen_spent = %s, want 2500", o.FrozenSpent)
	}
	q := state.Balance(1001, "USDT")
	if q.Frozen.String() != "2500" {
		t.Fatalf("USDT frozen = %s, want 2500 (5000 - 2500 consumed)", q.Frozen)
	}
	b := state.Balance(1001, "BTC")
	if b.Available.String() != "0.05" {
		t.Fatalf("BTC available = %s, want 0.05", b.Available)
	}
}

// TestApplyFreezeEvent_RestoresProtectedMarketBuyShape pins the
// ADR-0083 fields on the rebuilt order: a protected market buy by base
// qty must come back with SlippageBps (>0 marks the protected shape)
// and zero QuoteQty so the IsMarketBuyByBase predicate — which
// settlement keys off — holds after catch-up.
func TestApplyFreezeEvent_RestoresProtectedMarketBuyShape(t *testing.T) {
	state := NewShardState(0)
	evt := &eventpb.FreezeEvent{
		UserId:       1001,
		OrderId:      101,
		Symbol:       "BTC-USDT",
		Side:         eventpb.Side_SIDE_BUY,
		OrderType:    eventpb.OrderType_ORDER_TYPE_MARKET,
		Tif:          eventpb.TimeInForce_TIME_IN_FORCE_IOC,
		Price:        "0",
		Qty:          "0.5",
		QuoteQty:     "0",
		SlippageBps:  50,
		FreezeAsset:  "USDT",
		FreezeAmount: "26000", // quote_cap
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "USDT",
			Available: "4000", Frozen: "26000", Version: 3,
		},
	}
	if err := applyFreezeEvent(state, evt); err != nil {
		t.Fatalf("applyFreezeEvent: %v", err)
	}
	o := state.Orders().Get(101)
	if o == nil {
		t.Fatal("order 101 not inserted")
	}
	if o.SlippageBps != 50 {
		t.Fatalf("slippage_bps = %d, want 50", o.SlippageBps)
	}
	if !o.IsMarketBuyByBase() || o.IsMarketBuyByQuote() {
		t.Fatalf("rebuilt order = %+v, want IsMarketBuyByBase ∧ !IsMarketBuyByQuote", o)
	}
}

// TestApplyUnfreezeEvent_SetsBalance covers the refund path —
// UnfreezeEvent's job during catch-up is purely balance reset;
// associated OrderStatusEvent (emitted in same transaction) will
// advance the order's terminal status in a separate apply.
func TestApplyUnfreezeEvent_SetsBalance(t *testing.T) {
	state := NewShardState(0)
	state.Account(1001).PutForRestore("USDT", Balance{
		Available: dec.New("10000"),
		Frozen:    dec.New("5000"),
		Version:   5,
	})
	evt := &eventpb.UnfreezeEvent{
		UserId: 1001, OrderId: 100, Asset: "USDT", Amount: "5000",
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "USDT",
			Available: "15000", Frozen: "0", Version: 6,
		},
	}
	if err := applyUnfreezeEvent(state, evt); err != nil {
		t.Fatalf("applyUnfreezeEvent: %v", err)
	}
	b := state.Balance(1001, "USDT")
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
		UserID:       1001,
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
		UserId: 1001, OrderId: 100, Symbol: "BTC-USDT",
		Side:          eventpb.Side_SIDE_BUY,
		Price:         "50000",
		Qty:           "0.1",
		DeltaBase:     "0.1",
		DeltaQuote:    "-5000",
		UnfreezeBase:  "0",
		UnfreezeQuote: "5000",
		BaseBalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "BTC",
			Available: "0.3", Frozen: "0", Version: 3,
		},
		QuoteBalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "USDT",
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
	// Buy-side: frozen_spent += unfreeze_quote = 5000
	if o.FrozenSpent.String() != "15000" {
		t.Fatalf("frozen_spent = %s, want 15000 (10000 + 5000)", o.FrozenSpent)
	}
	b := state.Balance(1001, "BTC")
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
		ID: 100, UserID: 1001, Symbol: "BTC-USDT",
		Side: SideAsk, Type: OrderTypeLimit,
		Price: dec.New("50000"), Qty: dec.New("1"),
		FilledQty:   dec.New("0.5"),
		FrozenAsset: "BTC", FrozenAmount: dec.New("1"),
		Status: OrderStatusPartiallyFilled,
	})
	evt := &eventpb.SettlementEvent{
		UserId: 1001, OrderId: 100, Symbol: "BTC-USDT",
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

// TestApplySettlementEvent_PriceImprovedTakerMatchesLivePath is the
// regression test for the FrozenSpent recomputation gap: a limit-buy
// TAKER filled at a better (lower) maker price consumes reservation at
// its OWN order price (|FrozenQuoteDelta| = order_price × qty), while
// evt.Price carries the match price. Replay used to recompute
// FrozenSpent as match_price × qty — too small — so the terminal
// unfreeze later released more than the order still held and drove
// Balance.Frozen negative. The fix sources FrozenSpent from the
// event's unfreeze_base / unfreeze_quote, which BuildSettlementEvent
// stamps verbatim from the live PartySettlement.
func TestApplySettlementEvent_PriceImprovedTakerMatchesLivePath(t *testing.T) {
	seed := func() *ShardState {
		state := NewShardState(0)
		state.Account(1001).PutForRestore("USDT", Balance{
			Available: dec.New("0"), Frozen: dec.New("50000"), Version: 1,
		})
		state.Orders().RestoreInsert(&Order{
			ID: 100, UserID: 1001, Symbol: "BTC-USDT",
			Side: SideBid, Type: OrderTypeLimit,
			Price: dec.New("50000"), Qty: dec.New("1"),
			FrozenAsset: "USDT", FrozenAmount: dec.New("50000"),
			Status: OrderStatusNew,
		})
		return state
	}

	// Live path: taker limit buy at 50000 fills 0.1 against a maker at
	// 49000 → reservation consumed 5000 (own price), refund 100.
	live := seed()
	_, taker, err := ComputeSettlement(live, TradeInput{
		TradeID:             "t-1",
		Symbol:              "BTC-USDT",
		Price:               dec.New("49000"),
		Qty:                 dec.New("0.1"),
		MakerUserID:         2002,
		MakerOrderID:        999, // foreign — not in this state
		TakerUserID:         1001,
		TakerOrderID:        100,
		TakerSide:           SideBid,
		TakerFilledQtyAfter: dec.New("0.1"),
	})
	if err != nil {
		t.Fatalf("ComputeSettlement: %v", err)
	}
	if err := live.ApplyPartySettlement("BTC-USDT", taker); err != nil {
		t.Fatalf("ApplyPartySettlement: %v", err)
	}
	liveOrder := live.Orders().Get(100)

	// Replay path: apply the SettlementEvent exactly as
	// journal.BuildSettlementEvent emits it from that PartySettlement
	// (price = match price; unfreeze_* = |Frozen*Delta|; balance_after
	// snapshots = the live post-settlement balances).
	liveBase := live.Balance(1001, "BTC")
	liveQuote := live.Balance(1001, "USDT")
	replay := seed()
	evt := &eventpb.SettlementEvent{
		UserId: 1001, OrderId: 100, TradeId: "t-1", Symbol: "BTC-USDT",
		Side:          eventpb.Side_SIDE_BUY,
		Price:         "49000",
		Qty:           "0.1",
		DeltaBase:     taker.BaseDelta.String(),
		DeltaQuote:    taker.QuoteDelta.String(),
		UnfreezeBase:  taker.FrozenBaseDelta.Neg().String(),
		UnfreezeQuote: taker.FrozenQuoteDelta.Neg().String(),
		BaseBalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "BTC",
			Available: liveBase.Available.String(), Frozen: liveBase.Frozen.String(),
			Version: liveBase.Version,
		},
		QuoteBalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "USDT",
			Available: liveQuote.Available.String(), Frozen: liveQuote.Frozen.String(),
			Version: liveQuote.Version,
		},
	}
	if err := applySettlementEvent(replay, evt); err != nil {
		t.Fatalf("applySettlementEvent: %v", err)
	}
	replayOrder := replay.Orders().Get(100)

	if replayOrder.FrozenSpent.String() != "5000" {
		t.Fatalf("replayed frozen_spent = %s, want 5000 (order_price 50000 × 0.1, not match_price 49000 × 0.1)", replayOrder.FrozenSpent)
	}
	if !dec.Equal(replayOrder.FrozenSpent, liveOrder.FrozenSpent) {
		t.Fatalf("frozen_spent diverged: replay %s vs live %s", replayOrder.FrozenSpent, liveOrder.FrozenSpent)
	}
	if !dec.Equal(replayOrder.FilledQty, liveOrder.FilledQty) {
		t.Fatalf("filled_qty diverged: replay %s vs live %s", replayOrder.FilledQty, liveOrder.FilledQty)
	}
	rq := replay.Balance(1001, "USDT")
	if !dec.Equal(rq.Frozen, liveQuote.Frozen) || !dec.Equal(rq.Available, liveQuote.Available) {
		t.Fatalf("USDT balance diverged: replay %+v vs live %+v", rq, liveQuote)
	}

	// The failure mode the gap caused: a terminal transition after
	// catch-up releases FrozenAmount − FrozenSpent. With the undersized
	// FrozenSpent (4900) the residual (45100) exceeded the 45000 still
	// frozen → "frozen would be negative". With event-sourced
	// FrozenSpent the release matches exactly.
	if err := replay.UnfreezeOnTerminal(replayOrder, replayOrder.FrozenSpent); err != nil {
		t.Fatalf("UnfreezeOnTerminal after replay: %v", err)
	}
	rq = replay.Balance(1001, "USDT")
	if rq.Frozen.String() != "0" {
		t.Fatalf("USDT frozen after terminal unfreeze = %s, want 0", rq.Frozen)
	}
}

// TestApplyTransferEvent_RememberInRing ensures the transfer_id is
// re-seated into recentTransferIDs so RPC replay after restart
// correctly dedups.
func TestApplyTransferEvent_RememberInRing(t *testing.T) {
	state := NewShardState(0)
	evt := &eventpb.TransferEvent{
		UserId:     1001,
		TransferId: "tx-42",
		Asset:      "USDT",
		Amount:     "100",
		Type:       eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
		BalanceAfter: &eventpb.BalanceSnapshot{
			UserId: 1001, Asset: "USDT",
			Available: "100", Frozen: "0", Version: 1,
		},
	}
	if err := applyTransferEvent(state, evt); err != nil {
		t.Fatalf("applyTransferEvent: %v", err)
	}
	if !state.Account(1001).TransferSeen("tx-42") {
		t.Fatal("transfer_id not remembered in ring")
	}
	b := state.Balance(1001, "USDT")
	if b.Available.String() != "100" {
		t.Fatalf("balance = %+v, want 100", b)
	}
}

// TestApplyOrderStatusEvent_TransitionsToTerminalDeletes (ADR-0063):
// an OrderStatusEvent moving to a terminal state drives Status forward
// and removes the order from byID.
func TestApplyOrderStatusEvent_TransitionsToTerminalDeletes(t *testing.T) {
	state := NewShardState(0)
	state.Orders().RestoreInsert(&Order{
		ID: 100, UserID: 1001, Symbol: "BTC-USDT",
		Type: OrderTypeLimit, Status: OrderStatusNew,
	})
	evt := &eventpb.OrderStatusEvent{
		UserId: 1001, OrderId: 100,
		OldStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW,
		NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED,
		FilledQty: "0.5",
	}
	if err := applyOrderStatusEvent(state, evt); err != nil {
		t.Fatalf("applyOrderStatusEvent: %v", err)
	}
	if state.Orders().Get(100) != nil {
		t.Fatal("terminal transition must delete the order from byID")
	}
}

// TestApplyOrderStatusEvent_NonTerminalKeepsByID: a non-terminal
// transition (e.g. PendingNew → New) advances Status but keeps the
// order in byID.
func TestApplyOrderStatusEvent_NonTerminalKeepsByID(t *testing.T) {
	state := NewShardState(0)
	state.Orders().RestoreInsert(&Order{
		ID: 100, UserID: 1001, Symbol: "BTC-USDT",
		Type: OrderTypeLimit, Status: OrderStatusPendingNew,
	})
	evt := &eventpb.OrderStatusEvent{
		UserId: 1001, OrderId: 100,
		OldStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
		NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW,
	}
	if err := applyOrderStatusEvent(state, evt); err != nil {
		t.Fatalf("applyOrderStatusEvent: %v", err)
	}
	o := state.Orders().Get(100)
	if o == nil {
		t.Fatal("non-terminal transition must keep the order in byID")
	}
	if o.Status != OrderStatusNew {
		t.Fatalf("status = %v, want New", o.Status)
	}
}

// TestApplyOrderStatusEvent_TerminalReplayIsIdempotent: replaying a
// terminal status event after the order is already evicted must not
// error (Delete is ErrOrderNotFound-safe).
func TestApplyOrderStatusEvent_TerminalReplayIsIdempotent(t *testing.T) {
	state := NewShardState(0)
	evt := &eventpb.OrderStatusEvent{
		UserId: 1001, OrderId: 999,
		NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED,
	}
	// Order never existed (or already evicted) — apply is a no-op.
	if err := applyOrderStatusEvent(state, evt); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := applyOrderStatusEvent(state, evt); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if state.Orders().Get(999) != nil {
		t.Fatal("order 999 should still be absent")
	}
}

// TestApplyOrderStatusEvent_MissingOrderIsNoOp covers the foreign /
// evicted order case — status event arrives for an ID not in byID
// and must not error.
func TestApplyOrderStatusEvent_MissingOrderIsNoOp(t *testing.T) {
	state := NewShardState(0)
	evt := &eventpb.OrderStatusEvent{
		UserId: 1001, OrderId: 999,
		NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW,
	}
	if err := applyOrderStatusEvent(state, evt); err != nil {
		t.Fatalf("applyOrderStatusEvent: %v", err)
	}
	if state.Orders().Get(999) != nil {
		t.Fatal("order 999 should still be absent")
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
						UserId: 1001, OrderId: 1, Symbol: "BTC-USDT",
						Side:      eventpb.Side_SIDE_BUY,
						OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
						Price:     "10", Qty: "1", FreezeAsset: "USDT", FreezeAmount: "10",
						BalanceAfter: &eventpb.BalanceSnapshot{
							UserId: 1001, Asset: "USDT", Available: "90", Frozen: "10",
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

// TestApplyCounterJournalEvent_StartupFenceIsNoOp pins down ADR-0064
// §1.2 + §3: the StartupFence sentinel applies as a strict no-op — no
// balance movement, no order insert, no counter_seq advance. Its
// whole value is in *existing* on the partition (fences prior epoch
// + stabilizes LEO for trade-dump). If this test fails because a
// future change taught the engine to mutate state on StartupFence,
// that change is wrong: see ADR-0064 §1.2 "Apply 协议".
func TestApplyCounterJournalEvent_StartupFenceIsNoOp(t *testing.T) {
	state := NewShardState(0)
	// Seed some benign state so "unchanged after apply" is observable.
	state.Account(1001).PutForRestore("USDT", Balance{
		Available: dec.New("100"), Frozen: dec.New("0"), Version: 1,
	})
	state.Orders().RestoreInsert(&Order{
		ID: 42, UserID: 1001, Symbol: "BTC-USDT",
		Status: OrderStatusNew,
	})

	evt := &eventpb.CounterJournalEvent{
		CounterSeqId: 0, // sentinels do not allocate counter_seq
		Payload: &eventpb.CounterJournalEvent_StartupFence{
			StartupFence: &eventpb.StartupFenceEvent{
				NodeId: "counter-node-B",
				Epoch:  7,
				TsMs:   1_700_000_000_000,
			},
		},
	}
	if err := ApplyCounterJournalEvent(state, evt); err != nil {
		t.Fatalf("ApplyCounterJournalEvent(StartupFence): %v", err)
	}

	// Balance unchanged.
	b := state.Balance(1001, "USDT")
	if b.Available.String() != "100" || b.Frozen.String() != "0" || b.Version != 1 {
		t.Fatalf("balance mutated by StartupFence apply: %+v", b)
	}
	// Order unchanged.
	o := state.Orders().Get(42)
	if o == nil {
		t.Fatal("order 42 disappeared after StartupFence apply")
	}
	if o.Status != OrderStatusNew {
		t.Fatalf("order status mutated by StartupFence apply: %v", o.Status)
	}
}

// TestApplyCounterJournalEvent_UnknownPayloadIsNoOp locks in ADR-0064
// F10 forward-compat constraint: an envelope with an unset oneof (or a
// future variant not yet compiled into this binary) MUST NOT panic and
// MUST return nil. Without this guarantee, rolling upgrades where one
// side emits a new event type before the other side understands it
// will crash consumers.
//
// We simulate "unknown variant" via the two already-reachable branches
// of the switch that return nil without side effects: nil payload
// (evt.Payload == nil) and nil event envelope. The default branch is
// guarded by the same return-nil behaviour; future variants added to
// the oneof without a corresponding case will hit it.
func TestApplyCounterJournalEvent_UnknownPayloadIsNoOp(t *testing.T) {
	state := NewShardState(0)
	state.Account(1001).PutForRestore("USDT", Balance{
		Available: dec.New("100"), Frozen: dec.New("0"), Version: 1,
	})

	// Case 1: nil envelope.
	if err := ApplyCounterJournalEvent(state, nil); err != nil {
		t.Fatalf("ApplyCounterJournalEvent(nil): %v", err)
	}

	// Case 2: envelope with Payload=nil (oneof unset — e.g. a consumer
	// decoded a protobuf produced by a newer version that dropped a
	// field we understood, or a malformed record).
	if err := ApplyCounterJournalEvent(state, &eventpb.CounterJournalEvent{
		CounterSeqId: 99,
		// Payload deliberately unset.
	}); err != nil {
		t.Fatalf("ApplyCounterJournalEvent(empty payload): %v", err)
	}

	// Sanity: state untouched.
	b := state.Balance(1001, "USDT")
	if b.Available.String() != "100" || b.Version != 1 {
		t.Fatalf("balance mutated by no-op applies: %+v", b)
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
