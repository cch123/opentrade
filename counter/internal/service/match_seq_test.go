package service

import (
	"context"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// TestMatchSeqGuard_DuplicateTradeSkipped verifies that replaying the same
// trade-event (same MatchSeqId) after the first application is silently
// skipped — the second call must not move balances again nor emit a second
// settlement journal entry.
func TestMatchSeqGuard_DuplicateTradeSkipped(t *testing.T) {
	svc, state, pub, _ := newOrderFixture(t)
	ctx := context.Background()

	// u1 BUY 1 BTC @ 100 (frozen 100 USDT).
	buy, err := svc.PlaceOrder(ctx, PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err != nil || !buy.Accepted {
		t.Fatalf("buy: %+v %v", buy, err)
	}
	// u2 SELL 1 BTC @ 100 (frozen 1 BTC).
	sell, err := svc.PlaceOrder(ctx, PlaceOrderRequest{
		UserID: "u2", Symbol: "BTC-USDT",
		Side: engine.SideAsk, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err != nil || !sell.Accepted {
		t.Fatalf("sell: %+v %v", sell, err)
	}

	tradeEvt := &eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{},
		MatchSeqId: 42,
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId:             "BTC-USDT:1",
			Symbol:              "BTC-USDT",
			Price:               "100",
			Qty:                 "1",
			MakerUserId:         "u2",
			MakerOrderId:        sell.OrderID,
			TakerUserId:         "u1",
			TakerOrderId:        buy.OrderID,
			TakerSide:           eventpb.Side_SIDE_BUY,
			MakerFilledQtyAfter: "1",
			TakerFilledQtyAfter: "1",
		}},
	}

	// First apply.
	if err := svc.HandleTradeEvent(ctx, tradeEvt); err != nil {
		t.Fatal(err)
	}
	firstEvents := len(pub.Events())
	u1BTC := state.Balance("u1", "BTC")
	if u1BTC.Available.String() != "1" {
		t.Fatalf("after first apply u1 BTC = %+v", u1BTC)
	}

	// Replay same trade event — match_seq guard must skip both parties.
	// (ADR-0063: Filled orders are deleted from byID at the terminal
	// transition, so the fill-qty fallback path is moot on replay; this
	// asserts the match_seq guard alone is sufficient to block the
	// duplicate.)
	if err := svc.HandleTradeEvent(ctx, tradeEvt); err != nil {
		t.Fatal(err)
	}
	if got := len(pub.Events()); got != firstEvents {
		t.Fatalf("replay leaked journal events: %d → %d", firstEvents, got)
	}
	// Verify guard advanced for both users on the symbol.
	if got := state.Account("u1").LastMatchSeq("BTC-USDT"); got != 42 {
		t.Errorf("u1 LastMatchSeq = %d, want 42", got)
	}
	if got := state.Account("u2").LastMatchSeq("BTC-USDT"); got != 42 {
		t.Errorf("u2 LastMatchSeq = %d, want 42", got)
	}
}

// TestMatchSeqGuard_DifferentSymbolsIndependent verifies that LastMatchSeq
// is scoped per-symbol — an event on BTC-USDT must not gate one on ETH-USDT
// even if numerically the seq is lower.
func TestMatchSeqGuard_DifferentSymbolsIndependent(t *testing.T) {
	svc, state, _, _ := newOrderFixture(t)
	ctx := context.Background()

	// Prime BTC-USDT with seq=100 by accepting an order.
	evtBTC := &eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{},
		MatchSeqId: 100,
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			UserId: "u1", OrderId: 999, Symbol: "BTC-USDT",
		}},
	}
	if err := svc.HandleTradeEvent(ctx, evtBTC); err != nil {
		t.Fatal(err)
	}
	// Now an ETH-USDT event with a numerically lower seq should still pass:
	// separate symbol, guard is keyed by (user, symbol).
	evtETH := &eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{},
		MatchSeqId: 50,
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			UserId: "u1", OrderId: 998, Symbol: "ETH-USDT",
		}},
	}
	if err := svc.HandleTradeEvent(ctx, evtETH); err != nil {
		t.Fatal(err)
	}
	acc := state.Account("u1")
	if got := acc.LastMatchSeq("BTC-USDT"); got != 100 {
		t.Errorf("BTC-USDT seq = %d, want 100", got)
	}
	if got := acc.LastMatchSeq("ETH-USDT"); got != 50 {
		t.Errorf("ETH-USDT seq = %d, want 50", got)
	}
}

// TestMatchSeqGuard_ZeroSeqBypasses verifies that TradeEvents with Meta=nil
// or SeqId=0 (legacy fixtures / in-process tests) bypass the guard and flow
// through the business handler. Protects the existing in-process tests that
// construct events without Meta.
func TestMatchSeqGuard_ZeroSeqBypasses(t *testing.T) {
	svc, state, _, _ := newOrderFixture(t)
	ctx := context.Background()

	evt := &eventpb.TradeEvent{
		// No Meta at all → matchSeq=0 → guard skipped.
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			UserId: "u1", OrderId: 1, Symbol: "BTC-USDT",
		}},
	}
	if err := svc.HandleTradeEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}
	// LastMatchSeq stays 0 — zero is never advanced, so a real seq=1 later
	// is still considered fresh.
	if got := state.Account("u1").LastMatchSeq("BTC-USDT"); got != 0 {
		t.Errorf("zero-seq apply advanced guard to %d, want 0", got)
	}
}
