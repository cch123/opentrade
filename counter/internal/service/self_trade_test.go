package service

import (
	"context"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// TestSelfTradeAppliesBothSides guards against the bug where a self-trade
// (maker_user == taker_user) only applied one settlement leg — the match_seq
// guard on (user, symbol) short-circuited the second call and left half the
// balance deltas unapplied.
func TestSelfTradeAppliesBothSides(t *testing.T) {
	svc, state, _, _ := newOrderFixture(t)
	ctx := context.Background()

	// Give alice enough on both sides to place buy + sell.
	state.CommitBalance("alice", "USDT", engine.Balance{Available: dec.New("1000")})
	state.CommitBalance("alice", "BTC", engine.Balance{Available: dec.New("10")})

	buy, err := svc.PlaceOrder(ctx, PlaceOrderRequest{
		UserID: "alice", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err != nil || !buy.Accepted {
		t.Fatalf("buy: %+v %v", buy, err)
	}
	sell, err := svc.PlaceOrder(ctx, PlaceOrderRequest{
		UserID: "alice", Symbol: "BTC-USDT",
		Side: engine.SideAsk, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err != nil || !sell.Accepted {
		t.Fatalf("sell: %+v %v", sell, err)
	}

	// Snapshot pre-trade balances after both freezes. With the bug, the second
	// settlement side was dropped, so frozen funds stayed locked post-trade.
	preUSDT := state.Balance("alice", "USDT")
	preBTC := state.Balance("alice", "BTC")

	tradeEvt := &eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{},
		MatchSeqId: 42,
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId:             "BTC-USDT:1",
			Symbol:              "BTC-USDT",
			Price:               "100",
			Qty:                 "1",
			MakerUserId:         "alice",
			MakerOrderId:        sell.OrderID, // maker sells
			TakerUserId:         "alice",
			TakerOrderId:        buy.OrderID, // taker buys
			TakerSide:           eventpb.Side_SIDE_BUY,
			MakerFilledQtyAfter: "1",
			TakerFilledQtyAfter: "1",
		}},
	}
	if err := svc.HandleTradeEvent(ctx, tradeEvt); err != nil {
		t.Fatal(err)
	}

	postUSDT := state.Balance("alice", "USDT")
	postBTC := state.Balance("alice", "BTC")

	// Expected: both sides settle against the same account.
	//   USDT: buyer pays 100 from frozen; seller gains 100 to available → net avail +100, frozen -100
	//   BTC : seller releases 1 from frozen; buyer gains 1 to available → net avail +1,   frozen -1
	wantUSDTAvail := preUSDT.Available.Add(dec.New("100"))
	wantUSDTFrozen := preUSDT.Frozen.Sub(dec.New("100"))
	wantBTCAvail := preBTC.Available.Add(dec.New("1"))
	wantBTCFrozen := preBTC.Frozen.Sub(dec.New("1"))

	if postUSDT.Available.Cmp(wantUSDTAvail) != 0 || postUSDT.Frozen.Cmp(wantUSDTFrozen) != 0 {
		t.Errorf("USDT post avail=%s frozen=%s, want avail=%s frozen=%s",
			postUSDT.Available, postUSDT.Frozen, wantUSDTAvail, wantUSDTFrozen)
	}
	if postBTC.Available.Cmp(wantBTCAvail) != 0 || postBTC.Frozen.Cmp(wantBTCFrozen) != 0 {
		t.Errorf("BTC post avail=%s frozen=%s, want avail=%s frozen=%s",
			postBTC.Available, postBTC.Frozen, wantBTCAvail, wantBTCFrozen)
	}

	// match_seq guard still advances so replays are idempotent.
	if got := state.Account("alice").LastMatchSeq("BTC-USDT"); got != 42 {
		t.Errorf("LastMatchSeq = %d, want 42", got)
	}
}

// TestSelfTradeReplayIdempotent ensures a redelivered self-trade event does
// not double-apply the deltas.
func TestSelfTradeReplayIdempotent(t *testing.T) {
	svc, state, _, _ := newOrderFixture(t)
	ctx := context.Background()

	state.CommitBalance("alice", "USDT", engine.Balance{Available: dec.New("1000")})
	state.CommitBalance("alice", "BTC", engine.Balance{Available: dec.New("10")})

	buy, _ := svc.PlaceOrder(ctx, PlaceOrderRequest{
		UserID: "alice", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	sell, _ := svc.PlaceOrder(ctx, PlaceOrderRequest{
		UserID: "alice", Symbol: "BTC-USDT",
		Side: engine.SideAsk, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})

	tradeEvt := &eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{},
		MatchSeqId: 7,
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId: "BTC-USDT:1", Symbol: "BTC-USDT",
			Price: "100", Qty: "1",
			MakerUserId: "alice", MakerOrderId: sell.OrderID,
			TakerUserId: "alice", TakerOrderId: buy.OrderID,
			TakerSide:           eventpb.Side_SIDE_BUY,
			MakerFilledQtyAfter: "1", TakerFilledQtyAfter: "1",
		}},
	}
	if err := svc.HandleTradeEvent(ctx, tradeEvt); err != nil {
		t.Fatal(err)
	}
	after1USDT := state.Balance("alice", "USDT")
	after1BTC := state.Balance("alice", "BTC")

	// Replay: must be a no-op for balances.
	if err := svc.HandleTradeEvent(ctx, tradeEvt); err != nil {
		t.Fatal(err)
	}
	after2USDT := state.Balance("alice", "USDT")
	after2BTC := state.Balance("alice", "BTC")
	if after1USDT.Available.Cmp(after2USDT.Available) != 0 || after1USDT.Frozen.Cmp(after2USDT.Frozen) != 0 {
		t.Errorf("USDT moved on replay: %+v → %+v", after1USDT, after2USDT)
	}
	if after1BTC.Available.Cmp(after2BTC.Available) != 0 || after1BTC.Frozen.Cmp(after2BTC.Frozen) != 0 {
		t.Errorf("BTC moved on replay: %+v → %+v", after1BTC, after2BTC)
	}
}
