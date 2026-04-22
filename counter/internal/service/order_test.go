package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

type mockTxnPublisher struct {
	mu        sync.Mutex
	pairs     []txnPair
	failNext  error
}

type txnPair struct {
	Journal *eventpb.CounterJournalEvent
	Order   *eventpb.OrderEvent
	JKey    string
	OKey    string
}

func (m *mockTxnPublisher) PublishOrderPlacement(
	_ context.Context,
	journalEvt *eventpb.CounterJournalEvent,
	orderEvt *eventpb.OrderEvent,
	journalKey string,
	orderKey string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext != nil {
		err := m.failNext
		m.failNext = nil
		return err
	}
	m.pairs = append(m.pairs, txnPair{journalEvt, orderEvt, journalKey, orderKey})
	return nil
}

type intIDGen struct{ n atomic.Uint64 }

func (g *intIDGen) Next() uint64 { return g.n.Add(1) }

func newOrderFixture(t *testing.T) (*Service, *engine.ShardState, *mockPublisher, *mockTxnPublisher) {
	t.Helper()
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &mockPublisher{}
	txn := &mockTxnPublisher{}
	svc := New(Config{ShardID: 0, ProducerID: "counter-shard-0-main"},
		state, seq, dt, pub, zap.NewNop())
	svc.SetOrderDeps(txn, &intIDGen{})
	// Seed u1 with USDT, u2 with BTC.
	_, _ = svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "seed-u1", UserID: "u1", Asset: "USDT",
		Amount: dec.New("1000"), Type: engine.TransferDeposit,
	})
	_, _ = svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "seed-u2", UserID: "u2", Asset: "BTC",
		Amount: dec.New("1"), Type: engine.TransferDeposit,
	})
	return svc, state, pub, txn
}

func TestPlaceOrderBuyFreezesQuote(t *testing.T) {
	svc, state, _, txn := newOrderFixture(t)
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", ClientOrderID: "c1", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Accepted {
		t.Fatalf("res = %+v, want accepted", res)
	}
	if len(txn.pairs) != 1 {
		t.Fatalf("expected 1 txn pair, got %d", len(txn.pairs))
	}

	bal := state.Balance("u1", "USDT")
	if bal.Available.String() != "800" || bal.Frozen.String() != "200" {
		t.Fatalf("u1 USDT = %+v", bal)
	}
	o := state.Orders().Get(res.OrderID)
	if o == nil || o.Status != engine.OrderStatusPendingNew || o.FrozenAmount.String() != "200" {
		t.Fatalf("order = %+v", o)
	}
}

func TestPlaceOrderRejectedOnInsufficientBalance(t *testing.T) {
	svc, state, _, txn := newOrderFixture(t)
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("10000"), Qty: dec.New("1"), // needs 10000 USDT, have 1000
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted {
		t.Fatal("expected rejection")
	}
	if len(txn.pairs) != 0 {
		t.Fatalf("rejection produced Kafka pair: %d", len(txn.pairs))
	}
	if !state.Balance("u1", "USDT").Frozen.IsZero() {
		t.Fatalf("rejection froze funds: %+v", state.Balance("u1", "USDT"))
	}
}

func TestPlaceOrderIdempotencyViaCOID(t *testing.T) {
	svc, _, _, txn := newOrderFixture(t)
	req := PlaceOrderRequest{
		UserID: "u1", ClientOrderID: "dup",
		Symbol: "BTC-USDT", Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	}
	first, err := svc.PlaceOrder(context.Background(), req)
	if err != nil || !first.Accepted {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, err := svc.PlaceOrder(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if second.Accepted || second.OrderID != first.OrderID {
		t.Fatalf("second not deduped: %+v", second)
	}
	if len(txn.pairs) != 1 {
		t.Fatalf("txn pairs = %d, want 1 (dup should not produce)", len(txn.pairs))
	}
}

func TestPlaceOrderKafkaFailureKeepsStateClean(t *testing.T) {
	svc, state, _, txn := newOrderFixture(t)
	txn.mu.Lock()
	txn.failNext = errors.New("kafka down")
	txn.mu.Unlock()
	_, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !state.Balance("u1", "USDT").Frozen.IsZero() {
		t.Fatal("state mutated despite Kafka failure")
	}
}

func TestCancelOrderTransitionsToPendingCancel(t *testing.T) {
	svc, state, _, _ := newOrderFixture(t)
	placed, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelRes, err := svc.CancelOrder(context.Background(), CancelOrderRequest{
		UserID: "u1", OrderID: placed.OrderID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cancelRes.Accepted {
		t.Fatalf("cancel not accepted: %+v", cancelRes)
	}
	o := state.Orders().Get(placed.OrderID)
	if o.Status != engine.OrderStatusPendingCancel {
		t.Fatalf("status = %s, want pending_cancel", o.Status)
	}
}

func TestCancelOrderNotOwner(t *testing.T) {
	svc, _, _, _ := newOrderFixture(t)
	placed, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := svc.CancelOrder(context.Background(), CancelOrderRequest{
		UserID: "u2", OrderID: placed.OrderID, // u2 is not the owner
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted {
		t.Fatal("expected rejection for wrong owner")
	}
}

// TestCancelOrder_ByIDMiss — order id is not in byID (never existed, or
// already evicted after its terminal transition). CancelOrder returns
// Accepted=false with the "not exist or already finished" reject reason.
func TestCancelOrder_ByIDMiss(t *testing.T) {
	svc, _, _, _ := newOrderFixture(t)
	res, err := svc.CancelOrder(context.Background(), CancelOrderRequest{
		UserID: "u1", OrderID: 999,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Accepted {
		t.Fatalf("unknown order must not be accepted: %+v", res)
	}
	const want = "Cancel Failed, Order Not Exist or Already Finished"
	if res.RejectReason != want {
		t.Fatalf("reject reason = %q, want %q", res.RejectReason, want)
	}
}

// End-to-end (in-process) Trade: u2 sells BTC, u1 buys, synthesize a trade
// event and drive Counter through handleTrade.
func TestEndToEndTradeSettlement(t *testing.T) {
	svc, state, pub, _ := newOrderFixture(t)

	// u1 places BUY 1 BTC @ 100 USDT  -> needs 100 USDT frozen.
	buy, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: engine.SideBid, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err != nil || !buy.Accepted {
		t.Fatalf("buy: %+v %v", buy, err)
	}
	// u2 places SELL 1 BTC @ 100 USDT -> freezes 1 BTC.
	sell, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u2", Symbol: "BTC-USDT",
		Side: engine.SideAsk, OrderType: engine.OrderTypeLimit, TIF: engine.TIFGTC,
		Price: dec.New("100"), Qty: dec.New("1"),
	})
	if err != nil || !sell.Accepted {
		t.Fatalf("sell: %+v %v", sell, err)
	}

	// OrderAccepted for both (as Match would produce).
	if err := svc.HandleTradeEvent(context.Background(), &eventpb.TradeEvent{
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			UserId: "u1", OrderId: buy.OrderID, Symbol: "BTC-USDT",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// Synthesize a Trade (sell is maker on book, buy is taker).
	tradeEvt := &eventpb.TradeEvent{
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
	if err := svc.HandleTradeEvent(context.Background(), tradeEvt); err != nil {
		t.Fatal(err)
	}

	// Final balances:
	//   u1: USDT (1000 - 100) available, 0 frozen;  BTC: +1 available, 0 frozen
	//   u2: BTC (1 - 1) available, 0 frozen;        USDT: +100 available, 0 frozen
	u1Q := state.Balance("u1", "USDT")
	u1B := state.Balance("u1", "BTC")
	u2Q := state.Balance("u2", "USDT")
	u2B := state.Balance("u2", "BTC")
	if u1Q.Available.String() != "900" || u1Q.Frozen.String() != "0" {
		t.Fatalf("u1 USDT = %+v", u1Q)
	}
	if u1B.Available.String() != "1" || u1B.Frozen.String() != "0" {
		t.Fatalf("u1 BTC = %+v", u1B)
	}
	if u2B.Available.String() != "0" || u2B.Frozen.String() != "0" {
		t.Fatalf("u2 BTC = %+v", u2B)
	}
	if u2Q.Available.String() != "100" || u2Q.Frozen.String() != "0" {
		t.Fatalf("u2 USDT = %+v", u2Q)
	}

	// ADR-0063: Filled orders are deleted from byID at the terminal
	// transition. The absence of buy / sell from byID is the success
	// signal here (status check is implicit: only terminal orders get
	// deleted in this path).
	if state.Orders().Get(buy.OrderID) != nil {
		t.Fatalf("buy order %d should be deleted from byID after Filled", buy.OrderID)
	}
	if state.Orders().Get(sell.OrderID) != nil {
		t.Fatalf("sell order %d should be deleted from byID after Filled", sell.OrderID)
	}

	// publisher saw: 2 seed transfers + 1 accepted status event + 2
	// settlement events + 2 terminal OrderStatusEvent(Filled) = 7.
	if got := len(pub.Events()); got < 7 {
		t.Fatalf("publisher events = %d, want >= 7", got)
	}
}
