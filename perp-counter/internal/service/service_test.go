package service

import (
	"errors"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

type fakeDispatcher struct {
	orders    []Order
	cancels   int
	failOrder bool
}

func (f *fakeDispatcher) DispatchOrder(o Order) error {
	if f.failOrder {
		return errors.New("dispatch boom")
	}
	f.orders = append(f.orders, o)
	return nil
}
func (f *fakeDispatcher) DispatchCancel(string, string, uint64) error { f.cancels++; return nil }

type fakeJournal struct{ evts []*eventpb.PerpJournalEvent }

func (f *fakeJournal) Emit(e *eventpb.PerpJournalEvent) { f.evts = append(f.evts, e) }

func (f *fakeJournal) count(pick func(*eventpb.PerpJournalEvent) bool) int {
	n := 0
	for _, e := range f.evts {
		if pick(e) {
			n++
		}
	}
	return n
}

func newSvc() (*Service, *engine.Engine, *fakeDispatcher, *fakeJournal) {
	eng := engine.New()
	disp := &fakeDispatcher{}
	jr := &fakeJournal{}
	var id uint64
	svc := New(eng, disp, jr, func() uint64 { id++; return id },
		Config{MaxLeverage: dec.New("100"), ProducerID: "perp-shard-0"})
	return svc, eng, disp, jr
}

func placeReq(user, sym string, side eventpb.Side, price, qty, lev string, reduceOnly bool) *perprpc.PlaceOrderRequest {
	return &perprpc.PlaceOrderRequest{
		UserId: user, Symbol: sym, Side: side,
		OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT, Tif: eventpb.TimeInForce_TIME_IN_FORCE_GTC,
		Price: price, Qty: qty, Leverage: lev, ReduceOnly: reduceOnly,
	}
}

func eqd(t *testing.T, got dec.Decimal, want, what string) {
	t.Helper()
	if got.Cmp(dec.New(want)) != 0 {
		t.Fatalf("%s: got %s want %s", what, got.String(), want)
	}
}

func TestPlaceOrder_ReservesAndDispatches(t *testing.T) {
	svc, eng, disp, jr := newSvc()
	eng.Deposit("u1", dec.New("1000"))
	resp, err := svc.PlaceOrder(placeReq("u1", "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if err != nil || !resp.Accepted {
		t.Fatalf("place: err=%v accepted=%v reason=%s", err, resp.Accepted, resp.RejectReason)
	}
	w := eng.WalletOf("u1")
	eqd(t, w.Reserved, "10", "reserved IM")
	eqd(t, w.Available, "990", "available after reserve")
	if len(disp.orders) != 1 || disp.orders[0].OrderID != resp.OrderId {
		t.Fatalf("order not dispatched: %+v", disp.orders)
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetOrderStatus() != nil }) != 1 {
		t.Fatal("expected one order-status journal event")
	}
}

func TestPlaceOrder_InsufficientMargin(t *testing.T) {
	svc, eng, disp, _ := newSvc()
	eng.Deposit("u1", dec.New("5")) // IM needed is 10
	resp, _ := svc.PlaceOrder(placeReq("u1", "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if resp.Accepted || resp.RejectReason != "insufficient_margin" {
		t.Fatalf("want insufficient_margin reject, got accepted=%v reason=%s", resp.Accepted, resp.RejectReason)
	}
	if len(disp.orders) != 0 {
		t.Fatal("rejected order must not dispatch")
	}
	eqd(t, eng.WalletOf("u1").Available, "5", "available unchanged on reject")
}

func TestPlaceOrder_ReduceOnlyNeedsOppositePosition(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit("u1", dec.New("1000"))
	resp, _ := svc.PlaceOrder(placeReq("u1", "BTC-USDT-PERP", eventpb.Side_SIDE_SELL, "100", "1", "10", true))
	if resp.Accepted || resp.RejectReason != "reduce_only_requires_opposite_position" {
		t.Fatalf("want reduce_only reject, got accepted=%v reason=%s", resp.Accepted, resp.RejectReason)
	}
}

func TestPlaceOrder_DispatchFailureReleasesMargin(t *testing.T) {
	svc, eng, disp, _ := newSvc()
	disp.failOrder = true
	eng.Deposit("u1", dec.New("1000"))
	resp, _ := svc.PlaceOrder(placeReq("u1", "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if resp.Accepted || resp.RejectReason != "dispatch_failed" {
		t.Fatalf("want dispatch_failed, got accepted=%v reason=%s", resp.Accepted, resp.RejectReason)
	}
	w := eng.WalletOf("u1")
	eqd(t, w.Reserved, "0", "IM released after dispatch failure")
	eqd(t, w.Available, "1000", "available restored after dispatch failure")
}

func TestHandleTrade_SettlesBothLegs(t *testing.T) {
	svc, eng, _, jr := newSvc()
	eng.Deposit("u1", dec.New("1000"))
	eng.Deposit("u2", dec.New("1000"))
	rT, _ := svc.PlaceOrder(placeReq("u1", "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	rM, _ := svc.PlaceOrder(placeReq("u2", "BTC-USDT-PERP", eventpb.Side_SIDE_SELL, "100", "1", "10", false))

	tr := &eventpb.Trade{
		TradeId: "t1", Symbol: "BTC-USDT-PERP", Price: "100", Qty: "1",
		MakerUserId: "u2", MakerOrderId: rM.OrderId, TakerUserId: "u1", TakerOrderId: rT.OrderId,
		TakerSide:           eventpb.Side_SIDE_BUY,
		MakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		MakerFilledQtyAfter: "1", TakerFilledQtyAfter: "1",
	}
	svc.HandleTrade(tr, 1)

	p1, ok := eng.PositionOf("u1", "BTC-USDT-PERP")
	if !ok || p1.Side.String() != "buy" {
		t.Fatalf("u1 should be long: %+v ok=%v", p1, ok)
	}
	eqd(t, p1.Size, "1", "u1 size")
	eqd(t, p1.Entry, "100", "u1 entry")
	p2, ok := eng.PositionOf("u2", "BTC-USDT-PERP")
	if !ok || p2.Side.String() != "sell" {
		t.Fatalf("u2 should be short: %+v ok=%v", p2, ok)
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetSettlement() != nil }) != 2 {
		t.Fatal("expected two settlement events (both legs)")
	}
	// FILLED orders are dropped.
	if svc.OrderCount() != 0 {
		t.Fatalf("filled orders should be removed, have %d", svc.OrderCount())
	}
}

func TestHandleTrade_ReplayGuarded(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit("u1", dec.New("1000"))
	eng.Deposit("u2", dec.New("1000"))
	rT, _ := svc.PlaceOrder(placeReq("u1", "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "2", "10", false))
	rM, _ := svc.PlaceOrder(placeReq("u2", "BTC-USDT-PERP", eventpb.Side_SIDE_SELL, "100", "2", "10", false))
	// Partial fill 1 of 2 → orders stay live so the seq guard (not order
	// deletion) is what blocks the replay.
	tr := &eventpb.Trade{
		TradeId: "t1", Symbol: "BTC-USDT-PERP", Price: "100", Qty: "1",
		MakerUserId: "u2", MakerOrderId: rM.OrderId, TakerUserId: "u1", TakerOrderId: rT.OrderId,
		TakerSide:           eventpb.Side_SIDE_BUY,
		MakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		MakerFilledQtyAfter: "1", TakerFilledQtyAfter: "1",
	}
	svc.HandleTrade(tr, 7)
	svc.HandleTrade(tr, 7) // replay, same seq

	p1, _ := eng.PositionOf("u1", "BTC-USDT-PERP")
	eqd(t, p1.Size, "1", "u1 size unchanged by replay")
}

func TestHandleTrade_SelfTradeAppliesBothLegs(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit("u1", dec.New("1000"))
	rM, _ := svc.PlaceOrder(placeReq("u1", "BTC-USDT-PERP", eventpb.Side_SIDE_SELL, "100", "1", "10", false))
	rT, _ := svc.PlaceOrder(placeReq("u1", "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	tr := &eventpb.Trade{
		TradeId: "t1", Symbol: "BTC-USDT-PERP", Price: "100", Qty: "1",
		MakerUserId: "u1", MakerOrderId: rM.OrderId, TakerUserId: "u1", TakerOrderId: rT.OrderId,
		TakerSide:           eventpb.Side_SIDE_BUY,
		MakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		MakerFilledQtyAfter: "1", TakerFilledQtyAfter: "1",
	}
	svc.HandleTrade(tr, 1)
	// Buy 1 then sell 1 at the same price nets flat — proving BOTH legs were
	// applied (if the maker leg were dropped by the guard, u1 would be long 1).
	if _, ok := eng.PositionOf("u1", "BTC-USDT-PERP"); ok {
		t.Fatal("self-trade should net flat (both legs applied)")
	}
}
