package service

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/dec"
)

const perpSym = "BTC-USDT-PERP"

func queryReq(user uint64, orderID uint64) *perprpc.QueryOrderRequest {
	return &perprpc.QueryOrderRequest{UserId: user, OrderId: orderID}
}

// rejected/cancelled/expired all wrap into a TradeEvent the consumer decodes;
// these helpers exercise the real HandleTradeEvent entry point.
func rejectedEvt(seq uint64, user uint64, orderID uint64) *eventpb.TradeEvent {
	return &eventpb.TradeEvent{MatchSeqId: seq, Payload: &eventpb.TradeEvent_Rejected{
		Rejected: &eventpb.OrderRejected{UserId: user, OrderId: orderID, Symbol: perpSym,
			Reason: eventpb.RejectReason_REJECT_REASON_POST_ONLY_WOULD_TAKE}}}
}

func cancelledEvt(seq uint64, user uint64, orderID uint64) *eventpb.TradeEvent {
	return &eventpb.TradeEvent{MatchSeqId: seq, Payload: &eventpb.TradeEvent_Cancelled{
		Cancelled: &eventpb.OrderCancelled{UserId: user, OrderId: orderID, Symbol: perpSym, FilledQty: "0"}}}
}

func acceptedEvt(seq uint64, user uint64, orderID uint64) *eventpb.TradeEvent {
	return &eventpb.TradeEvent{MatchSeqId: seq, Payload: &eventpb.TradeEvent_Accepted{
		Accepted: &eventpb.OrderAccepted{UserId: user, OrderId: orderID, Symbol: perpSym}}}
}

func TestHandleAccepted_MarksNew(t *testing.T) {
	svc, eng, _, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))

	svc.HandleTradeEvent(acceptedEvt(1, user1s, r.OrderId), 0, 10)

	q, ok := svc.QueryOrder(queryReq(user1, r.OrderId))
	if !ok || q.Status != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW {
		t.Fatalf("accept should mark NEW, got status=%v ok=%v", q.GetStatus(), ok)
	}
	// PENDING_NEW (at placement) + NEW (this accept) = 2 status journal events.
	if n := jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetOrderStatus() != nil }); n != 2 {
		t.Fatalf("want 2 order-status events, got %d", n)
	}
}

func TestHandleAccepted_Idempotent(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	svc.HandleTradeEvent(acceptedEvt(1, user1s, r.OrderId), 0, 1)
	svc.HandleTradeEvent(acceptedEvt(1, user1s, r.OrderId), 0, 2) // replay
	q, _ := svc.QueryOrder(queryReq(user1, r.OrderId))
	if q.Status != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW {
		t.Fatalf("replayed accept should stay NEW, got %v", q.GetStatus())
	}
}

func TestHandleRejected_ReleasesFullIM(t *testing.T) {
	svc, eng, _, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	eqd(t, eng.WalletOf(user1).Reserved, "10", "reserved after place")

	svc.HandleTradeEvent(rejectedEvt(1, user1s, r.OrderId), 0, 5)

	w := eng.WalletOf(user1)
	eqd(t, w.Reserved, "0", "reserved released on reject")
	eqd(t, w.Available, "1000", "available restored on reject")
	if svc.OrderCount() != 0 {
		t.Fatalf("rejected order should be evicted, have %d", svc.OrderCount())
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool {
		os := e.GetOrderStatus()
		return os != nil && os.GetNewStatus() == eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED &&
			os.GetRejectReason() == eventpb.RejectReason_REJECT_REASON_POST_ONLY_WOULD_TAKE
	}) != 1 {
		t.Fatal("expected one REJECTED status event carrying the reject reason")
	}
}

func TestHandleRejected_Idempotent(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	svc.HandleTradeEvent(rejectedEvt(1, user1s, r.OrderId), 0, 5)
	svc.HandleTradeEvent(rejectedEvt(1, user1s, r.OrderId), 0, 6) // replay: order gone, no double release
	w := eng.WalletOf(user1)
	eqd(t, w.Reserved, "0", "reserved still zero after replay")
	eqd(t, w.Available, "1000", "available not double-credited on replay")
}

func TestHandleCancelled_ReleasesRemainingIMAfterPartialFill(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.Deposit(user2, dec.New("1000"))
	// u1 buys 2 @100 lev10 → IM 20 reserved; u2 sells 2 to be the maker.
	rT, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "2", "10", false))
	rM, _ := svc.PlaceOrder(placeReq(user2, perpSym, eventpb.Side_SIDE_SELL, "100", "2", "10", false))
	eqd(t, eng.WalletOf(user1).Reserved, "20", "u1 reserved after place")

	// Partial fill 1 of 2: 10 IM converts to position margin, 10 stays reserved.
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t1", Symbol: perpSym, Price: "100", Qty: "1",
		MakerUserId: user2s, MakerOrderId: rM.OrderId, TakerUserId: user1s, TakerOrderId: rT.OrderId,
		TakerSide:           eventpb.Side_SIDE_BUY,
		MakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		MakerFilledQtyAfter: "1", TakerFilledQtyAfter: "1",
	}, 1)
	w := eng.WalletOf(user1)
	eqd(t, w.Reserved, "10", "u1 reserved after partial fill (half committed)")

	// Cancel the unfilled remainder: only the still-held 10 releases.
	svc.HandleTradeEvent(cancelledEvt(2, user1s, rT.OrderId), 0, 20)
	w = eng.WalletOf(user1)
	eqd(t, w.Reserved, "0", "remaining IM released on cancel")
	eqd(t, w.Available, "990", "available = 1000 - 10 position margin still held")
	p, ok := eng.PositionOf(user1, perpSym, 0)
	if !ok {
		t.Fatal("u1 should still hold the filled position")
	}
	eqd(t, p.Size, "1", "filled position survives the cancel")
	eqd(t, p.Margin, "10", "position margin retained")
}

func TestHandleTradeEvent_RecordsOffset(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	svc.HandleTradeEvent(acceptedEvt(1, user1s, r.OrderId), 3, 42)
	if got := svc.ConsumedOffsets()[3]; got != 43 {
		t.Fatalf("offset for partition 3 = %d, want 43 (record offset + 1)", got)
	}
}

func TestHandleTradeEvent_NilSafe(t *testing.T) {
	svc, _, _, _ := newSvc()
	svc.HandleTradeEvent(nil, 0, 0) // must not panic
}
