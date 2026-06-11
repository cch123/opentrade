package service

// product_api_test.go covers the ADR-0078 order-command surface: COID
// idempotency (修订 #3), amend cancel+new with its races (§2), batch
// per-item results + cancel-all (§3), pre-check (§4), and the duplicate-
// reject replay convergence (修订 #6).

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

func placeReqCOID(user uint64, coid, price, qty string) *perprpc.PlaceOrderRequest {
	r := placeReq(user, perpSym, eventpb.Side_SIDE_BUY, price, qty, "10", false)
	r.ClientOrderId = coid
	return r
}

// --- COID dedup (修订 #3) -----------------------------------------------------

func TestPlaceOrder_COIDDedup_ActiveOrder(t *testing.T) {
	svc, eng, disp, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	first, _ := svc.PlaceOrder(placeReqCOID(user1, "c-1", "100", "1"))
	if !first.Accepted {
		t.Fatalf("first place rejected: %s", first.RejectReason)
	}
	dup, _ := svc.PlaceOrder(placeReqCOID(user1, "c-1", "100", "1"))
	if dup.Accepted || dup.OrderId != first.OrderId {
		t.Fatalf("dedup hit must return original id with accepted=false: %+v", dup)
	}
	if len(disp.orders) != 1 {
		t.Fatalf("duplicate must not dispatch: %d", len(disp.orders))
	}
	eqd(t, eng.WalletOf(user1).Reserved, "10", "no double reservation")
}

func TestPlaceOrder_COIDDedup_TerminalRing(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	first, _ := svc.PlaceOrder(placeReqCOID(user1, "c-ring", "100", "1"))
	fillOrder(svc, user1, first.OrderId, eventpb.Side_SIDE_BUY, "100", "1", nextSeq()) // FILLED → retired
	if svc.OrderCount() != 0 {
		t.Fatal("order should be evicted on terminal")
	}
	dup, _ := svc.PlaceOrder(placeReqCOID(user1, "c-ring", "100", "1"))
	if dup.Accepted || dup.OrderId != first.OrderId {
		t.Fatalf("terminal-ring dedup must return original id: %+v", dup)
	}
}

func TestPlaceOrder_COIDDedup_DispatchFailureDoesNotRetire(t *testing.T) {
	svc, eng, disp, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	disp.failOrder = true
	r, _ := svc.PlaceOrder(placeReqCOID(user1, "c-retry", "100", "1"))
	if r.Accepted || r.RejectReason != "dispatch_failed" {
		t.Fatalf("want dispatch_failed: %+v", r)
	}
	disp.failOrder = false
	retry, _ := svc.PlaceOrder(placeReqCOID(user1, "c-retry", "100", "1"))
	if !retry.Accepted {
		t.Fatalf("retry after dispatch failure must be admitted fresh: %+v", retry)
	}
}

func TestCOIDRing_SnapshotRoundtrip(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	first, _ := svc.PlaceOrder(placeReqCOID(user1, "c-snap", "100", "1"))
	fillOrder(svc, user1, first.OrderId, eventpb.Side_SIDE_BUY, "100", "1", nextSeq())

	_, snap, err := svc.Capture(nil)
	if err != nil {
		t.Fatal(err)
	}
	svc2, eng2, _, _ := newSvc()
	eng2.Deposit(user1, dec.New("1000"))
	svc2.Restore(snap)
	dup, _ := svc2.PlaceOrder(placeReqCOID(user1, "c-snap", "100", "1"))
	if dup.Accepted || dup.OrderId != first.OrderId {
		t.Fatalf("ring must survive restore: %+v", dup)
	}
}

// --- duplicate-reject replay convergence (修订 #6) ----------------------------

func TestHandleRejected_DuplicateOrderIDIgnoredForTrackedOrder(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	svc.HandleTradeEvent(&eventpb.TradeEvent{MatchSeqId: 1, Payload: &eventpb.TradeEvent_Rejected{
		Rejected: &eventpb.OrderRejected{UserId: user1, OrderId: r.OrderId, Symbol: perpSym,
			Reason: eventpb.RejectReason_REJECT_REASON_DUPLICATE_ORDER_ID}}}, 0, 1)
	if svc.OrderCount() != 1 {
		t.Fatal("duplicate-order-id reject for a tracked order must be ignored (replay echo)")
	}
	eqd(t, eng.WalletOf(user1).Reserved, "10", "reservation must stay held")
}

// --- amend (§2) ----------------------------------------------------------------

func amendReq(user, orderID uint64, price, qty string) *perprpc.AmendOrderRequest {
	return &perprpc.AmendOrderRequest{UserId: user, OrderId: orderID, NewPrice: price, NewQty: qty}
}

func TestAmendOrder_ConservativeHappyPath(t *testing.T) {
	svc, eng, disp, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReqCOID(user1, "c-am", "100", "1"))

	am, err := svc.AmendOrder(amendReq(user1, r.OrderId, "90", "2"))
	if err != nil || !am.Accepted || am.NewOrderId == 0 {
		t.Fatalf("amend: err=%v resp=%+v", err, am)
	}
	if disp.cancels != 1 {
		t.Fatal("amend must dispatch the cancel")
	}
	if len(disp.orders) != 1 {
		t.Fatal("replacement must NOT dispatch before the old order is terminal (conservative)")
	}
	// Old order terminal → continuation places the replacement.
	svc.HandleTradeEvent(cancelledEvt(0, user1, r.OrderId), 0, 1)
	if len(disp.orders) != 2 {
		t.Fatal("replacement must dispatch after old terminal")
	}
	placed := disp.orders[1].GetPlaced()
	if placed.GetOrderId() != am.NewOrderId || placed.GetPrice() != "90" || placed.GetQty() != "2" {
		t.Fatalf("replacement wire mismatch: %+v", placed)
	}
	if placed.GetClientOrderId() != "c-am" {
		t.Fatal("replacement must inherit the client_order_id")
	}
	// IM: old 10 released, new 90*2/10 = 18 reserved.
	eqd(t, eng.WalletOf(user1).Reserved, "18", "replacement reservation")
	eqd(t, eng.WalletOf(user1).Available, "982", "available after swap")
	// Journal: REQUESTED then COMPLETED.
	states := amendStates(jr)
	if len(states) != 2 || states[0] != eventpb.PerpAmendEvent_STATE_REQUESTED || states[1] != eventpb.PerpAmendEvent_STATE_COMPLETED {
		t.Fatalf("amend journal states: %v", states)
	}
	// The dedup index follows the replacement.
	dup, _ := svc.PlaceOrder(placeReqCOID(user1, "c-am", "100", "1"))
	if dup.Accepted || dup.OrderId != am.NewOrderId {
		t.Fatalf("active COID must point at the replacement: %+v", dup)
	}
}

func amendStates(jr *fakeJournal) []eventpb.PerpAmendEvent_State {
	var out []eventpb.PerpAmendEvent_State
	for _, e := range jr.evts {
		if a := e.GetAmend(); a != nil {
			out = append(out, a.GetState())
		}
	}
	return out
}

func TestAmendOrder_PartialFillRace_PlacesRemainder(t *testing.T) {
	svc, _, disp, _ := newSvc()
	eng := svc.eng
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "2", "10", false))
	am, _ := svc.AmendOrder(amendReq(user1, r.OrderId, "95", "3"))
	if !am.Accepted {
		t.Fatalf("amend rejected: %s", am.RejectReason)
	}
	// In-flight fill of 1 lands before the cancel confirmation.
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t-partial", Symbol: perpSym, Price: "100", Qty: "1",
		TakerUserId: user1, TakerOrderId: r.OrderId, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		TakerFilledQtyAfter: "1",
	}, nextSeq())
	svc.HandleTradeEvent(cancelledEvt(0, user1, r.OrderId), 0, 2)
	placed := disp.orders[len(disp.orders)-1].GetPlaced()
	if placed.GetOrderId() != am.NewOrderId || placed.GetQty() != "2" {
		t.Fatalf("replacement qty must be new_total - filled = 3 - 1 = 2: %+v", placed)
	}
}

func TestAmendOrder_FullyFilledRace_AlreadyFilled(t *testing.T) {
	svc, eng, disp, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	am, _ := svc.AmendOrder(amendReq(user1, r.OrderId, "95", "1"))
	if !am.Accepted {
		t.Fatalf("amend rejected: %s", am.RejectReason)
	}
	dispatched := len(disp.orders)
	// The old order fills in full before the cancel lands.
	fillOrder(svc, user1, r.OrderId, eventpb.Side_SIDE_BUY, "100", "1", nextSeq())
	if len(disp.orders) != dispatched {
		t.Fatal("no replacement when old fills reach the new total qty")
	}
	states := amendStates(jr)
	if states[len(states)-1] != eventpb.PerpAmendEvent_STATE_ALREADY_FILLED {
		t.Fatalf("want ALREADY_FILLED terminal, got %v", states)
	}
}

func TestAmendOrder_Guards(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "2", "10", false))

	if am, _ := svc.AmendOrder(amendReq(user1, 99999, "90", "1")); am.RejectReason != "not_found" {
		t.Fatalf("unknown order: %+v", am)
	}
	if am, _ := svc.AmendOrder(amendReq(user2, r.OrderId, "90", "1")); am.RejectReason != "not_found" {
		t.Fatalf("foreign order: %+v", am)
	}
	// qty at/below filled is rejected up front.
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t-g", Symbol: perpSym, Price: "100", Qty: "1",
		TakerUserId: user1, TakerOrderId: r.OrderId, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		TakerFilledQtyAfter: "1",
	}, nextSeq())
	if am, _ := svc.AmendOrder(amendReq(user1, r.OrderId, "90", "1")); am.RejectReason != "qty_not_above_filled" {
		t.Fatalf("qty <= filled: %+v", am)
	}
	// Second amend while one is pending.
	if am, _ := svc.AmendOrder(amendReq(user1, r.OrderId, "90", "2")); !am.Accepted {
		t.Fatalf("first amend rejected: %+v", am)
	}
	if am, _ := svc.AmendOrder(amendReq(user1, r.OrderId, "91", "2")); am.RejectReason != "cancel_in_progress" {
		t.Fatalf("amend on PENDING_CANCEL order: %+v", am)
	}
	// Market orders are not amendable.
	mkt := placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "", "1", "10", false)
	mkt.OrderType = eventpb.OrderType_ORDER_TYPE_MARKET
	rm, _ := svc.PlaceOrder(mkt)
	if !rm.Accepted {
		t.Fatalf("market place rejected: %s", rm.RejectReason)
	}
	if am, _ := svc.AmendOrder(amendReq(user1, rm.OrderId, "90", "2")); am.RejectReason != "market_order_not_amendable" {
		t.Fatalf("market amend: %+v", am)
	}
}

func TestCancelOrder_AbortsPendingAmend(t *testing.T) {
	svc, eng, disp, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	am, _ := svc.AmendOrder(amendReq(user1, r.OrderId, "90", "2"))
	if !am.Accepted {
		t.Fatalf("amend rejected: %s", am.RejectReason)
	}
	c, _ := svc.CancelOrder(&perprpc.CancelOrderRequest{UserId: user1, OrderId: r.OrderId})
	if !c.Accepted {
		t.Fatalf("explicit cancel rejected: %+v", c)
	}
	dispatched := len(disp.orders)
	svc.HandleTradeEvent(cancelledEvt(0, user1, r.OrderId), 0, 1)
	if len(disp.orders) != dispatched {
		t.Fatal("aborted amend must not place a replacement")
	}
	states := amendStates(jr)
	if states[len(states)-1] != eventpb.PerpAmendEvent_STATE_ABORTED_BY_CANCEL {
		t.Fatalf("want ABORTED_BY_CANCEL, got %v", states)
	}
}

func TestAmend_SnapshotRoundtrip_ReplacementKeepsPreallocatedID(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	am, _ := svc.AmendOrder(amendReq(user1, r.OrderId, "90", "2"))

	engSnap, snap, err := svc.Capture(nil)
	if err != nil {
		t.Fatal(err)
	}
	// Restart: restore engine + service, then replay the terminal event.
	eng2 := engine.New()
	eng2.Restore(engSnap)
	disp2 := &fakeDispatcher{}
	var id uint64 = 9000 // fresh snowflake namespace after restart
	svc2 := New(eng2, disp2, &fakeJournal{}, func() uint64 { id++; return id },
		Config{MaxLeverage: dec.New("100"), ProducerID: "perp-shard-0"})
	svc2.Restore(snap)
	svc2.HandleTradeEvent(cancelledEvt(0, user1, r.OrderId), 0, 1)
	if len(disp2.orders) != 1 {
		t.Fatal("replayed terminal must re-run the continuation")
	}
	if got := disp2.orders[0].GetPlaced().GetOrderId(); got != am.NewOrderId {
		t.Fatalf("replacement must reuse the PRE-ALLOCATED id across restart: got %d want %d", got, am.NewOrderId)
	}
}

// --- batch + cancel-all (§3) ----------------------------------------------------

func TestBatchPlaceOrders_PartialSuccess(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("25")) // funds one 10-IM order plus fees, not two
	req := &perprpc.BatchPlaceOrdersRequest{
		UserId: user1, BatchId: "b-1",
		Items: []*perprpc.PlaceOrderRequest{
			placeReq(0, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false),
			placeReq(0, perpSym, eventpb.Side_SIDE_BUY, "100", "bogus", "10", false), // shape error stays per-item
			placeReq(0, perpSym, eventpb.Side_SIDE_BUY, "100", "2", "10", false),     // 20 IM > remaining 15
		},
	}
	resp, err := svc.BatchPlaceOrders(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.BatchId != "b-1" || len(resp.Items) != 3 {
		t.Fatalf("batch echo: %+v", resp)
	}
	if !resp.Items[0].Accepted {
		t.Fatalf("item 0 should pass: %s", resp.Items[0].RejectReason)
	}
	if resp.Items[1].Accepted || resp.Items[1].RejectReason == "" {
		t.Fatalf("item 1 should fail with a shape reason: %+v", resp.Items[1])
	}
	if resp.Items[2].Accepted || resp.Items[2].RejectReason != "insufficient_margin" {
		t.Fatalf("item 2 should fail margin: %+v", resp.Items[2])
	}
}

func TestBatchPlaceOrders_UserMismatchRejected(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	resp, _ := svc.BatchPlaceOrders(&perprpc.BatchPlaceOrdersRequest{
		UserId: user1,
		Items:  []*perprpc.PlaceOrderRequest{placeReq(user2, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false)},
	})
	if resp.Items[0].Accepted || resp.Items[0].RejectReason != "user_mismatch" {
		t.Fatalf("cross-user item must be rejected: %+v", resp.Items[0])
	}
}

func TestBatchCancelOrders_PerItemResults(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	resp, _ := svc.BatchCancelOrders(&perprpc.BatchCancelOrdersRequest{
		UserId: user1, BatchId: "bc-1", OrderIds: []uint64{r.OrderId, 424242},
	})
	if !resp.Items[0].Accepted {
		t.Fatalf("live order cancel should pass: %+v", resp.Items[0])
	}
	if resp.Items[1].Accepted || resp.Items[1].RejectReason != "not_found" {
		t.Fatalf("unknown order: %+v", resp.Items[1])
	}
}

func TestCancelAllOrders_SymbolScope(t *testing.T) {
	svc, eng, disp, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark("ETH-USDT-PERP", dec.New("10"))
	r1, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	r2, _ := svc.PlaceOrder(placeReq(user1, "ETH-USDT-PERP", eventpb.Side_SIDE_BUY, "10", "1", "10", false))
	if !r1.Accepted || !r2.Accepted {
		t.Fatal("setup places failed")
	}
	resp, _ := svc.CancelAllOrders(&perprpc.CancelAllOrdersRequest{UserId: user1, Symbol: perpSym})
	if len(resp.Items) != 1 || resp.Items[0].OrderId != r1.OrderId || !resp.Items[0].Accepted {
		t.Fatalf("symbol scope must target only the BTC order: %+v", resp.Items)
	}
	if disp.cancels != 1 {
		t.Fatalf("one cancel dispatched, got %d", disp.cancels)
	}
	all, _ := svc.CancelAllOrders(&perprpc.CancelAllOrdersRequest{UserId: user1})
	if len(all.Items) != 2 { // BTC order is PENDING_CANCEL (non-terminal) + ETH order
		t.Fatalf("all-scope items: %+v", all.Items)
	}
}

// --- pre-check (§4) -------------------------------------------------------------

func TestPreCheckOrder_MirrorsAdmissionWithoutMutation(t *testing.T) {
	svc, eng, disp, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	pre, err := svc.PreCheckOrder(&perprpc.PreCheckOrderRequest{
		Order: placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false),
	})
	if err != nil || !pre.WouldAccept {
		t.Fatalf("precheck: err=%v resp=%+v", err, pre)
	}
	if pre.RequiredInitialMargin != "10" {
		t.Fatalf("IM estimate: %s", pre.RequiredInitialMargin)
	}
	if pre.MaxOpenQty != "100" { // 1000 / (100/10 + 0 fee)
		t.Fatalf("max_open_qty estimate: %s", pre.MaxOpenQty)
	}
	w := eng.WalletOf(user1)
	eqd(t, w.Reserved, "0", "precheck must not reserve")
	eqd(t, w.Available, "1000", "precheck must not move cash")
	if len(disp.orders) != 0 {
		t.Fatal("precheck must not dispatch")
	}
	// And the rejecting path mirrors PlaceOrder's reason.
	pre2, _ := svc.PreCheckOrder(&perprpc.PreCheckOrderRequest{
		Order: placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "200", "10", false),
	})
	if pre2.WouldAccept || pre2.RejectReason != "insufficient_margin" {
		t.Fatalf("precheck reject: %+v", pre2)
	}
}

func TestPreCheckOrder_NoLeverageWriteThrough(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	// First place pins config leverage 10; a precheck at 20 must neither
	// mutate the config nor reject differently than PlaceOrder would.
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !r.Accepted {
		t.Fatal("setup place failed")
	}
	pre, _ := svc.PreCheckOrder(&perprpc.PreCheckOrderRequest{
		Order: placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "20", false),
	})
	if pre.WouldAccept || pre.RejectReason != "leverage_conflict_use_set_leverage" {
		t.Fatalf("divergent leverage with active orders: %+v", pre)
	}
	if cfg := eng.SymbolOrderConfigOf(user1, perpSym); cfg.Leverage.Cmp(dec.New("10")) != 0 {
		t.Fatalf("precheck must not write leverage config: %s", cfg.Leverage)
	}
}
