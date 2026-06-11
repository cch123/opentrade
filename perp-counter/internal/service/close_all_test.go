package service

// close_all_test.go covers the ADR-0078 §5 conservative two-phase close-all:
// the cancel→quiesce→place pipeline, the scope guard, in-flight fills landing
// in the sizing, the liquidation-overlap skip, idempotency, and the snapshot
// roundtrip of a mid-run state.

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func closeAllReq(user uint64, symbol, opID string) *perprpc.CloseAllPositionsRequest {
	return &perprpc.CloseAllPositionsRequest{UserId: user, Symbol: symbol, SlippageBps: 100, ClientOpId: opID}
}

func TestCloseAll_TwoPhaseHappyPath(t *testing.T) {
	svc, eng, disp, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10") // long 1
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "90", "1", "10", false))
	if !r.Accepted {
		t.Fatal("setup resting order failed")
	}

	resp, err := svc.CloseAllPositions(closeAllReq(user1, "", "ca-1"))
	if err != nil || !resp.Accepted {
		t.Fatalf("close-all: err=%v resp=%+v", err, resp)
	}
	if resp.Phase != perprpc.CloseAllPhase_CLOSE_ALL_PHASE_CANCELING {
		t.Fatalf("phase: %v", resp.Phase)
	}
	if disp.cancels != 1 {
		t.Fatalf("resting order cancel dispatched, got %d", disp.cancels)
	}
	// Scope guard: new orders rejected while the run owns the scope.
	blocked, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_SELL, "110", "1", "10", false))
	if blocked.Accepted || blocked.RejectReason != "close_all_in_progress" {
		t.Fatalf("scope guard: %+v", blocked)
	}

	// Resting order goes terminal → PLACING: one reduce-only protected
	// market IOC sized at the leg.
	preOrders := len(disp.orders)
	svc.HandleTradeEvent(cancelledEvt(0, user1, r.OrderId), 0, 1)
	if len(disp.orders) != preOrders+1 {
		t.Fatal("close order must dispatch after quiesce")
	}
	placed := disp.orders[len(disp.orders)-1].GetPlaced()
	if placed.GetQty() != "1" || placed.GetOrderType() != eventpb.OrderType_ORDER_TYPE_MARKET || placed.GetSlippageBps() != 100 {
		t.Fatalf("close order wire: %+v", placed)
	}
	st, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-1")) // idempotent re-read
	if st.Phase != perprpc.CloseAllPhase_CLOSE_ALL_PHASE_PLACING || len(st.Legs) != 1 {
		t.Fatalf("placing state: %+v", st)
	}

	// Close order fills → DONE, flat, guard lifted.
	fillOrder(svc, user1, placed.GetOrderId(), eventpb.Side_SIDE_SELL, "100", "1", nextSeq())
	if _, ok := eng.PositionOf(user1, perpSym, perpstate.IdxNet); ok {
		t.Fatal("position must be flat after close-all")
	}
	final, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-1"))
	if final.Phase != perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE {
		t.Fatalf("final phase: %v", final.Phase)
	}
	again, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "90", "1", "10", false))
	if !again.Accepted {
		t.Fatalf("guard must lift after DONE: %+v", again)
	}
	// Journal: REQUESTED → PLACING → DONE.
	var states []eventpb.PerpCloseAllEvent_State
	for _, e := range jr.evts {
		if ca := e.GetCloseAll(); ca != nil {
			states = append(states, ca.GetState())
		}
	}
	want := []eventpb.PerpCloseAllEvent_State{
		eventpb.PerpCloseAllEvent_STATE_REQUESTED,
		eventpb.PerpCloseAllEvent_STATE_PLACING,
		eventpb.PerpCloseAllEvent_STATE_DONE,
	}
	if len(states) != len(want) {
		t.Fatalf("close-all journal states: %v", states)
	}
	for i := range want {
		if states[i] != want[i] {
			t.Fatalf("close-all journal states: %v", states)
		}
	}
}

func TestCloseAll_InFlightFillIncludedInSizing(t *testing.T) {
	svc, eng, disp, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10") // long 1
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))

	resp, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-2"))
	if !resp.Accepted {
		t.Fatalf("close-all rejected: %s", resp.RejectReason)
	}
	// The resting buy fills IN FULL while its cancel is in flight — the
	// position grows to 2 and the fill's terminal drives the run forward.
	fillOrder(svc, user1, r.OrderId, eventpb.Side_SIDE_BUY, "100", "1", nextSeq())
	placed := disp.orders[len(disp.orders)-1].GetPlaced()
	if placed.GetQty() != "2" {
		t.Fatalf("close order must be sized AFTER the in-flight fill (2), got %s", placed.GetQty())
	}
}

func TestCloseAll_LiquidationOverlapSkipsLeg(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10")
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "90", "1", "10", false))

	resp, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-3"))
	if !resp.Accepted {
		t.Fatalf("close-all rejected: %s", resp.RejectReason)
	}
	// A liquidation arms on the leg DURING the canceling phase (mark-tick
	// scan path) — close-all must yield the leg to the bankruptcy order.
	svc.registerLiquidation(liqKey(user1, perpSym, perpstate.IdxNet), 777777,
		&liquidation{userID: user1, symbol: perpSym, positionIdx: perpstate.IdxNet, orderID: 777777})
	svc.HandleTradeEvent(cancelledEvt(0, user1, r.OrderId), 0, 1)

	final, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-3"))
	if final.Phase != perprpc.CloseAllPhase_CLOSE_ALL_PHASE_DONE_WITH_ERRORS {
		t.Fatalf("want DONE_WITH_ERRORS, got %v", final.Phase)
	}
	if len(final.Legs) != 1 || final.Legs[0].RejectReason != "liquidation_in_flight" {
		t.Fatalf("leg outcome: %+v", final.Legs)
	}
}

func TestCloseAll_Guards(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))

	if _, err := svc.CloseAllPositions(&perprpc.CloseAllPositionsRequest{UserId: user1, ClientOpId: "x"}); err == nil {
		t.Fatal("slippage_bps required")
	}
	if resp, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-empty")); resp.Accepted || resp.RejectReason != "no_scope" {
		t.Fatalf("empty scope: %+v", resp)
	}

	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10")
	svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "90", "1", "10", false))
	first, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-a"))
	if !first.Accepted {
		t.Fatalf("first run: %+v", first)
	}
	second, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-b"))
	if second.Accepted || second.RejectReason != "close_all_in_progress" {
		t.Fatalf("second concurrent run must reject: %+v", second)
	}
	// Amend during close-all is rejected too.
	if am, _ := svc.AmendOrder(amendReq(user1, 1, "90", "2")); am.RejectReason != "not_found" && am.RejectReason != "close_all_in_progress" {
		t.Fatalf("amend during close-all: %+v", am)
	}
}

func TestCloseAll_LiquidationAtRequestRejects(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10")
	svc.registerLiquidation(liqKey(user1, perpSym, perpstate.IdxNet), 777778,
		&liquidation{userID: user1, symbol: perpSym, positionIdx: perpstate.IdxNet, orderID: 777778})
	resp, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-liq"))
	if resp.Accepted || resp.RejectReason != "liquidation_in_flight" {
		t.Fatalf("want liquidation_in_flight: %+v", resp)
	}
}

func TestCloseAll_AbortsPendingAmendsInScope(t *testing.T) {
	svc, eng, disp, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10")
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "90", "1", "10", false))
	am, _ := svc.AmendOrder(amendReq(user1, r.OrderId, "85", "2"))
	if !am.Accepted {
		t.Fatalf("amend setup: %+v", am)
	}
	resp, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-amend"))
	if !resp.Accepted {
		t.Fatalf("close-all rejected: %s", resp.RejectReason)
	}
	preOrders := len(disp.orders)
	svc.HandleTradeEvent(cancelledEvt(0, user1, r.OrderId), 0, 1)
	// Exactly ONE placement after terminal: the close order — never the
	// amend replacement (it was aborted by the close-all's cancel pass).
	if len(disp.orders) != preOrders+1 {
		t.Fatalf("placements after terminal: %d", len(disp.orders)-preOrders)
	}
	states := amendStates(jr)
	if states[len(states)-1] != eventpb.PerpAmendEvent_STATE_ABORTED_BY_CANCEL {
		t.Fatalf("amend must be aborted by the close-all cancel: %v", states)
	}
}

func TestCloseAll_SnapshotRoundtrip_MidRun(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(perpSym, dec.New("100"))
	fillOpen(svc, eng, user1, perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10")
	r, _ := svc.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_BUY, "90", "1", "10", false))
	resp, _ := svc.CloseAllPositions(closeAllReq(user1, "", "ca-snap"))
	if !resp.Accepted {
		t.Fatalf("close-all rejected: %s", resp.RejectReason)
	}

	engSnap, snap, err := svc.Capture(nil)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := engine.New()
	eng2.Restore(engSnap)
	disp2 := &fakeDispatcher{}
	var id uint64 = 9000
	svc2 := New(eng2, disp2, &fakeJournal{}, func() uint64 { id++; return id },
		Config{MaxLeverage: dec.New("100"), ProducerID: "perp-shard-0"})
	svc2.Restore(snap)

	// Guard survives the restart.
	blocked, _ := svc2.PlaceOrder(placeReq(user1, perpSym, eventpb.Side_SIDE_SELL, "110", "1", "10", false))
	if blocked.Accepted || blocked.RejectReason != "close_all_in_progress" {
		t.Fatalf("guard after restore: %+v", blocked)
	}
	// Replay of the terminal drives PLACING with the PRE-ALLOCATED leg id.
	svc2.HandleTradeEvent(cancelledEvt(0, user1, r.OrderId), 0, 1)
	if len(disp2.orders) != 1 {
		t.Fatal("close order must dispatch after replayed quiesce")
	}
	got := disp2.orders[0].GetPlaced().GetOrderId()
	st, _ := svc2.CloseAllPositions(closeAllReq(user1, "", "ca-snap"))
	if len(st.Legs) != 1 || st.Legs[0].OrderId != got {
		t.Fatalf("leg id mismatch: %+v vs %d", st.Legs, got)
	}
	if got >= 9000 {
		t.Fatalf("close order id must be the pre-allocated (pre-restart) id, got fresh %d", got)
	}
}
