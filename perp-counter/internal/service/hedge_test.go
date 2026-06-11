package service

// ADR-0077 hedge-mode service tests: the PlaceOrder intent matrix, the
// SetPositionMode rejection ladder (orders / triggers / non-flat), dual-leg
// settlement routing, per-leg reduce_only, the never-flip breach emission,
// per-leg funding journals, and per-leg AdjustIsolatedMargin validation.

import (
	"errors"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func hedgeReq(user uint64, side eventpb.Side, idx uint32, price, qty string, reduceOnly bool) *perprpc.PlaceOrderRequest {
	r := placeReq(user, "BTC-USDT-PERP", side, price, qty, "10", reduceOnly)
	r.PositionIdx = idx
	return r
}

func switchHedge(t *testing.T, svc *Service, user uint64) {
	t.Helper()
	resp, err := svc.SetPositionMode(&perprpc.SetPositionModeRequest{
		UserId: user, Symbol: "BTC-USDT-PERP",
		TargetMode: perprpc.PositionMode_POSITION_MODE_HEDGE,
	})
	if err != nil || !resp.Accepted {
		t.Fatalf("switch to hedge: err=%v resp=%+v", err, resp)
	}
}

// fill delivers a Match trade for one resting order against an anonymous
// counterparty (user 0 leg is skipped by HandleTrade).
func fillOrder(svc *Service, user, orderID uint64, side eventpb.Side, price, qty string, seq uint64) {
	tr := &eventpb.Trade{
		TradeId: "t", Symbol: "BTC-USDT-PERP", Price: price, Qty: qty,
		TakerUserId: user, TakerOrderId: orderID, TakerSide: side,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		TakerFilledQtyAfter: qty,
	}
	svc.HandleTrade(tr, seq)
}

func TestHedge_PlaceOrderIntentMatrix(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))

	// ONE_WAY rejects a leg idx.
	resp, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 1, "100", "1", false))
	if resp.Accepted || resp.RejectReason != "position_idx_requires_hedge_mode" {
		t.Fatalf("one-way + idx1: %+v", resp)
	}

	switchHedge(t, svc, user1)

	// HEDGE rejects idx 0.
	resp, _ = svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 0, "100", "1", false))
	if resp.Accepted || resp.RejectReason != "position_idx_required_in_hedge_mode" {
		t.Fatalf("hedge + idx0: %+v", resp)
	}
	// Contradiction quadrants.
	resp, _ = svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 1, "100", "1", true))
	if resp.Accepted || resp.RejectReason != "position_intent_mismatch" {
		t.Fatalf("buy idx1 reduce_only: %+v", resp)
	}
	resp, _ = svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_SELL, 1, "100", "1", false))
	if resp.Accepted || resp.RejectReason != "position_intent_mismatch" {
		t.Fatalf("sell idx1 open: %+v", resp)
	}
	// Valid opens pass.
	resp, _ = svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 1, "100", "1", false))
	if !resp.Accepted {
		t.Fatalf("open long leg: %+v", resp)
	}
	resp, _ = svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_SELL, 2, "100", "1", false))
	if !resp.Accepted {
		t.Fatalf("open short leg: %+v", resp)
	}
	// Out-of-range idx is an invalid argument, not a business reject.
	if _, err := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 3, "100", "1", false)); err == nil {
		t.Fatal("idx 3 must error")
	}
}

type stubTriggers struct {
	active bool
	err    error
}

func (s stubTriggers) HasActiveTriggers(uint64, string) (bool, error) { return s.active, s.err }

func TestHedge_SetPositionModeRejectionLadder(t *testing.T) {
	// Active order blocks the switch.
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	if r, _ := svc.PlaceOrder(placeReq(user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false)); !r.Accepted {
		t.Fatalf("setup order: %+v", r)
	}
	resp, _ := svc.SetPositionMode(&perprpc.SetPositionModeRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", TargetMode: perprpc.PositionMode_POSITION_MODE_HEDGE,
	})
	if resp.Accepted || resp.RejectReason != "active_orders_cancel_first" {
		t.Fatalf("want order reject: %+v", resp)
	}

	// Active position-bound trigger blocks the switch (TriggerChecker seam).
	eng2 := engine.New()
	var id uint64
	svc2 := New(eng2, nil, nil, func() uint64 { id++; return id },
		Config{MaxLeverage: dec.New("100"), Triggers: stubTriggers{active: true}})
	resp, _ = svc2.SetPositionMode(&perprpc.SetPositionModeRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", TargetMode: perprpc.PositionMode_POSITION_MODE_HEDGE,
	})
	if resp.Accepted || resp.RejectReason != "active_triggers_cancel_first" {
		t.Fatalf("want trigger reject: %+v", resp)
	}

	// An unanswerable trigger query fails CLOSED (ADR-0078 §6): the mode
	// switch rejects with its own reason instead of assuming "no triggers".
	eng2b := engine.New()
	var id2 uint64
	svc2b := New(eng2b, nil, nil, func() uint64 { id2++; return id2 },
		Config{MaxLeverage: dec.New("100"), Triggers: stubTriggers{err: errors.New("rpc unavailable")}})
	resp, _ = svc2b.SetPositionMode(&perprpc.SetPositionModeRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", TargetMode: perprpc.PositionMode_POSITION_MODE_HEDGE,
	})
	if resp.Accepted || resp.RejectReason != "active_triggers_check_unavailable" {
		t.Fatalf("want fail-closed reject: %+v", resp)
	}

	// Non-flat position blocks (engine re-check).
	svc3, eng3, _, _ := newSvc()
	eng3.Deposit(user1, dec.New("1000"))
	eng3.Reserve(user1, dec.New("10"))
	eng3.ApplyFill(user1, "BTC-USDT-PERP", 0, dec.New("10"),
		perpstate.Fill{Side: perpstate.SideBuy, Price: dec.New("100"), Qty: dec.New("1")})
	resp, _ = svc3.SetPositionMode(&perprpc.SetPositionModeRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", TargetMode: perprpc.PositionMode_POSITION_MODE_HEDGE,
	})
	if resp.Accepted || resp.RejectReason != "position_not_flat" {
		t.Fatalf("want non-flat reject: %+v", resp)
	}

	// Clean account switches and journals the config event.
	svc4, _, _, jr4 := newSvc()
	resp, _ = svc4.SetPositionMode(&perprpc.SetPositionModeRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", TargetMode: perprpc.PositionMode_POSITION_MODE_HEDGE,
	})
	if !resp.Accepted || resp.PositionMode != perprpc.PositionMode_POSITION_MODE_HEDGE {
		t.Fatalf("clean switch: %+v", resp)
	}
	n := jr4.count(func(e *eventpb.PerpJournalEvent) bool {
		pc := e.GetPositionConfig()
		return pc != nil && pc.GetReason() == "set_position_mode" &&
			pc.GetPositionMode() == eventpb.PerpPositionMode_PERP_POSITION_MODE_HEDGE
	})
	if n != 1 {
		t.Fatalf("want one set_position_mode config event, got %d", n)
	}
}

func TestHedge_DualLegTradeFlowAndReduceOnly(t *testing.T) {
	svc, eng, _, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	switchHedge(t, svc, user1)

	// Open both legs.
	rL, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 1, "100", "2", false))
	fillOrder(svc, user1, rL.OrderId, eventpb.Side_SIDE_BUY, "100", "2", 1)
	rS, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_SELL, 2, "100", "1", false))
	fillOrder(svc, user1, rS.OrderId, eventpb.Side_SIDE_SELL, "100", "1", 2)

	long, okL := eng.PositionOf(user1, "BTC-USDT-PERP", perpstate.IdxLong)
	short, okS := eng.PositionOf(user1, "BTC-USDT-PERP", perpstate.IdxShort)
	if !okL || !okS {
		t.Fatalf("both legs should exist: %v %v", okL, okS)
	}
	eqd(t, long.Size, "2", "long size")
	eqd(t, short.Size, "1", "short size")

	// reduce_only close on the long leg reduces ONLY the long leg.
	rC, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_SELL, 1, "110", "1", true))
	if !rC.Accepted {
		t.Fatalf("close long: %+v", rC)
	}
	fillOrder(svc, user1, rC.OrderId, eventpb.Side_SIDE_SELL, "110", "1", 3)

	long, _ = eng.PositionOf(user1, "BTC-USDT-PERP", perpstate.IdxLong)
	short, _ = eng.PositionOf(user1, "BTC-USDT-PERP", perpstate.IdxShort)
	eqd(t, long.Size, "1", "long reduced")
	eqd(t, short.Size, "1", "short untouched")

	// Settlement snapshots carry the leg idx.
	nLong := jr.count(func(e *eventpb.PerpJournalEvent) bool {
		s := e.GetSettlement()
		return s != nil && s.GetPositionAfter().GetPositionIdx() == uint32(perpstate.IdxLong)
	})
	if nLong != 2 { // open + close
		t.Fatalf("want 2 long-leg settlements, got %d", nLong)
	}
}

// The never-flip breach: a hedge close fill whose leg shrank in between is
// clamped and the excess journals a REDUCE_ONLY_INVARIANT_BREACH.
func TestHedge_ReduceOnlyExcessEmitsBreach(t *testing.T) {
	svc, eng, _, jr := newSvc()
	eng.Deposit(user1, dec.New("1000"))
	switchHedge(t, svc, user1)

	rL, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 1, "100", "1", false))
	fillOrder(svc, user1, rL.OrderId, eventpb.Side_SIDE_BUY, "100", "1", 1)

	// Admission passes while the leg is live...
	rC, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_SELL, 1, "90", "1", true))
	if !rC.Accepted {
		t.Fatalf("close admission: %+v", rC)
	}
	// ...then the leg is closed by another path before the fill returns
	// (TOCTOU residue) — here via a direct engine close.
	eng.ApplyFill(user1, "BTC-USDT-PERP", perpstate.IdxLong, dec.New("10"),
		perpstate.Fill{Side: perpstate.SideSell, Price: dec.New("100"), Qty: dec.New("1")})

	fillOrder(svc, user1, rC.OrderId, eventpb.Side_SIDE_SELL, "90", "1", 9)

	// The leg must NOT have flipped short.
	if p, ok := eng.PositionOf(user1, "BTC-USDT-PERP", perpstate.IdxLong); ok {
		t.Fatalf("leg must stay flat, got %+v", p)
	}
	breaches := 0
	for _, e := range jr.evts {
		if b := e.GetInvariantBreach(); b != nil {
			breaches++
			if b.GetKind() != "reduce_only_excess" || b.GetExcessQty() != "1" ||
				b.GetPositionIdx() != uint32(perpstate.IdxLong) {
				t.Fatalf("breach payload wrong: %+v", b)
			}
		}
	}
	if breaches != 1 {
		t.Fatalf("want exactly one breach event, got %d", breaches)
	}
}

func TestHedge_FundingJournalPerLeg(t *testing.T) {
	svc, eng, _, jr := newSvc()
	eng.Deposit(user1, dec.New("10000"))
	switchHedge(t, svc, user1)

	rL, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 1, "100", "10", false))
	fillOrder(svc, user1, rL.OrderId, eventpb.Side_SIDE_BUY, "100", "10", 1)
	rS, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_SELL, 2, "100", "8", false))
	fillOrder(svc, user1, rS.OrderId, eventpb.Side_SIDE_SELL, "100", "8", 2)

	eng.SetMark("BTC-USDT-PERP", dec.New("100"))
	svc.HandlePerpPriceEvent(&eventpb.PerpPriceEvent{
		Symbol: "BTC-USDT-PERP",
		Payload: &eventpb.PerpPriceEvent_Funding{Funding: &eventpb.FundingTick{
			FundingRate: "0.001", FundingRoundId: "BTC-USDT-PERP:1700000000",
		}},
	})

	var legs []uint32
	var payments []string
	for _, e := range jr.evts {
		if f := e.GetFunding(); f != nil {
			legs = append(legs, f.GetPositionAfter().GetPositionIdx())
			payments = append(payments, f.GetPayment())
		}
	}
	if len(legs) != 2 || legs[0] != uint32(perpstate.IdxLong) || legs[1] != uint32(perpstate.IdxShort) {
		t.Fatalf("want one funding event per leg in idx order, got %v", legs)
	}
	if payments[0] != "-1" || payments[1] != "0.8" {
		t.Fatalf("per-leg payments wrong (no netting): %v", payments)
	}
}

func TestHedge_AdjustIsolatedMarginLegValidation(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit(user1, dec.New("1000"))

	// ONE_WAY + idx 1 → fail-closed.
	resp, _ := svc.AdjustIsolatedMargin(&perprpc.AdjustIsolatedMarginRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Delta: "5", PositionIdx: 1,
	})
	if resp.Accepted || resp.RejectReason != "position_idx_requires_hedge_mode" {
		t.Fatalf("one-way idx1 adjust: %+v", resp)
	}

	switchHedge(t, svc, user1)
	// HEDGE + idx 0 → fail-closed.
	resp, _ = svc.AdjustIsolatedMargin(&perprpc.AdjustIsolatedMarginRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Delta: "5", PositionIdx: 0,
	})
	if resp.Accepted || resp.RejectReason != "position_idx_required_in_hedge_mode" {
		t.Fatalf("hedge idx0 adjust: %+v", resp)
	}

	// Live long leg adds margin on idx 1 only.
	rL, _ := svc.PlaceOrder(hedgeReq(user1, eventpb.Side_SIDE_BUY, 1, "100", "1", false))
	fillOrder(svc, user1, rL.OrderId, eventpb.Side_SIDE_BUY, "100", "1", 1)
	resp, _ = svc.AdjustIsolatedMargin(&perprpc.AdjustIsolatedMarginRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Delta: "5", PositionIdx: 1,
	})
	if !resp.Accepted {
		t.Fatalf("leg adjust: %+v", resp)
	}
	p, _ := eng.PositionRaw(user1, "BTC-USDT-PERP", perpstate.IdxLong)
	eqd(t, p.Margin, "15", "long leg margin topped up")
}
