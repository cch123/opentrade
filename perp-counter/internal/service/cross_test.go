package service

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/dec"
)

const (
	btc = "BTC-USDT-PERP"
	eth = "ETH-USDT-PERP"
)

func setCross(t *testing.T, svc *Service, user uint64, symbol, opID string) {
	t.Helper()
	out, err := svc.SetMarginMode(&perprpc.SetMarginModeRequest{
		UserId: user, Symbol: symbol, TargetMode: perprpc.MarginMode_MARGIN_MODE_CROSS, ClientOpId: opID})
	if err != nil || !out.Accepted {
		t.Fatalf("set cross: %+v err=%v", out, err)
	}
}

func TestSetMarginMode_Matrix(t *testing.T) {
	svc, eng, _, jr := newSvcWithTiers()
	eng.Deposit(user1, dec.New("100"))
	eng.SetMark(btc, dec.New("100"))

	// Flat config flip → cross.
	setCross(t, svc, user1, btc, "op-c1")

	// Live order blocks a switch back.
	resp, _ := svc.PlaceOrder(placeReq(user1, btc, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("place: %s", resp.RejectReason)
	}
	out, _ := svc.SetMarginMode(&perprpc.SetMarginModeRequest{
		UserId: user1, Symbol: btc, TargetMode: perprpc.MarginMode_MARGIN_MODE_ISOLATED, ClientOpId: "op-i1"})
	if out.Accepted || out.RejectReason != "active_orders_cancel_first" {
		t.Fatalf("want active-order reject: %+v", out)
	}

	// Fill the order → cross position, reservation fully released.
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "ct1", Symbol: btc, Price: "100", Qty: "1",
		TakerUserId: user1, TakerOrderId: resp.OrderId, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED, TakerFilledQtyAfter: "1",
	}, nextSeq())
	w := eng.WalletOf(user1)
	eqd(t, w.CrossReserved, "0", "cross reservation released on full fill")
	eqd(t, w.Available, "100", "free balance restored on cross fill")
	p, _ := eng.PositionOf(user1, btc)
	eqd(t, p.Margin, "0", "cross position margin")

	// Switch back demanding more isolated margin than the free balance.
	out, _ = svc.SetMarginMode(&perprpc.SetMarginModeRequest{
		UserId: user1, Symbol: btc, TargetMode: perprpc.MarginMode_MARGIN_MODE_ISOLATED,
		TargetMargin: "150", ClientOpId: "op-i2"})
	if out.Accepted || out.RejectReason != "insufficient_free_balance" {
		t.Fatalf("want insufficient reject: %+v", out)
	}

	// Proper switch back: margin = max(50, IM 10 + buffer 0) = 50.
	out, _ = svc.SetMarginMode(&perprpc.SetMarginModeRequest{
		UserId: user1, Symbol: btc, TargetMode: perprpc.MarginMode_MARGIN_MODE_ISOLATED,
		TargetMargin: "50", ClientOpId: "op-i3"})
	if !out.Accepted || out.MarginMode != perprpc.MarginMode_MARGIN_MODE_ISOLATED || out.PositionMargin != "50" {
		t.Fatalf("switch to isolated: %+v", out)
	}
	// Duplicate op replays the first outcome.
	dup, _ := svc.SetMarginMode(&perprpc.SetMarginModeRequest{
		UserId: user1, Symbol: btc, TargetMode: perprpc.MarginMode_MARGIN_MODE_ISOLATED,
		TargetMargin: "50", ClientOpId: "op-i3"})
	if !dup.Accepted || dup.PositionMargin != "50" {
		t.Fatalf("duplicate mode switch: %+v", dup)
	}
	if n := jr.count(func(e *eventpb.PerpJournalEvent) bool {
		m := e.GetMarginAdjustment()
		return m != nil && m.GetKind() == eventpb.PerpMarginAdjustmentEvent_KIND_MODE_SWITCH
	}); n != 1 { // only the cross→isolated leg moved cash (flat flip moved none)
		t.Fatalf("MODE_SWITCH adjustment events = %d, want 1", n)
	}
}

func TestCrossOrder_ProportionalReleaseAndCancel(t *testing.T) {
	svc, eng, _, _ := newSvcWithTiers()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark(btc, dec.New("100"))
	setCross(t, svc, user1, btc, "op-c")

	resp, _ := svc.PlaceOrder(placeReq(user1, btc, eventpb.Side_SIDE_BUY, "100", "2", "10", false))
	if !resp.Accepted {
		t.Fatalf("place: %s", resp.RejectReason)
	}
	w := eng.WalletOf(user1)
	eqd(t, w.CrossReserved, "20", "cross IM reserved")
	eqd(t, w.Available, "980", "free after reserve")

	// Half fill: half the reservation converts back to free cash.
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "cp1", Symbol: btc, Price: "100", Qty: "1",
		TakerUserId: user1, TakerOrderId: resp.OrderId, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED, TakerFilledQtyAfter: "1",
	}, nextSeq())
	w = eng.WalletOf(user1)
	eqd(t, w.CrossReserved, "10", "half reservation released")
	eqd(t, w.Available, "990", "free after half fill")

	// Cancel the rest: remainder released.
	svc.handleCancelled(&eventpb.OrderCancelled{UserId: user1, OrderId: resp.OrderId, Symbol: btc})
	w = eng.WalletOf(user1)
	eqd(t, w.CrossReserved, "0", "reservation cleared on cancel")
	eqd(t, w.Available, "1000", "free restored on cancel")
	p, _ := eng.PositionOf(user1, btc)
	eqd(t, p.Size, "1", "position size after partial fill")
	eqd(t, p.Margin, "0", "cross margin stays zero")
}

func TestCrossOrder_LeverageFromConfigAndAdmission(t *testing.T) {
	svc, eng, _, _ := newSvcWithTiers()
	eng.Deposit(user1, dec.New("150"))
	eng.SetMark(btc, dec.New("100"))
	setCross(t, svc, user1, btc, "op-c")

	// No config leverage yet → empty leverage rejects.
	resp, _ := svc.PlaceOrder(placeReq(user1, btc, eventpb.Side_SIDE_BUY, "100", "1", "", false))
	if resp.Accepted || resp.RejectReason != "leverage_required" {
		t.Fatalf("want leverage_required: %+v", resp)
	}
	if out, err := svc.SetPositionLeverage(&perprpc.SetPositionLeverageRequest{
		UserId: user1, Symbol: btc, Leverage: "10", ClientOpId: "op-l"}); err != nil || !out.Accepted {
		t.Fatalf("set leverage: %+v err=%v", out, err)
	}
	// Empty leverage now resolves from config; candidate pool must pass.
	resp, _ = svc.PlaceOrder(placeReq(user1, btc, eventpb.Side_SIDE_BUY, "100", "1", "", false))
	if !resp.Accepted {
		t.Fatalf("config-leverage order: %s", resp.RejectReason)
	}
	// Second big order exceeds the pool's headroom: candidate requirement =
	// 10 (held position) + 150 (this chunk) = 160 > equity 150.
	resp, _ = svc.PlaceOrder(placeReq(user1, btc, eventpb.Side_SIDE_BUY, "100", "15", "", false))
	if resp.Accepted || resp.RejectReason != "insufficient_margin" {
		t.Fatalf("want cross admission reject: %+v", resp)
	}
}

func TestCrossLiquidation_PartialPoolCloseStopsWhenHealthy(t *testing.T) {
	svc, eng, _, jr := newSvcWithTiers()
	eng.Deposit(user1, dec.New("250"))
	eng.SetMark(btc, dec.New("100"))
	eng.SetMark(eth, dec.New("100"))
	setCross(t, svc, user1, btc, "op-cb")
	setCross(t, svc, user1, eth, "op-ce")
	// BTC long 10 (loss leg), ETH short 2 (small healthy leg).
	r1, _ := svc.PlaceOrder(placeReq(user1, btc, eventpb.Side_SIDE_BUY, "100", "10", "10", false))
	svc.HandleTrade(&eventpb.Trade{TradeId: "cl1", Symbol: btc, Price: "100", Qty: "10",
		TakerUserId: user1, TakerOrderId: r1.OrderId, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED, TakerFilledQtyAfter: "10"}, nextSeq())
	r2, _ := svc.PlaceOrder(placeReq(user1, eth, eventpb.Side_SIDE_SELL, "100", "2", "10", false))
	svc.HandleTrade(&eventpb.Trade{TradeId: "cl2", Symbol: eth, Price: "100", Qty: "2",
		TakerUserId: user1, TakerOrderId: r2.OrderId, TakerSide: eventpb.Side_SIDE_SELL,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED, TakerFilledQtyAfter: "2"}, nextSeq())

	// Crash BTC: equity = 250 + (75.4-100)*10 = 4; MM = (754+200)*0.005 = 4.77
	// → pool liquidatable. Closing BTC alone restores health:
	// equity' = 250 - 246 - fee 0.754 = 3.246 > ETH MM 1.
	svc.HandlePerpPriceEvent(&eventpb.PerpPriceEvent{Symbol: btc,
		Payload: &eventpb.PerpPriceEvent_Tick{Tick: &eventpb.MarkTick{MarkPrice: "75.4"}}})

	if _, ok := eng.PositionOf(user1, btc); ok {
		t.Fatal("loss leg must be closed")
	}
	if p, ok := eng.PositionOf(user1, eth); !ok || p.Size.Cmp(dec.New("2")) != 0 {
		t.Fatal("healthy leg must survive")
	}
	if h, ok := eng.CrossPoolHealth(user1); !ok || h.Liquidatable() {
		t.Fatalf("pool must be healthy after partial close: %+v", h)
	}
	// One takeover journal (BTC), no deficit event.
	takeovers := 0
	for _, e := range jr.evts {
		if tk := e.GetTakeover(); tk != nil {
			takeovers++
			if tk.GetSymbol() != btc || tk.GetBackstopUserId() != backstopUser || tk.GetLotId() == "" {
				t.Fatalf("takeover event wrong: %+v", tk)
			}
		}
	}
	if takeovers != 1 {
		t.Fatalf("takeover events = %d, want 1", takeovers)
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool {
		l := e.GetLiquidation()
		return l != nil && l.GetClosedQty() == "0"
	}) != 0 {
		t.Fatal("no deficit event expected")
	}
	// Backstop carries the inventory.
	if bp, ok := eng.PositionOf(backstopUser, btc); !ok || bp.Size.Cmp(dec.New("10")) != 0 {
		t.Fatalf("backstop inventory: %+v ok=%v", bp, ok)
	}
}

func TestCrossLiquidation_BankruptcySettlesDeficit(t *testing.T) {
	svc, eng, _, jr := newSvcWithTiers()
	eng.Deposit(user1, dec.New("100"))
	eng.SetMark(btc, dec.New("100"))
	setCross(t, svc, user1, btc, "op-c")
	r1, _ := svc.PlaceOrder(placeReq(user1, btc, eventpb.Side_SIDE_BUY, "100", "10", "10", false))
	svc.HandleTrade(&eventpb.Trade{TradeId: "cb1", Symbol: btc, Price: "100", Qty: "10",
		TakerUserId: user1, TakerOrderId: r1.OrderId, TakerSide: eventpb.Side_SIDE_BUY,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED, TakerFilledQtyAfter: "10"}, nextSeq())

	insBefore := eng.InsuranceFund(btc)
	// equity = 100 + (85-100)*10 = -50 → bankrupt.
	svc.HandlePerpPriceEvent(&eventpb.PerpPriceEvent{Symbol: btc,
		Payload: &eventpb.PerpPriceEvent_Tick{Tick: &eventpb.MarkTick{MarkPrice: "85"}}})

	w := eng.WalletOf(user1)
	eqd(t, w.Available, "0", "bankrupt wallet zeroed")
	if _, ok := eng.PositionOf(user1, btc); ok {
		t.Fatal("position must be closed")
	}
	// fee 0.85 in, deficit 50.85 out → net -50.
	if got := eng.InsuranceFund(btc).Sub(insBefore); got.Cmp(dec.New("-50")) != 0 {
		t.Fatalf("insurance net delta = %s, want -50", got)
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool {
		l := e.GetLiquidation()
		return l != nil && l.GetClosedQty() == "0" && l.GetInsuranceDelta() == "-50.85"
	}) != 1 {
		t.Fatal("expected one deficit settlement event")
	}
}
