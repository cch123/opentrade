package service

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// newSvcWithTiers builds a service whose risk model has one open-ended tier
// (MMR 0.5%, 100x) so liquidation and tier-cap paths are active.
func newSvcWithTiers() (*Service, *engine.Engine, *fakeDispatcher, *fakeJournal) {
	eng := engine.New()
	disp := &fakeDispatcher{}
	jr := &fakeJournal{}
	var id uint64
	svc := New(eng, disp, jr, func() uint64 { id++; return id },
		Config{
			MaxLeverage: dec.New("100"), MMR: dec.New("0.005"), ProducerID: "perp-shard-0",
			RiskTiers: []perpstate.RiskTier{
				{TierMaxNotional: dec.New("10000"), MaintMarginRatio: dec.New("0.005"), MaxLeverage: dec.New("100"), LiqFeeRate: dec.New("0.001")},
				{TierMaxNotional: dec.New("0"), MaintMarginRatio: dec.New("0.02"), MaxLeverage: dec.New("20"), LiqFeeRate: dec.New("0.002")},
			},
			BackstopAccount: backstopUser,
		})
	return svc, eng, disp, jr
}

// fillOpen drives a position open through the real trade path.
func fillOpen(svc *Service, eng *engine.Engine, user uint64, sym string, side eventpb.Side, price, qty, lev string) uint64 {
	resp, err := svc.PlaceOrder(placeReq(user, sym, side, price, qty, lev, false))
	if err != nil || !resp.Accepted {
		panic("fillOpen place failed: " + resp.GetRejectReason())
	}
	takerSide := eventpb.Side_SIDE_SELL
	if side == eventpb.Side_SIDE_BUY {
		takerSide = eventpb.Side_SIDE_BUY
	}
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t-" + price + "-" + qty, Symbol: sym, Price: price, Qty: qty,
		TakerUserId: user, TakerOrderId: resp.OrderId, TakerSide: takerSide,
		TakerStatusAfter: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED, TakerFilledQtyAfter: qty,
	}, nextSeq())
	return resp.OrderId
}

var testSeq uint64 = 100

func nextSeq() uint64 { testSeq++; return testSeq }

func TestSetPositionLeverage_RejectsWithActiveOrders(t *testing.T) {
	svc, eng, _, _ := newSvcWithTiers()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark("BTC-USDT-PERP", dec.New("100"))
	// Resting order (no fill yet).
	resp, _ := svc.PlaceOrder(placeReq(user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if !resp.Accepted {
		t.Fatalf("place: %s", resp.RejectReason)
	}
	out, err := svc.SetPositionLeverage(&perprpc.SetPositionLeverageRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Leverage: "5", ClientOpId: "op1"})
	if err != nil || out.Accepted || out.RejectReason != "active_orders_cancel_first" {
		t.Fatalf("want active-orders reject, got %+v err=%v", out, err)
	}
}

func TestSetPositionLeverage_ResizesAndJournals(t *testing.T) {
	svc, eng, _, jr := newSvcWithTiers()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark("BTC-USDT-PERP", dec.New("100"))
	fillOpen(svc, eng, user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10") // margin 10

	out, err := svc.SetPositionLeverage(&perprpc.SetPositionLeverageRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Leverage: "5", ClientOpId: "op-lev"})
	if err != nil || !out.Accepted {
		t.Fatalf("set leverage: %+v err=%v", out, err)
	}
	if out.PositionMargin != "20" {
		t.Fatalf("margin after = %s, want 20", out.PositionMargin)
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool {
		return e.GetPositionConfig().GetReason() == "set_leverage"
	}) != 1 {
		t.Fatal("expected a set_leverage position-config journal event")
	}
	adj := 0
	for _, e := range jr.evts {
		if m := e.GetMarginAdjustment(); m != nil {
			adj++
			if m.GetKind() != eventpb.PerpMarginAdjustmentEvent_KIND_LEVERAGE_RESIZE ||
				m.GetAmount() != "10" || m.GetMarginBefore() != "10" || m.GetMarginAfter() != "20" {
				t.Fatalf("margin adjustment event wrong: %+v", m)
			}
		}
	}
	if adj != 1 {
		t.Fatalf("expected one margin-adjustment event, got %d", adj)
	}

	// Same client_op_id replays the first outcome without re-applying.
	dup, _ := svc.SetPositionLeverage(&perprpc.SetPositionLeverageRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Leverage: "5", ClientOpId: "op-lev"})
	if !dup.Accepted || dup.PositionMargin != "20" {
		t.Fatalf("duplicate must replay first outcome: %+v", dup)
	}
}

func TestAdjustIsolatedMargin_JournalsBothDirections(t *testing.T) {
	svc, eng, _, jr := newSvcWithTiers()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark("BTC-USDT-PERP", dec.New("100"))
	fillOpen(svc, eng, user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10")

	add, err := svc.AdjustIsolatedMargin(&perprpc.AdjustIsolatedMarginRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Delta: "40", ClientOpId: "op-add"})
	if err != nil || !add.Accepted || add.PositionMargin != "50" {
		t.Fatalf("add: %+v err=%v", add, err)
	}
	rm, err := svc.AdjustIsolatedMargin(&perprpc.AdjustIsolatedMarginRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Delta: "-40", ClientOpId: "op-rm"})
	if err != nil || !rm.Accepted || rm.PositionMargin != "10" {
		t.Fatalf("remove: %+v err=%v", rm, err)
	}
	kinds := map[eventpb.PerpMarginAdjustmentEvent_Kind]int{}
	for _, e := range jr.evts {
		if m := e.GetMarginAdjustment(); m != nil {
			kinds[m.GetKind()]++
		}
	}
	if kinds[eventpb.PerpMarginAdjustmentEvent_KIND_ADD_ISOLATED] != 1 ||
		kinds[eventpb.PerpMarginAdjustmentEvent_KIND_REMOVE_ISOLATED] != 1 {
		t.Fatalf("adjustment kinds: %v", kinds)
	}
}

func TestSetRiskID_CountsActiveOrderNotional(t *testing.T) {
	svc, eng, _, _ := newSvcWithTiers()
	eng.Deposit(user1, dec.New("10000"))
	eng.SetMark("BTC-USDT-PERP", dec.New("100"))
	fillOpen(svc, eng, user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10") // notional 100
	// Resting order with remaining notional 9950 → total 10050 > tier1 cap 10000.
	resp, _ := svc.PlaceOrder(placeReq(user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "99.5", "10", false))
	if !resp.Accepted {
		t.Fatalf("place: %s", resp.RejectReason)
	}
	out, _ := svc.SetRiskId(&perprpc.SetRiskIdRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", RiskId: 1, ClientOpId: "op-r1"})
	if out.Accepted || out.RejectReason != "notional_exceeds_tier_cap" {
		t.Fatalf("want tier-cap reject incl. order notional, got %+v", out)
	}
	// Tier 2 (open-ended) covers it.
	out, _ = svc.SetRiskId(&perprpc.SetRiskIdRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", RiskId: 2, ClientOpId: "op-r2"})
	if !out.Accepted || out.RiskId != 2 {
		t.Fatalf("set riskID 2: %+v", out)
	}
}

func TestAutoAdd_RunsBeforeLiquidationArm(t *testing.T) {
	svc, eng, disp, jr := newSvcWithTiers()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark("BTC-USDT-PERP", dec.New("100"))
	fillOpen(svc, eng, user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10") // margin 10, bankruptcy 90
	if out, err := svc.SetAutoAddMargin(&perprpc.SetAutoAddMarginRequest{
		UserId: user1, Symbol: "BTC-USDT-PERP", Enabled: true, ClientOpId: "op-aa"}); err != nil || !out.Accepted {
		t.Fatalf("enable auto add: %+v err=%v", out, err)
	}
	ordersBefore := len(disp.orders)

	// Mark 90.2: equity 0.2, ratio ~0.0022 ≤ MMR 0.005 → liq-price crossed.
	// Auto-add must top up BEFORE the scan arms a liquidation: target
	// 0.005+0.01 → margin tops to ~1.3635+10... need = 0.015*90.2 - 0.2.
	svc.HandlePerpPriceEvent(&eventpb.PerpPriceEvent{
		Symbol:  "BTC-USDT-PERP",
		Payload: &eventpb.PerpPriceEvent_Tick{Tick: &eventpb.MarkTick{MarkPrice: "90.2"}},
	})

	if n := jr.count(func(e *eventpb.PerpJournalEvent) bool {
		m := e.GetMarginAdjustment()
		return m != nil && m.GetKind() == eventpb.PerpMarginAdjustmentEvent_KIND_AUTO_ADD
	}); n != 1 {
		t.Fatalf("expected one AUTO_ADD journal event, got %d", n)
	}
	if len(disp.orders) != ordersBefore {
		t.Fatal("liquidation order dispatched despite successful auto-add")
	}
	p, _ := eng.PositionOf(user1, "BTC-USDT-PERP", 0)
	if p.Margin.Cmp(dec.New("10")) <= 0 {
		t.Fatalf("margin not topped up: %s", p.Margin)
	}

	// Drain the wallet and crash the mark: auto-add can't fire (no cash) and
	// the scan must arm a liquidation this time.
	avail := eng.WalletOf(user1).Available
	if !eng.Withdraw(user1, avail) {
		t.Fatal("drain wallet")
	}
	svc.HandlePerpPriceEvent(&eventpb.PerpPriceEvent{
		Symbol:  "BTC-USDT-PERP",
		Payload: &eventpb.PerpPriceEvent_Tick{Tick: &eventpb.MarkTick{MarkPrice: "89"}},
	})
	if len(disp.orders) <= ordersBefore {
		t.Fatal("liquidation must arm once auto-add has no cash")
	}
}

func TestCustomerLeverageLimit_GatesPlaceOrder(t *testing.T) {
	svc, eng, _, jr := newSvcWithTiers()
	eng.Deposit(user1, dec.New("10000"))
	eng.SetMark("BTC-USDT-PERP", dec.New("100"))
	if _, err := svc.SetCustomerLeverageLimit(&perprpc.SetCustomerLeverageLimitRequest{
		UserId: user1, MaxLeverage: "5", Reason: "risk desk", UpdatedBy: "admin"}); err != nil {
		t.Fatal(err)
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetCustomerRiskLimit() != nil }) != 1 {
		t.Fatal("expected a customer-risk-limit journal event")
	}
	resp, _ := svc.PlaceOrder(placeReq(user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if resp.Accepted || resp.RejectReason != "leverage_exceeds_max" {
		t.Fatalf("customer cap must gate admission, got %+v", resp)
	}
	resp, _ = svc.PlaceOrder(placeReq(user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "5", false))
	if !resp.Accepted {
		t.Fatalf("5x within cap should pass: %s", resp.RejectReason)
	}
	lst, _ := svc.ListCustomerLeverageLimits(&perprpc.ListCustomerLeverageLimitsRequest{UserId: user1})
	if len(lst.Limits) != 1 || lst.Limits[0].MaxLeverage != "5" {
		t.Fatalf("list limits: %+v", lst.Limits)
	}
}

func TestQueryPositionAndAccountConfig(t *testing.T) {
	svc, eng, _, _ := newSvcWithTiers()
	eng.Deposit(user1, dec.New("1000"))
	eng.SetMark("BTC-USDT-PERP", dec.New("100"))
	fillOpen(svc, eng, user1, "BTC-USDT-PERP", eventpb.Side_SIDE_BUY, "100", "1", "10")
	svc.SetRiskId(&perprpc.SetRiskIdRequest{UserId: user1, Symbol: "BTC-USDT-PERP", RiskId: 2, ClientOpId: "op-q"})

	pc, err := svc.QueryPositionConfig(&perprpc.QueryPositionConfigRequest{UserId: user1, Symbol: "BTC-USDT-PERP"})
	if err != nil || len(pc.Configs) != 1 {
		t.Fatalf("query config: %+v err=%v", pc, err)
	}
	c := pc.Configs[0]
	if c.RiskId != 2 || c.MarginMode != perprpc.MarginMode_MARGIN_MODE_ISOLATED || c.Leverage != "10" {
		t.Fatalf("config view: %+v", c)
	}
	// riskID 2 (open-ended, 20x) → effective max leverage 20.
	if c.EffectiveMaxLeverage != "20" {
		t.Fatalf("effective max leverage = %s, want 20", c.EffectiveMaxLeverage)
	}
	ac, err := svc.QueryAccountConfig(&perprpc.QueryAccountConfigRequest{UserId: user1})
	if err != nil || ac.RiskModel != "STANDARD" || ac.CrossPoolId != "cross:1001:USDT" {
		t.Fatalf("account config: %+v err=%v", ac, err)
	}
}
