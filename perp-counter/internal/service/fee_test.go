package service

// fee_test.go covers the ADR-0079 service-level fee flow: admission pinning
// (rates + rule id + buffer), maker/taker selection at settlement, the
// terminal 多退少补 release, negative-maker handling under the deployment
// switch, self-trade rebate suppression, the liquidation-leg exemption, the
// platform SUM audit invariant, and the snapshot round-trip of pins.

import (
	"testing"
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpcfg"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// newFeeSvc builds a catalog-backed service with the given fee params and
// negative-maker switch. maker/taker replace the fixture defaults via a
// version-2 publish so orders pin against the published rates.
func newFeeSvc(t *testing.T, maker, taker string, allowNeg bool) (*Service, *engine.Engine, *fakeJournal, *catalogFixture) {
	t.Helper()
	fix := newCatalogFixture(t, perpcfg.StatusTrading)
	fix.publish(t, func(c *perpcfg.PerpSymbolConfig) {
		c.Fees = perpcfg.FeeParams{MakerFeeRate: dec.New(maker), TakerFeeRate: dec.New(taker)}
	})
	eng := engine.New()
	jr := &fakeJournal{}
	var id uint64
	svc := New(eng, &fakeDispatcher{}, jr, func() uint64 { id++; return id },
		Config{ProducerID: "perp-shard-0", Catalog: fix.cache,
			AllowNegativeMakerFee: allowNeg,
			Clock:                 func() time.Time { return fix.clock.now }})
	return svc, eng, jr, fix
}

func mustPlace(t *testing.T, svc *Service, req *perprpc.PlaceOrderRequest) uint64 {
	t.Helper()
	resp, err := svc.PlaceOrder(req)
	if err != nil || !resp.Accepted {
		t.Fatalf("place: err=%v resp=%+v", err, resp)
	}
	return resp.OrderId
}

// fillTrade settles maker/taker orders fully at price/qty.
func fillTrade(svc *Service, seq uint64, price, qty string, makerUser, makerOrder, takerUser, takerOrder uint64, takerSide eventpb.Side) {
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t1", Symbol: catSym, Price: price, Qty: qty,
		MakerUserId: makerUser, MakerOrderId: makerOrder,
		TakerUserId: takerUser, TakerOrderId: takerOrder, TakerSide: takerSide,
		MakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		MakerFilledQtyAfter: qty, TakerFilledQtyAfter: qty,
	}, seq)
}

func settlementOf(t *testing.T, jr *fakeJournal, orderID uint64) *eventpb.PerpSettlementEvent {
	t.Helper()
	for _, e := range jr.evts {
		if s := e.GetSettlement(); s != nil && s.GetOrderId() == orderID {
			return s
		}
	}
	t.Fatalf("no settlement event for order %d", orderID)
	return nil
}

// Taker pays the taker rate, maker the maker rate; both consume their fee
// buffer; the platform account collects the sum; the journal carries the
// full ADR-0079 attribution.
func TestFee_MakerTakerRatesAndPlatformAccount(t *testing.T) {
	svc, eng, jr, _ := newFeeSvc(t, "0.001", "0.002", false)
	eng.Deposit(user1, dec.New("10000"))
	eng.Deposit(user2, dec.New("10000"))
	makerOrd := mustPlace(t, svc, placeReq(user2, catSym, eventpb.Side_SIDE_SELL, "100", "1", "10", false))
	// Order cost = IM 10 + fee buffer (100 × 1 × taker 0.002 = 0.2).
	eqd(t, eng.WalletOf(user2).Reserved, "10.2", "order cost incl. fee buffer")
	takerOrd := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))

	fillTrade(svc, 1, "100", "1", user2, makerOrd, user1, takerOrd, eventpb.Side_SIDE_BUY)

	// Taker: fee 100×1×0.002 = 0.2, drawn entirely from the buffer.
	w1 := eng.WalletOf(user1)
	eqd(t, w1.Available, "9989.8", "taker available (10000 - IM 10 - fee 0.2)")
	eqd(t, w1.Reserved, "0", "taker fully released")
	// Maker: fee 0.1; the unconsumed half of the buffer released at FILLED.
	w2 := eng.WalletOf(user2)
	eqd(t, w2.Available, "9989.9", "maker available (10000 - IM 10 - fee 0.1)")
	eqd(t, w2.Reserved, "0", "maker fully released")
	eqd(t, eng.PlatformFee("USDT"), "0.3", "platform collected both fees")

	ts := settlementOf(t, jr, takerOrd)
	if ts.GetLiquidityRole() != eventpb.LiquidityRole_LIQUIDITY_ROLE_TAKER ||
		ts.GetFee() != "0.2" || ts.GetFeeRate() != "0.002" || ts.GetFeeAsset() != "USDT" ||
		ts.GetFeeRuleId() != "sym:"+catSym+"@v2" || ts.GetFeeDeficit() != "0" || ts.GetRebateSuppressed() {
		t.Fatalf("taker settlement fee attribution wrong: %+v", ts)
	}
	eqd(t, dec.New(ts.GetWalletAfter()), "9989.8", "taker wallet_after")
	ms := settlementOf(t, jr, makerOrd)
	if ms.GetLiquidityRole() != eventpb.LiquidityRole_LIQUIDITY_ROLE_MAKER || ms.GetFee() != "0.1" {
		t.Fatalf("maker settlement fee attribution wrong: %+v", ms)
	}

	// ADR-0079 §2 audit invariant: platform balance == Σ(fee - fee_deficit).
	sum := zero
	for _, e := range jr.evts {
		if s := e.GetSettlement(); s != nil {
			sum = sum.Add(dec.New(s.GetFee())).Sub(dec.New(s.GetFeeDeficit()))
		}
	}
	if sum.Cmp(eng.PlatformFee("USDT")) != 0 {
		t.Fatalf("audit invariant broken: journal sum %s platform %s", sum, eng.PlatformFee("USDT"))
	}
}

// A partially filled order keeps the unconsumed buffer until the terminal
// transition (多退少补 settles at terminal): cancel releases remaining IM +
// remaining fee buffer.
func TestFee_BufferRefundOnCancelAfterPartialFill(t *testing.T) {
	svc, eng, _, _ := newFeeSvc(t, "0.001", "0.002", false)
	eng.Deposit(user1, dec.New("10000"))
	eng.Deposit(user2, dec.New("10000"))
	makerOrd := mustPlace(t, svc, placeReq(user2, catSym, eventpb.Side_SIDE_SELL, "100", "2", "10", false))
	eqd(t, eng.WalletOf(user2).Reserved, "20.4", "IM 20 + buffer 0.4")
	takerOrd := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))

	svc.HandleTrade(&eventpb.Trade{
		TradeId: "t1", Symbol: catSym, Price: "100", Qty: "1",
		MakerUserId: user2, MakerOrderId: makerOrd,
		TakerUserId: user1, TakerOrderId: takerOrd, TakerSide: eventpb.Side_SIDE_BUY,
		MakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		MakerFilledQtyAfter: "1", TakerFilledQtyAfter: "1",
	}, 1)
	// Maker after partial fill: IM 10 → position margin, fee 0.1 consumed
	// from the buffer; hold = remaining IM 10 + remaining buffer 0.3.
	eqd(t, eng.WalletOf(user2).Reserved, "10.3", "remaining hold after partial fill")

	svc.HandleTradeEvent(&eventpb.TradeEvent{
		MatchSeqId: 2,
		Payload: &eventpb.TradeEvent_Cancelled{Cancelled: &eventpb.OrderCancelled{
			UserId: user2, OrderId: makerOrd, Symbol: catSym, FilledQty: "1",
		}},
	}, 0, 1)
	w := eng.WalletOf(user2)
	eqd(t, w.Reserved, "0", "everything released on cancel")
	// 10000 - position margin 10 - paid fee 0.1.
	eqd(t, w.Available, "9989.9", "available after refund")
}

// Per-user overrides pin with fixed precedence: user+symbol > user-global >
// symbol config; removing rows falls back down the chain (ADR-0079 §1).
func TestFee_OverridePrecedenceAtAdmission(t *testing.T) {
	svc, eng, _, _ := newFeeSvc(t, "0.001", "0.002", false)
	eng.Deposit(user1, dec.New("10000"))

	set := func(symbol, rule, maker, taker string) {
		t.Helper()
		resp, err := svc.SetCustomerFeeRate(&perprpc.SetCustomerFeeRateRequest{
			UserId: user1, Symbol: symbol, FeeRuleId: rule,
			MakerFeeRate: maker, TakerFeeRate: taker, UpdatedBy: "ops",
		})
		if err != nil || !resp.Accepted {
			t.Fatalf("set fee rate: err=%v resp=%+v", err, resp)
		}
	}
	set("", "vip-global", "0.0001", "0.001")
	set(catSym, "vip-sym", "0.0002", "0.0005")

	pinOf := func(orderID uint64) (string, string) {
		o := svc.getOrder(orderID)
		if o == nil {
			t.Fatalf("order %d not found", orderID)
		}
		return o.FeeRuleID, o.FeeTakerRate.String()
	}

	id1 := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if rule, taker := pinOf(id1); rule != "vip-sym" || taker != "0.0005" {
		t.Fatalf("symbol override must win: rule=%s taker=%s", rule, taker)
	}

	set(catSym, "", "", "") // remove the symbol row → global row pins
	id2 := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if rule, taker := pinOf(id2); rule != "vip-global" || taker != "0.001" {
		t.Fatalf("global override must back: rule=%s taker=%s", rule, taker)
	}

	set("", "", "", "") // remove the global row → symbol config pins
	id3 := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	if rule, taker := pinOf(id3); rule != "sym:"+catSym+"@v2" || taker != "0.002" {
		t.Fatalf("symbol config must back: rule=%s taker=%s", rule, taker)
	}

	// An earlier order keeps its admission pin regardless of later changes.
	if rule, _ := pinOf(id1); rule != "vip-sym" {
		t.Fatalf("in-flight order re-pinned: %s", rule)
	}
}

// Negative maker rate with the deployment switch ON: the maker receives the
// rebate, the platform balance absorbs it as a liability.
func TestFee_NegativeMakerRebatePaid(t *testing.T) {
	svc, eng, jr, _ := newFeeSvc(t, "-0.0005", "0.002", true)
	eng.Deposit(user1, dec.New("10000"))
	eng.Deposit(user2, dec.New("10000"))
	makerOrd := mustPlace(t, svc, placeReq(user2, catSym, eventpb.Side_SIDE_SELL, "100", "1", "10", false))
	takerOrd := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	fillTrade(svc, 1, "100", "1", user2, makerOrd, user1, takerOrd, eventpb.Side_SIDE_BUY)

	// Maker: -0.05 fee = rebate; full buffer (0.2) refunds at terminal.
	eqd(t, eng.WalletOf(user2).Available, "9990.05", "maker got the rebate (10000 - IM 10 + 0.05)")
	eqd(t, eng.PlatformFee("USDT"), "0.15", "platform net = taker 0.2 - rebate 0.05")
	ms := settlementOf(t, jr, makerOrd)
	if ms.GetFee() != "-0.05" || ms.GetRebateSuppressed() {
		t.Fatalf("maker rebate attribution wrong: %+v", ms)
	}
}

// Negative maker rate with the switch OFF: degraded to zero at admission,
// journaled as rebate_suppressed on maker fills.
func TestFee_NegativeMakerDegradedWhenDisabled(t *testing.T) {
	svc, eng, jr, _ := newFeeSvc(t, "-0.0005", "0.002", false)
	eng.Deposit(user1, dec.New("10000"))
	eng.Deposit(user2, dec.New("10000"))
	makerOrd := mustPlace(t, svc, placeReq(user2, catSym, eventpb.Side_SIDE_SELL, "100", "1", "10", false))
	takerOrd := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	fillTrade(svc, 1, "100", "1", user2, makerOrd, user1, takerOrd, eventpb.Side_SIDE_BUY)

	eqd(t, eng.WalletOf(user2).Available, "9990", "no rebate paid (10000 - IM 10)")
	eqd(t, eng.PlatformFee("USDT"), "0.2", "platform = taker fee only")
	ms := settlementOf(t, jr, makerOrd)
	if ms.GetFee() != "0" || !ms.GetRebateSuppressed() {
		t.Fatalf("maker degrade attribution wrong: %+v", ms)
	}

	// The admin override path rejects outright instead of degrading.
	resp, err := svc.SetCustomerFeeRate(&perprpc.SetCustomerFeeRateRequest{
		UserId: user1, FeeRuleId: "mm-neg", MakerFeeRate: "-0.0001", TakerFeeRate: "0.001",
	})
	if err != nil || resp.Accepted || resp.RejectReason != "negative_maker_fee_disabled" {
		t.Fatalf("negative override must be rejected: err=%v resp=%+v", err, resp)
	}
}

// Self-trade: the maker leg's rebate is suppressed deterministically from
// the Trade payload; the taker leg still pays (ADR-0079 §5).
func TestFee_SelfTradeRebateSuppressedTakerPays(t *testing.T) {
	svc, eng, jr, _ := newFeeSvc(t, "-0.0005", "0.002", true)
	eng.Deposit(user1, dec.New("10000"))
	makerOrd := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_SELL, "100", "1", "10", false))
	takerOrd := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	fillTrade(svc, 1, "100", "1", user1, makerOrd, user1, takerOrd, eventpb.Side_SIDE_BUY)

	ms := settlementOf(t, jr, makerOrd)
	if ms.GetFee() != "0" || !ms.GetRebateSuppressed() {
		t.Fatalf("self-trade maker rebate must be suppressed: %+v", ms)
	}
	ts := settlementOf(t, jr, takerOrd)
	if ts.GetFee() != "0.2" || ts.GetRebateSuppressed() {
		t.Fatalf("self-trade taker must pay normally: %+v", ts)
	}
	eqd(t, eng.PlatformFee("USDT"), "0.2", "platform = taker fee, no rebate out")
	// Buy 1 + sell 1 on the net leg → flat; the user paid exactly the fee.
	eqd(t, eng.WalletOf(user1).Available, "9999.8", "flat position, taker fee paid")
}

// A bankruptcy-order fill settles through the liquidation path (liq fee →
// insurance, ADR-0070) and must NOT charge trade fees; its normal-order
// counterparty pays as usual.
func TestFee_LiquidationLegExemptCounterpartyPays(t *testing.T) {
	svc, eng, jr, _ := newFeeSvc(t, "0.001", "0.002", false)
	eng.Deposit(user1, dec.New("10000"))
	eng.Deposit(user2, dec.New("10000"))

	// Give user2 a short position to liquidate (via a normal trade first).
	m1 := mustPlace(t, svc, placeReq(user2, catSym, eventpb.Side_SIDE_SELL, "100", "1", "10", false))
	t1 := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	fillTrade(svc, 1, "100", "1", user2, m1, user1, t1, eventpb.Side_SIDE_BUY)
	feesBefore := eng.PlatformFee("USDT")

	// System-owned bankruptcy reduce-only order on user2's short (in-package
	// registration mirrors beginLiquidation's bookkeeping).
	liqOrd := &Order{
		OrderID: 777_001, UserID: user2, Symbol: catSym,
		Side: perpstate.SideBuy, Type: eventpb.OrderType_ORDER_TYPE_LIMIT,
		TIF:   eventpb.TimeInForce_TIME_IN_FORCE_GTC,
		Price: dec.New("110"), Qty: dec.New("1"), Leverage: zero, ReduceOnly: true,
		ReservedIM: zero, FilledQty: zero,
		Status:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
		CreatedMs: svc.now(), UpdatedMs: svc.now(),
	}
	svc.putOrder(liqOrd)
	svc.registerLiquidation(liqKey(user2, catSym, 0), liqOrd.OrderID, &liquidation{
		userID: user2, symbol: catSym, positionIdx: 0, orderID: liqOrd.OrderID,
		mode: liquidationFull, side: perpstate.SideSell,
		orderPrice: dec.New("110"), bankruptcy: dec.New("110"), liqFeeRate: dec.New("0.001"),
	})

	// user1 takes the other side (increases its long) against the bankruptcy
	// order resting as maker.
	t2 := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "110", "1", "10", false))
	fillTrade(svc, 2, "110", "1", user2, liqOrd.OrderID, user1, t2, eventpb.Side_SIDE_BUY)

	// The liquidated leg journals a PerpLiquidationEvent, no settlement.
	for _, e := range jr.evts {
		if s := e.GetSettlement(); s != nil && s.GetOrderId() == liqOrd.OrderID {
			t.Fatalf("bankruptcy order must not emit a fee settlement: %+v", s)
		}
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetLiquidation() != nil }) != 1 {
		t.Fatal("expected exactly one liquidation event")
	}
	// The counterparty paid its taker fee: 110 × 1 × 0.002 = 0.22.
	ts := settlementOf(t, jr, t2)
	if ts.GetFee() != "0.22" {
		t.Fatalf("counterparty fee wrong: %+v", ts)
	}
	eqd(t, eng.PlatformFee("USDT"), feesBefore.Add(dec.New("0.22")).String(), "platform grew by the counterparty fee only")
}

// Reduce-only orders reserve nothing (closability invariant): a user with
// zero Available can still close; the closing fee comes out of the released
// margin (ADR-0079 §4).
func TestFee_ReduceOnlyNoReserveFeeFromProceeds(t *testing.T) {
	svc, eng, jr, _ := newFeeSvc(t, "0.001", "0.002", false)
	eng.Deposit(user1, dec.New("10000"))
	eng.Deposit(user2, dec.New("10.2")) // exactly IM 10 + buffer 0.2
	m1 := mustPlace(t, svc, placeReq(user2, catSym, eventpb.Side_SIDE_SELL, "100", "1", "10", false))
	t1 := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	fillTrade(svc, 1, "100", "1", user2, m1, user1, t1, eventpb.Side_SIDE_BUY)
	// Maker fee 0.1 consumed; 0.1 of the buffer refunded.
	eqd(t, eng.WalletOf(user2).Available, "0.1", "all-in position, fee paid from buffer")

	// user2 closes its short with zero-ish Available: reduce-only reserves 0.
	c1 := mustPlace(t, svc, placeReq(user2, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", true))
	eqd(t, eng.WalletOf(user2).Reserved, "0", "reduce-only reserves nothing")
	t2 := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_SELL, "100", "1", "10", true))
	// user2's close rests as maker; user1 takes by SELLING (closes its long).
	fillTrade(svc, 2, "100", "1", user2, c1, user1, t2, eventpb.Side_SIDE_SELL)

	// Close at entry: margin 10 released, realized 0, maker fee 0.1 from the
	// released proceeds → 0.1 + 10 - 0.1 = 10. No deficit.
	w := eng.WalletOf(user2)
	eqd(t, w.Available, "10", "fee paid from close proceeds")
	cs := settlementOf(t, jr, c1)
	if cs.GetFeeDeficit() != "0" || cs.GetFee() != "0.1" {
		t.Fatalf("close fee attribution wrong: %+v", cs)
	}
}

// Order fee pins + buffers survive the snapshot round-trip.
func TestFee_SnapshotRoundTripsPins(t *testing.T) {
	svc, eng, _, fix := newFeeSvc(t, "0.001", "0.002", false)
	eng.Deposit(user1, dec.New("10000"))
	id1 := mustPlace(t, svc, placeReq(user1, catSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))

	engSnap, svcSnap, err := svc.Capture(nil)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	eng2 := engine.New()
	eng2.Restore(engSnap)
	var id uint64
	svc2 := New(eng2, &fakeDispatcher{}, &fakeJournal{}, func() uint64 { id++; return id },
		Config{ProducerID: "perp-shard-0", Catalog: fix.cache,
			Clock: func() time.Time { return fix.clock.now }})
	svc2.Restore(svcSnap)

	o := svc2.getOrder(id1)
	if o == nil {
		t.Fatal("order lost in snapshot")
	}
	if o.FeeRuleID != "sym:"+catSym+"@v2" || o.FeeTakerRate.Cmp(dec.New("0.002")) != 0 ||
		o.FeeMakerRate.Cmp(dec.New("0.001")) != 0 || o.FeeAsset != "USDT" {
		t.Fatalf("fee pin lost: %+v", o)
	}
	eqd(t, o.ReservedFee, "0.2", "fee buffer restored")
}
