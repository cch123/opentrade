package service

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func newLiqSvc() (*Service, *engine.Engine, *fakeDispatcher, *fakeJournal) {
	eng := engine.New()
	disp := &fakeDispatcher{}
	jr := &fakeJournal{}
	var id uint64
	svc := New(eng, disp, jr, func() uint64 { id++; return id },
		Config{MaxLeverage: dec.New("100"), MMR: dec.New("0.05"), ProducerID: "perp-shard-0"})
	return svc, eng, disp, jr
}

// liqFill drives a fill of the bankruptcy order (taker = liquidated user) at
// price, closing qty against an external maker on another shard.
func liqFill(svc *Service, bankruptcyID uint64, price, qty string, matchSeq uint64) {
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "liq", Symbol: perpSym, Price: price, Qty: qty,
		MakerUserId: "mm", MakerOrderId: 9999, TakerUserId: "u1", TakerOrderId: bankruptcyID,
		TakerSide:           eventpb.Side_SIDE_SELL,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		TakerFilledQtyAfter: qty,
	}, matchSeq)
}

// triggerLiquidation opens a long 1@100 (margin 10, bankruptcy 90), drops the
// mark below maintenance, and returns the dispatched bankruptcy order id.
func triggerLiquidation(t *testing.T, svc *Service, eng *engine.Engine, disp *fakeDispatcher) uint64 {
	t.Helper()
	openPosition(eng, "u1", perpSym, perpstate.SideBuy, "100", "1", "10")
	svc.HandlePerpPriceEvent(markTickEvt(perpSym, "90")) // ratio (10-10)/90 = 0 <= 0.05
	if len(disp.orders) != 1 {
		t.Fatalf("expected one bankruptcy order dispatched, got %d", len(disp.orders))
	}
	placed := disp.orders[0].GetPlaced()
	if placed.GetSide() != eventpb.Side_SIDE_SELL || placed.GetQty() != "1" {
		t.Fatalf("bankruptcy order should be SELL 1, got side=%v qty=%s", placed.GetSide(), placed.GetQty())
	}
	if placed.GetPrice() != "90" {
		t.Fatalf("bankruptcy price = %s, want 90", placed.GetPrice())
	}
	return placed.GetOrderId()
}

func TestLiquidation_FillAtBankruptcy_InsuranceUnchanged(t *testing.T) {
	svc, eng, disp, jr := newLiqSvc()
	bankID := triggerLiquidation(t, svc, eng, disp)

	liqFill(svc, bankID, "90", "1", 5) // fill exactly at bankruptcy

	if _, ok := eng.PositionOf("u1", perpSym); ok {
		t.Fatal("position should be closed after full liquidation")
	}
	if got := eng.InsuranceFund(perpSym); got.Sign() != 0 {
		t.Fatalf("insurance should be 0 at bankruptcy fill, got %s", got)
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetLiquidation() != nil }) != 1 {
		t.Fatal("expected one liquidation journal event")
	}
	if svc.hasLiquidation(liqKey("u1", perpSym)) {
		t.Fatal("liquidation guard should clear once the position is closed")
	}
	if svc.OrderCount() != 0 {
		t.Fatalf("bankruptcy order should be evicted, have %d", svc.OrderCount())
	}
}

func TestLiquidation_FillAboveBankruptcy_InsuranceSurplus(t *testing.T) {
	svc, eng, disp, _ := newLiqSvc()
	bankID := triggerLiquidation(t, svc, eng, disp)

	// Filled at 92 (better than bankruptcy 90): realized = (92-100)*1 = -8,
	// margin released = 10 → surplus 2 into the fund.
	liqFill(svc, bankID, "92", "1", 5)
	if got := eng.InsuranceFund(perpSym); got.String() != "2" {
		t.Fatalf("insurance surplus = %s, want 2", got)
	}
}

func TestLiquidation_FillBelowBankruptcy_FundCoversAndAdlFlag(t *testing.T) {
	svc, eng, disp, jr := newLiqSvc()
	bankID := triggerLiquidation(t, svc, eng, disp)

	// Filled at 85 (worse than bankruptcy 90): realized = (85-100)*1 = -15,
	// margin released 10 → deficit 5; fund goes negative → adl_queued alert.
	liqFill(svc, bankID, "85", "1", 5)
	if got := eng.InsuranceFund(perpSym); got.String() != "-5" {
		t.Fatalf("insurance after deficit = %s, want -5", got)
	}
	adl := false
	for _, e := range jr.evts {
		if l := e.GetLiquidation(); l != nil && l.GetAdlQueued() {
			adl = true
		}
	}
	if !adl {
		t.Fatal("adl_queued should be set when the fund goes negative")
	}
}

func TestLiquidation_OneOrderPerPosition(t *testing.T) {
	svc, eng, disp, _ := newLiqSvc()
	triggerLiquidation(t, svc, eng, disp)
	// A second mark tick while the takeover is in flight must NOT dispatch again.
	svc.HandlePerpPriceEvent(markTickEvt(perpSym, "90"))
	svc.HandlePerpPriceEvent(markTickEvt(perpSym, "88"))
	if len(disp.orders) != 1 {
		t.Fatalf("liquidation must place exactly one bankruptcy order, got %d", len(disp.orders))
	}
}

func TestLiquidation_PartialFillThenClose(t *testing.T) {
	svc, eng, disp, _ := newLiqSvc()
	// Long 2 @ 100 lev 10 → margin 20, bankruptcy 90.
	openPosition(eng, "u1", perpSym, perpstate.SideBuy, "100", "2", "10")
	svc.HandlePerpPriceEvent(markTickEvt(perpSym, "90"))
	bankID := disp.orders[0].GetPlaced().GetOrderId()

	// Partial fill 1 of 2 at bankruptcy: position still open, guard held.
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "p1", Symbol: perpSym, Price: "90", Qty: "1",
		MakerUserId: "mm", MakerOrderId: 1, TakerUserId: "u1", TakerOrderId: bankID,
		TakerSide:           eventpb.Side_SIDE_SELL,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED,
		TakerFilledQtyAfter: "1",
	}, 5)
	if _, ok := eng.PositionOf("u1", perpSym); !ok {
		t.Fatal("position should still be open after partial liquidation fill")
	}
	if !svc.hasLiquidation(liqKey("u1", perpSym)) {
		t.Fatal("guard should stay set during a partial liquidation")
	}
	// Final fill (incremental qty 1, cumulative 2) closes it; insurance nets 0
	// across both fills at bankruptcy.
	svc.HandleTrade(&eventpb.Trade{
		TradeId: "p2", Symbol: perpSym, Price: "90", Qty: "1",
		MakerUserId: "mm", MakerOrderId: 2, TakerUserId: "u1", TakerOrderId: bankID,
		TakerSide:           eventpb.Side_SIDE_SELL,
		TakerStatusAfter:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		TakerFilledQtyAfter: "2",
	}, 6)
	if _, ok := eng.PositionOf("u1", perpSym); ok {
		t.Fatal("position should be closed after the final fill")
	}
	if got := eng.InsuranceFund(perpSym); got.Sign() != 0 {
		t.Fatalf("insurance should net 0 across bankruptcy fills, got %s", got)
	}
}
