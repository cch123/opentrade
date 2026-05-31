package perprisk

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func TestCoordinator_FoldsSettlementButNotADLInsuranceDelta(t *testing.T) {
	c := New()
	events := []*eventpb.PerpJournalEvent{
		{Payload: &eventpb.PerpJournalEvent_Adl{Adl: &eventpb.PerpAdlEvent{
			UserId: 2002, Symbol: "BTC-USDT-PERP", AdlRound: 1, InsuranceDelta: "3",
		}}},
		{Payload: &eventpb.PerpJournalEvent_RiskPoolSettlement{RiskPoolSettlement: &eventpb.RiskPoolSettlementEvent{
			LotId: "lot-1", Symbol: "BTC-USDT-PERP", Coin: "USDT",
			WorkingCapitalDrawn: "10", TakenOverBalance: "4", LiqAdlRealisedPnl: "1",
			BorrowedBalance: "5", FinalPoolDelta: "-5",
		}}},
	}
	for i, evt := range events {
		applied, err := c.ApplyJournalEventAt(evt, 0, int64(i))
		if err != nil {
			t.Fatalf("fold event %d: %v", i, err)
		}
		if i == 0 && applied {
			t.Fatal("ADL without lot_id must not carry a fund movement")
		}
		if i == 1 && !applied {
			t.Fatal("settlement should carry the authoritative fund movement")
		}
	}
	if got := c.Fund("USDT"); got.Cmp(dec.New("-5")) != 0 {
		t.Fatalf("fund = %s, want -5", got)
	}
	if got := c.Offset(0); got != 2 {
		t.Fatalf("next offset = %d, want 2", got)
	}
}

func TestJournalResultFromEvent_TakeoverCreatesLotBorrowRequest(t *testing.T) {
	result, err := JournalResultFromEvent(&eventpb.PerpJournalEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 99},
		Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
			UserId: 1001, Symbol: "BTC-USDT-PERP", LiqOrderId: 9,
			LotId: "lot-9", BankruptcyPrice: "90", MarkPrice: "85",
			ClosedQty: "2", TakenOverQty: "2", TakeoverPrice: "90",
			TakenOverBalance: "-7", TakeoverNotional: "180",
			InventorySide: eventpb.Side_SIDE_BUY, PositionVersion: 4,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != JournalKindTakeoverLot || !result.Applied {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.LotID != "lot-9" || result.BorrowRef != "takeover:lot-9" || result.BorrowAmount.Cmp(dec.New("180")) != 0 {
		t.Fatalf("unexpected borrow result: %+v", result)
	}
	if result.Lot.LeavesQty.Cmp(dec.New("2")) != 0 || result.Lot.TakenOverBalance.Cmp(dec.New("-7")) != 0 ||
		result.Lot.Side != perpstate.SideBuy || result.Lot.PositionVersion != 4 || result.Lot.CreatedUnixMs != 99 {
		t.Fatalf("unexpected lot: %+v", result.Lot)
	}
}

func TestCoordinator_BorrowWorkingCapitalUsesThreeWayMinAndDailyReset(t *testing.T) {
	c := New()
	if err := c.ApplyDelta(InsuranceDelta{Coin: "USDT", Symbol: "seed", Delta: dec.New("100")}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetQuotaPolicy("BTC-USDT-PERP", QuotaPolicy{Fraction: dec.New("0.25"), AbsoluteCap: dec.New("40")}); err != nil {
		t.Fatal(err)
	}

	got, err := c.BorrowWorkingCapital(BorrowRequest{
		Coin: "USDT", Symbol: "BTC-USDT-PERP", Day: "2026-05-31", Amount: dec.New("50"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Borrowed.Cmp(dec.New("25")) != 0 {
		t.Fatalf("first borrow = %s, want 25", got.Borrowed)
	}
	got, err = c.BorrowWorkingCapital(BorrowRequest{
		Coin: "USDT", Symbol: "BTC-USDT-PERP", Day: "2026-05-31", Amount: dec.New("50"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Quota is recomputed from the remaining global fund, then reduced by gross
	// usage. This conservative choice prevents borrow/repay churn from bypassing
	// the daily cap.
	if got.Borrowed.Sign() != 0 {
		t.Fatalf("same-day second borrow = %s, want 0", got.Borrowed)
	}
	got, err = c.BorrowWorkingCapital(BorrowRequest{
		Coin: "USDT", Symbol: "BTC-USDT-PERP", Day: "2026-06-01", Amount: dec.New("50"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Borrowed.Cmp(dec.New("18.75")) != 0 {
		t.Fatalf("next-day borrow = %s, want 18.75", got.Borrowed)
	}
}

func TestCoordinator_LotSettlementIsSnapshottedAndIdempotent(t *testing.T) {
	c := New()
	_ = c.ApplyDelta(InsuranceDelta{Coin: "USDT", Delta: dec.New("100")})
	result, err := c.ApplyJournalEventAtResult(&eventpb.PerpJournalEvent{
		Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
			UserId: 1001, Symbol: "BTC-USDT-PERP", LotId: "lot-1",
			BankruptcyPrice: "90", ClosedQty: "1", TakenOverBalance: "0", TakeoverNotional: "30",
			InventorySide: eventpb.Side_SIDE_BUY,
		}},
	}, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	borrow, err := c.BorrowWorkingCapital(BorrowRequest{
		Coin: "USDT", Symbol: "BTC-USDT-PERP", Day: "2026-05-31",
		RefID: result.BorrowRef, Amount: result.BorrowAmount,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.MarkLotWorkingCapital("lot-1", result.BorrowRef, borrow.Borrowed); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyLotADLResult("lot-1", dec.New("1"), dec.New("5")); err != nil {
		t.Fatal(err)
	}
	settlement, applied, err := c.SettleLot("lot-1")
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("first settlement should apply")
	}
	if settlement.FinalPoolDelta.Cmp(dec.New("-25")) != 0 {
		t.Fatalf("final_pool_delta = %s, want -25", settlement.FinalPoolDelta)
	}
	if got := c.Fund("USDT"); got.Cmp(dec.New("45")) != 0 {
		t.Fatalf("fund after borrow + settlement = %s, want 45", got)
	}
	if _, applied, err = c.SettleLot("lot-1"); err != nil || applied {
		t.Fatalf("duplicate settlement applied=%v err=%v", applied, err)
	}

	var restored Coordinator
	restored.Restore(c.Snapshot())
	if got := restored.Fund("USDT"); got.Cmp(dec.New("45")) != 0 {
		t.Fatalf("restored fund = %s, want 45", got)
	}
	if lot, ok := restored.Lot("lot-1"); !ok || lot.Status != LotStatusDone {
		t.Fatalf("restored lot = %+v ok=%v", lot, ok)
	}
}

func TestPlanADL_CarriesLotAndStopsAtLeavesQty(t *testing.T) {
	lot := TakenOverLot{
		LotID: "lot-1", Symbol: "BTC-USDT-PERP", UserID: 1001,
		LeavesQty: dec.New("4"), TakeoverPrice: dec.New("90"),
	}
	tasks := PlanADL(lot, dec.New("90"), 42, []ADLCandidate{
		{UserID: 2001, Symbol: "BTC-USDT-PERP", Side: perpstate.SideSell, Size: dec.New("10"), Score: dec.New("1"), SacrificePerQty: dec.New("1"), PosSeq: 11, PositionVersion: 4},
		{UserID: 2002, Symbol: "BTC-USDT-PERP", Side: perpstate.SideSell, Size: dec.New("3"), Score: dec.New("5"), SacrificePerQty: dec.New("2"), PosSeq: 99, PositionVersion: 8},
	})
	if len(tasks) != 2 {
		t.Fatalf("tasks = %d, want 2", len(tasks))
	}
	if tasks[0].LotID != "lot-1" || tasks[0].UserID != 2002 || tasks[0].Qty.Cmp(dec.New("3")) != 0 ||
		tasks[0].PosSeq != 99 || tasks[0].PositionVersion != 8 || tasks[0].AdlRound != 42 {
		t.Fatalf("unexpected first task: %+v", tasks[0])
	}
	if tasks[1].UserID != 2001 || tasks[1].Qty.Cmp(dec.New("1")) != 0 {
		t.Fatalf("unexpected second task: %+v", tasks[1])
	}
}

func TestCoordinator_SnapshotRoundTrip(t *testing.T) {
	c := New()
	_ = c.ApplyDelta(InsuranceDelta{Coin: "USDT", Delta: dec.New("7")})
	_, _ = c.BorrowWorkingCapital(BorrowRequest{Coin: "USDT", Symbol: "ETH-USDT-PERP", Day: "2026-05-31", Amount: dec.New("2")})
	_, _ = c.ApplyJournalEventAtResult(&eventpb.PerpJournalEvent{
		Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
			UserId: 1001, Symbol: "BTC-USDT-PERP", LotId: "lot-snap",
			BankruptcyPrice: "90", ClosedQty: "1", TakenOverBalance: "0",
			InventorySide: eventpb.Side_SIDE_BUY,
		}},
	}, 3, 10)
	round := c.ReserveAdlRound()
	if err := c.RegisterInFlightADL(ADLTask{
		LotID: "lot-snap", UserID: 2002, Symbol: "BTC-USDT-PERP",
		Side: perpstate.SideSell, Qty: dec.New("0.25"), Price: dec.New("90"),
		PosSeq: 44, PositionVersion: 5, AdlRound: round,
	}); err != nil {
		t.Fatal(err)
	}
	if planningLot, ok := c.LotForADLPlanning("lot-snap"); !ok || planningLot.LeavesQty.Cmp(dec.New("0.75")) != 0 {
		t.Fatalf("planning lot = %+v ok=%v, want leaves 0.75", planningLot, ok)
	}

	var restored Coordinator
	restored.Restore(c.Snapshot())
	if got := restored.Fund("USDT"); got.Cmp(dec.New("5")) != 0 {
		t.Fatalf("restored fund = %s, want 5", got)
	}
	if got := restored.Offset(3); got != 11 {
		t.Fatalf("restored offset = %d, want 11", got)
	}
	if got := restored.ReserveAdlRound(); got != round+1 {
		t.Fatalf("restored next round = %d, want %d", got, round+1)
	}
	if _, ok := restored.Lot("lot-snap"); !ok {
		t.Fatal("restored snapshot should include open lot")
	}
	if got := restored.Snapshot().InFlightADL; len(got) != 1 || got[0].LotID != "lot-snap" || got[0].AdlRound != round {
		t.Fatalf("restored in-flight ADL = %+v", got)
	}
	_, err := restored.ApplyJournalEventAtResult(&eventpb.PerpJournalEvent{
		Payload: &eventpb.PerpJournalEvent_Adl{Adl: &eventpb.PerpAdlEvent{
			UserId: 2002, Symbol: "BTC-USDT-PERP", LotId: "lot-snap", AdlRound: round,
			Price: "90", FactQty: "0.25", RealizedPnl: "1",
		}},
	}, 3, 11)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot().InFlightADL; len(got) != 0 {
		t.Fatalf("ADL journal event should clear in-flight task, got %+v", got)
	}
	result, err := restored.ApplyJournalEventAtResult(&eventpb.PerpJournalEvent{
		Payload: &eventpb.PerpJournalEvent_Adl{Adl: &eventpb.PerpAdlEvent{
			UserId: 2002, Symbol: "BTC-USDT-PERP", LotId: "lot-snap", AdlRound: round,
			Price: "90", FactQty: "0.25", RealizedPnl: "1",
		}},
	}, 3, 11)
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied {
		t.Fatal("duplicate offset should not reapply ADL fact_qty")
	}
	if lot, ok := restored.Lot("lot-snap"); !ok || lot.LeavesQty.Cmp(dec.New("0.75")) != 0 {
		t.Fatalf("duplicate offset changed lot: %+v ok=%v", lot, ok)
	}
}
