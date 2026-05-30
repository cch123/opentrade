package perprisk

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func TestCoordinator_FoldsInsuranceDeltaFromJournal(t *testing.T) {
	c := New()
	events := []*eventpb.PerpJournalEvent{
		{Payload: &eventpb.PerpJournalEvent_Liquidation{Liquidation: &eventpb.PerpLiquidationEvent{
			Symbol: "BTC-USDT-PERP", LiqOrderId: 7, InsuranceDelta: "-5",
		}}},
		{Payload: &eventpb.PerpJournalEvent_Adl{Adl: &eventpb.PerpAdlEvent{
			Symbol: "BTC-USDT-PERP", AdlRound: 1, InsuranceDelta: "3",
		}}},
	}
	for i, evt := range events {
		applied, err := c.ApplyJournalEventAt(evt, 0, int64(i))
		if err != nil {
			t.Fatalf("fold event %d: %v", i, err)
		}
		if !applied {
			t.Fatalf("event %d should carry an insurance delta", i)
		}
	}
	if got := c.Fund("USDT"); got.Cmp(dec.New("-2")) != 0 {
		t.Fatalf("fund = %s, want -2", got)
	}
	if got := c.Offset(0); got != 2 {
		t.Fatalf("next offset = %d, want 2", got)
	}
}

func TestDeltaFromJournalEvent_TakeoverCarriesBorrowRequest(t *testing.T) {
	d, ok, err := DeltaFromJournalEvent(&eventpb.PerpJournalEvent{
		Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
			UserId: "u1", Symbol: "BTC-USDT-PERP", LiqOrderId: 9,
			BankruptcyPrice: "90", ClosedQty: "2", InsuranceDelta: "-7",
			TakeoverNotional: "180",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("takeover should carry an insurance delta")
	}
	if !d.Backstop || d.RefID != "9" || d.TakeoverNotional.Cmp(dec.New("180")) != 0 || d.Delta.Cmp(dec.New("-7")) != 0 {
		t.Fatalf("unexpected takeover delta: %+v", d)
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
	// usage. This conservative choice is intentional: it prevents one symbol
	// from churning borrow/repay cycles to bypass the daily cap.
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

func TestCoordinator_WorkingCapitalLoanIsSnapshottedAndReplaySafe(t *testing.T) {
	c := New()
	_ = c.ApplyDelta(InsuranceDelta{Coin: "USDT", Delta: dec.New("100")})
	got, err := c.BorrowWorkingCapital(BorrowRequest{
		Coin: "USDT", Symbol: "BTC-USDT-PERP", Day: "2026-05-31",
		RefID: "takeover:1", Amount: dec.New("30"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Borrowed.Cmp(dec.New("30")) != 0 {
		t.Fatalf("borrowed = %s, want 30", got.Borrowed)
	}
	dup, err := c.BorrowWorkingCapital(BorrowRequest{
		Coin: "USDT", Symbol: "BTC-USDT-PERP", Day: "2026-05-31",
		RefID: "takeover:1", Amount: dec.New("30"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if dup.Borrowed.Sign() != 0 {
		t.Fatalf("duplicate borrow = %s, want 0", dup.Borrowed)
	}

	var restored Coordinator
	restored.Restore(c.Snapshot())
	if got := restored.Fund("USDT"); got.Cmp(dec.New("70")) != 0 {
		t.Fatalf("restored fund = %s, want 70", got)
	}
	if err := restored.RepayWorkingCapitalRef("takeover:1", dec.New("10")); err != nil {
		t.Fatal(err)
	}
	if got := restored.Fund("USDT"); got.Cmp(dec.New("80")) != 0 {
		t.Fatalf("fund after repayment = %s, want 80", got)
	}
}

func TestPlanADL_CarriesPosSeqAndStopsAtDeficit(t *testing.T) {
	tasks := PlanADL(dec.New("6"), dec.New("90"), 42, []ADLCandidate{
		{UserID: "low", Symbol: "BTC-USDT-PERP", Side: perpstate.SideSell, Size: dec.New("10"), Score: dec.New("1"), SacrificePerQty: dec.New("1"), PosSeq: 11, PositionVersion: 4},
		{UserID: "high", Symbol: "BTC-USDT-PERP", Side: perpstate.SideSell, Size: dec.New("3"), Score: dec.New("5"), SacrificePerQty: dec.New("2"), PosSeq: 99, PositionVersion: 8},
	})
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	if tasks[0].UserID != "high" || tasks[0].Qty.Cmp(dec.New("3")) != 0 || tasks[0].PosSeq != 99 || tasks[0].PositionVersion != 8 || tasks[0].AdlRound != 42 {
		t.Fatalf("unexpected task: %+v", tasks[0])
	}
}

func TestCoordinator_SnapshotRoundTrip(t *testing.T) {
	c := New()
	_ = c.ApplyDelta(InsuranceDelta{Coin: "USDT", Delta: dec.New("7")})
	_, _ = c.BorrowWorkingCapital(BorrowRequest{Coin: "USDT", Symbol: "ETH-USDT-PERP", Day: "2026-05-31", Amount: dec.New("2")})
	_, _ = c.ApplyJournalEventAt(&eventpb.PerpJournalEvent{
		Payload: &eventpb.PerpJournalEvent_Liquidation{Liquidation: &eventpb.PerpLiquidationEvent{
			Symbol: "BTC-USDT-PERP", InsuranceDelta: "1",
		}},
	}, 3, 10)
	round := c.ReserveAdlRound()

	var restored Coordinator
	restored.Restore(c.Snapshot())
	if got := restored.Fund("USDT"); got.Cmp(dec.New("6")) != 0 {
		t.Fatalf("restored fund = %s, want 6", got)
	}
	if got := restored.Offset(3); got != 11 {
		t.Fatalf("restored offset = %d, want 11", got)
	}
	if got := restored.ReserveAdlRound(); got != round+1 {
		t.Fatalf("restored next round = %d, want %d", got, round+1)
	}
}
