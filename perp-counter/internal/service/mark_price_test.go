package service

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

func markTickEvt(symbol, mark string) *eventpb.MarkPriceEvent {
	return &eventpb.MarkPriceEvent{Symbol: symbol, Payload: &eventpb.MarkPriceEvent_Tick{
		Tick: &eventpb.MarkTick{MarkPrice: mark, IndexPrice: mark, FundingRate: "0", TsUnixMs: 1}}}
}

func fundingTickEvt(symbol, roundUnixSec, rate, mark string) *eventpb.MarkPriceEvent {
	return &eventpb.MarkPriceEvent{Symbol: symbol, Payload: &eventpb.MarkPriceEvent_Funding{
		Funding: &eventpb.FundingTick{
			FundingRoundId: symbol + ":" + roundUnixSec, FundingRate: rate, MarkPrice: mark, TsUnixMs: 1}}}
}

// openPosition seeds a position directly on the engine (margin reserved then a
// fill committed), bypassing the order path — enough to exercise funding /
// liquidation settlement.
func openPosition(eng interface {
	Deposit(string, dec.Decimal) dec.Decimal
	Reserve(string, dec.Decimal) bool
	ApplyFill(string, string, dec.Decimal, perpstate.Fill) perpstate.FillResult
}, user, symbol string, side perpstate.Side, price, qty, lev string) {
	p, q, l := dec.New(price), dec.New(qty), dec.New(lev)
	eng.Deposit(user, dec.New("100000"))
	eng.Reserve(user, perpstate.InitMargin(p, q, l))
	eng.ApplyFill(user, symbol, l, perpstate.Fill{Side: side, Price: p, Qty: q})
}

func TestHandleMarkTick_SetsMark(t *testing.T) {
	svc, eng, _, _ := newSvc()
	svc.HandleMarkPriceEvent(markTickEvt(perpSym, "12345.5"))
	if got := eng.MarkOf(perpSym); got.String() != "12345.5" {
		t.Fatalf("mark = %s, want 12345.5", got.String())
	}
	// Garbage / non-positive marks are ignored.
	svc.HandleMarkPriceEvent(markTickEvt(perpSym, "nonsense"))
	svc.HandleMarkPriceEvent(markTickEvt(perpSym, "0"))
	if got := eng.MarkOf(perpSym); got.String() != "12345.5" {
		t.Fatalf("mark should be unchanged by bad ticks, got %s", got.String())
	}
}

func TestHandleFundingTick_SettlesBothSidesZeroSum(t *testing.T) {
	svc, eng, _, jr := newSvc()
	openPosition(eng, "u1", perpSym, perpstate.SideBuy, "100", "1", "10")  // long, margin 10
	openPosition(eng, "u2", perpSym, perpstate.SideSell, "100", "1", "10") // short, margin 10
	eng.SetMark(perpSym, dec.New("100"))

	p1Before, _ := eng.PositionOf("u1", perpSym)
	p2Before, _ := eng.PositionOf("u2", perpSym)
	sumBefore := p1Before.Margin.Add(p2Before.Margin)

	svc.HandleMarkPriceEvent(fundingTickEvt(perpSym, "1748505600", "0.01", "100"))

	p1, _ := eng.PositionOf("u1", perpSym)
	p2, _ := eng.PositionOf("u2", perpSym)
	// Zero-sum: one side pays exactly what the other receives.
	if got := p1.Margin.Add(p2.Margin); got.Cmp(sumBefore) != 0 {
		t.Fatalf("funding not zero-sum: before=%s after=%s", sumBefore, got)
	}
	if p1.Margin.Cmp(p2.Margin) == 0 {
		t.Fatal("funding should move margin between long and short")
	}
	if n := jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetFunding() != nil }); n != 2 {
		t.Fatalf("want 2 funding journal events, got %d", n)
	}

	// Idempotent: replaying the same round settles nobody again.
	svc.HandleMarkPriceEvent(fundingTickEvt(perpSym, "1748505600", "0.01", "100"))
	p1b, _ := eng.PositionOf("u1", perpSym)
	if p1b.Margin.Cmp(p1.Margin) != 0 {
		t.Fatalf("replayed funding round must not re-settle: %s -> %s", p1.Margin, p1b.Margin)
	}
	if n := jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetFunding() != nil }); n != 2 {
		t.Fatalf("replay must not emit more funding events, got %d", n)
	}
}

func TestHandleFundingTick_MalformedRoundIDSkipped(t *testing.T) {
	svc, eng, _, jr := newSvc()
	openPosition(eng, "u1", perpSym, perpstate.SideBuy, "100", "1", "10")
	eng.SetMark(perpSym, dec.New("100"))
	before, _ := eng.PositionOf("u1", perpSym)
	// No ":<seconds>" suffix → cannot guard idempotency → skip.
	svc.HandleMarkPriceEvent(&eventpb.MarkPriceEvent{Symbol: perpSym, Payload: &eventpb.MarkPriceEvent_Funding{
		Funding: &eventpb.FundingTick{FundingRoundId: "no-round", FundingRate: "0.01", MarkPrice: "100"}}})
	after, _ := eng.PositionOf("u1", perpSym)
	if before.Margin.Cmp(after.Margin) != 0 {
		t.Fatal("malformed funding_round_id should be skipped")
	}
	if jr.count(func(e *eventpb.PerpJournalEvent) bool { return e.GetFunding() != nil }) != 0 {
		t.Fatal("no funding event for malformed round")
	}
}

func TestParseFundingRound(t *testing.T) {
	cases := []struct {
		id   string
		want int64
		ok   bool
	}{
		{"BTC-USDT-PERP:1748505600", 1748505600, true},
		{"X:0", 0, true},
		{"no-colon", 0, false},
		{"trailing:", 0, false},
		{"bad:abc", 0, false},
	}
	for _, c := range cases {
		got, ok := parseFundingRound(c.id)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("parseFundingRound(%q) = (%d,%v), want (%d,%v)", c.id, got, ok, c.want, c.ok)
		}
	}
}
