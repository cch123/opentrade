package perpstate

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func tierTable() RiskModel {
	// tier1: <=10k notional, MMR 0.5%, 100x; tier2: <=50k, 1%, 50x;
	// tier3: open-ended, 2%, 20x.
	return NewRiskModel([]RiskTier{
		{TierMaxNotional: d("10000"), MaintMarginRatio: d("0.005"), MaxLeverage: d("100"), LiqFeeRate: d("0.0005")},
		{TierMaxNotional: d("50000"), MaintMarginRatio: d("0.01"), MaxLeverage: d("50"), LiqFeeRate: d("0.001")},
		{TierMaxNotional: d("0"), MaintMarginRatio: d("0.02"), MaxLeverage: d("20"), LiqFeeRate: d("0.002")},
	}, d("0.005"), d("100"), d("0.0005"))
}

func TestEffectiveTierIndex(t *testing.T) {
	m := tierTable()
	cases := []struct {
		notional string
		riskID   uint32
		want     int32
	}{
		{"5000", 0, 1},  // auto lowest covering tier
		{"20000", 0, 2}, // auto second tier
		{"99999", 0, 3}, // auto open-ended tail
		{"5000", 2, 2},  // user selected higher tier wins
		{"20000", 1, 2}, // auto tier higher than selection wins (conservative max)
		{"5000", 99, 3}, // out-of-range riskID clamps to last tier
		{"99999", 1, 3}, // auto tail beats low selection
	}
	for _, c := range cases {
		if got := m.EffectiveTierIndex(d(c.notional), c.riskID); got != c.want {
			t.Fatalf("EffectiveTierIndex(%s, %d) = %d, want %d", c.notional, c.riskID, got, c.want)
		}
	}
}

func TestEffectiveMMRAndLeverage(t *testing.T) {
	m := tierTable()
	if got := m.EffectiveMMR(d("5000"), 0); got.Cmp(d("0.005")) != 0 {
		t.Fatalf("auto tier1 MMR = %s", got)
	}
	if got := m.EffectiveMMR(d("5000"), 2); got.Cmp(d("0.01")) != 0 {
		t.Fatalf("selected tier2 MMR = %s", got)
	}
	if got := m.EffectiveMaxLeverage(d("5000"), 2); got.Cmp(d("50")) != 0 {
		t.Fatalf("selected tier2 max leverage = %s", got)
	}
	if got := m.MaxNotionalFor(0); got.Sign() != 0 {
		t.Fatalf("auto cap should be open-ended last tier, got %s", got)
	}
	if got := m.MaxNotionalFor(1); got.Cmp(d("10000")) != 0 {
		t.Fatalf("tier1 cap = %s", got)
	}
	if got := m.MaxNotionalFor(99); got.Sign() != 0 {
		t.Fatalf("clamped cap should be last tier (open-ended), got %s", got)
	}
}

func TestStandardRiskEvalIsolatedMatchesScalarBoundary(t *testing.T) {
	m := tierTable()
	p := &Position{UserID: 1, Symbol: "BTC-USDT-PERP", Side: SideBuy,
		Size: d("1"), Entry: d("10000"), Margin: d("100"), Leverage: d("100"), Mode: MarginIsolated}
	marks := map[string]dec.Decimal{"BTC-USDT-PERP": d("9920")}
	h := StandardRisk{Model: m}.Eval(Isolated(p), marks)
	// equity = 100 + (9920-10000)*1 = 20; notional = 9920;
	// MM = 9920*0.005 = 49.6 → liquidatable.
	if h.Equity.Cmp(d("20")) != 0 || h.MaintenanceRequirement.Cmp(d("49.6")) != 0 {
		t.Fatalf("health = %+v", h)
	}
	if !h.Liquidatable() {
		t.Fatal("expected liquidatable")
	}
	// Old scalar pool boundary must agree at the single-position degenerate case.
	if !Isolated(p).Liquidatable(marks, m.MMRFunc()) {
		t.Fatal("scalar pool boundary disagrees with StandardRisk")
	}
}

func TestStandardRiskEvalCrossSumsPerPositionRequirements(t *testing.T) {
	m := tierTable()
	long := &Position{UserID: 1, Symbol: "BTC-USDT-PERP", Side: SideBuy,
		Size: d("1"), Entry: d("9000"), Leverage: d("10"), Mode: MarginCross, Margin: zero}
	short := &Position{UserID: 1, Symbol: "ETH-USDT-PERP", Side: SideSell,
		Size: d("10"), Entry: d("2100"), Leverage: d("10"), Mode: MarginCross, Margin: zero, RiskID: 2}
	marks := map[string]dec.Decimal{"BTC-USDT-PERP": d("9500"), "ETH-USDT-PERP": d("2000")}
	h := StandardRisk{Model: m}.Eval(Cross(d("1000"), []*Position{long, short}), marks)

	// equity = 1000 + (9500-9000)*1 + (2100-2000)*10 = 2500
	if h.Equity.Cmp(d("2500")) != 0 {
		t.Fatalf("equity = %s", h.Equity)
	}
	// notional = 9500 + 20000 = 29500
	if h.Notional.Cmp(d("29500")) != 0 {
		t.Fatalf("notional = %s", h.Notional)
	}
	// MM = 9500*0.005 (tier1 auto) + 20000*0.01 (riskID 2) = 47.5 + 200 = 247.5
	if h.MaintenanceRequirement.Cmp(d("247.5")) != 0 {
		t.Fatalf("mm requirement = %s", h.MaintenanceRequirement)
	}
	// IM = 9500/10 + 20000/10 = 2950 (10x within both tier caps)
	if h.InitialRequirement.Cmp(d("2950")) != 0 {
		t.Fatalf("im requirement = %s", h.InitialRequirement)
	}
	if h.Liquidatable() {
		t.Fatal("healthy pool flagged liquidatable")
	}
	// Above maintenance (247.5) but below initial (2950): holdable, but a new
	// increase order must be rejected.
	if h.MeetsInitial(zero) {
		t.Fatal("equity 2500 must not meet IM requirement 2950")
	}
}

func TestStandardRiskInitialRequirementClampsLeverage(t *testing.T) {
	m := tierTable()
	// 100x configured but notional in tier3 (cap 20x) → IM uses 20x.
	p := &Position{UserID: 1, Symbol: "BTC-USDT-PERP", Side: SideBuy,
		Size: d("10"), Entry: d("10000"), Leverage: d("100"), Mode: MarginCross}
	marks := map[string]dec.Decimal{"BTC-USDT-PERP": d("10000")}
	h := StandardRisk{Model: m}.Eval(Cross(zero, []*Position{p}), marks)
	if h.InitialRequirement.Cmp(d("5000")) != 0 { // 100000/20
		t.Fatalf("im requirement = %s, want 5000", h.InitialRequirement)
	}
}

func TestCrossFillKeepsMarginZero(t *testing.T) {
	p := &Position{UserID: 1, Symbol: "BTC-USDT-PERP", Mode: MarginCross, Leverage: d("10")}

	res := p.ApplyFill(Fill{Side: SideBuy, Price: d("10000"), Qty: d("2")})
	if res.MarginAdded.Sign() != 0 || p.Margin.Sign() != 0 {
		t.Fatalf("cross open moved margin: res=%+v margin=%s", res, p.Margin)
	}
	if p.Size.Cmp(d("2")) != 0 || p.Side != SideBuy || p.Entry.Cmp(d("10000")) != 0 {
		t.Fatalf("geometry wrong: %+v", p)
	}

	// Reduce half at a profit: realized reported, no margin released.
	res = p.ApplyFill(Fill{Side: SideSell, Price: d("11000"), Qty: d("1")})
	if res.MarginReleased.Sign() != 0 || p.Margin.Sign() != 0 {
		t.Fatalf("cross reduce moved margin: res=%+v margin=%s", res, p.Margin)
	}
	if res.Realized.Cmp(d("1000")) != 0 {
		t.Fatalf("realized = %s", res.Realized)
	}

	// Flip through flat: new leg also takes no margin.
	res = p.ApplyFill(Fill{Side: SideSell, Price: d("10500"), Qty: d("3")})
	if res.MarginAdded.Sign() != 0 || p.Margin.Sign() != 0 {
		t.Fatalf("cross flip moved margin: res=%+v margin=%s", res, p.Margin)
	}
	if p.Side != SideSell || p.Size.Cmp(d("2")) != 0 || p.Entry.Cmp(d("10500")) != 0 {
		t.Fatalf("flip geometry wrong: %+v", p)
	}
}

func TestIsolatedFillStillMovesMargin(t *testing.T) {
	p := &Position{UserID: 1, Symbol: "BTC-USDT-PERP", Mode: MarginIsolated, Leverage: d("10")}
	res := p.ApplyFill(Fill{Side: SideBuy, Price: d("10000"), Qty: d("1")})
	if res.MarginAdded.Cmp(d("1000")) != 0 || p.Margin.Cmp(d("1000")) != 0 {
		t.Fatalf("isolated open: res=%+v margin=%s", res, p.Margin)
	}
}

func TestCrossFundingDoesNotTouchMargin(t *testing.T) {
	p := &Position{UserID: 1, Symbol: "BTC-USDT-PERP", Mode: MarginCross,
		Side: SideBuy, Size: d("1"), Entry: d("10000"), Leverage: d("10")}
	delta := p.ApplyFunding(d("10000"), d("0.0001"))
	if delta.Cmp(d("-1")) != 0 { // long pays 10000*0.0001
		t.Fatalf("delta = %s", delta)
	}
	if p.Margin.Sign() != 0 {
		t.Fatalf("cross funding moved margin: %s", p.Margin)
	}
	if p.Realized.Cmp(d("-1")) != 0 {
		t.Fatalf("realized = %s", p.Realized)
	}
}

func TestCrossClosePlanRanksLargestLossFirst(t *testing.T) {
	deepLoss := &Position{Symbol: "A-USDT-PERP", Side: SideBuy, Size: d("1"), Entry: d("1000"), Mode: MarginCross}
	smallLoss := &Position{Symbol: "B-USDT-PERP", Side: SideBuy, Size: d("1"), Entry: d("110"), Mode: MarginCross}
	profit := &Position{Symbol: "C-USDT-PERP", Side: SideSell, Size: d("1"), Entry: d("100"), Mode: MarginCross}
	flat := &Position{Symbol: "D-USDT-PERP", Mode: MarginCross}
	marks := map[string]dec.Decimal{
		"A-USDT-PERP": d("900"), // uPnL -100
		"B-USDT-PERP": d("100"), // uPnL -10
		"C-USDT-PERP": d("90"),  // uPnL +10
	}
	plan := CrossClosePlan(Cross(zero, []*Position{profit, flat, smallLoss, deepLoss}), marks)
	if len(plan) != 3 {
		t.Fatalf("plan size = %d", len(plan))
	}
	if plan[0] != deepLoss || plan[1] != smallLoss || plan[2] != profit {
		t.Fatalf("plan order wrong: %s %s %s", plan[0].Symbol, plan[1].Symbol, plan[2].Symbol)
	}
}
