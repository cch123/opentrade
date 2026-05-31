package perpstate

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func d(s string) dec.Decimal { return dec.New(s) }

func eq(t *testing.T, got dec.Decimal, want string, what string) {
	t.Helper()
	if got.Cmp(d(want)) != 0 {
		t.Fatalf("%s: got %s, want %s", what, got.String(), want)
	}
}

func approx(t *testing.T, got dec.Decimal, want, eps, what string) {
	t.Helper()
	if got.Sub(d(want)).Abs().Cmp(d(eps)) > 0 {
		t.Fatalf("%s: got %s, want ~%s (eps %s)", what, got.String(), want, eps)
	}
}

func longLev10() *Position {
	return &Position{UserID: 1001, Symbol: "BTC-USDT-PERP", Leverage: d("10"), Mode: MarginIsolated,
		Side: 0, Size: zero, Entry: zero, Margin: zero, Realized: zero}
}

func TestApplyFill_OpenAndIncrease(t *testing.T) {
	p := longLev10()
	r := p.ApplyFill(Fill{Side: SideBuy, Price: d("100"), Qty: d("1")})
	eq(t, p.Size, "1", "size after open")
	eq(t, p.Entry, "100", "entry after open")
	eq(t, p.Margin, "10", "margin after open") // 100*1/10
	eq(t, r.MarginAdded, "10", "marginAdded open")
	if p.Side != SideBuy {
		t.Fatalf("side = %v, want buy", p.Side)
	}

	r = p.ApplyFill(Fill{Side: SideBuy, Price: d("110"), Qty: d("1")})
	eq(t, p.Size, "2", "size after increase")
	eq(t, p.Entry, "105", "weighted entry (100*1+110*1)/2")
	eq(t, p.Margin, "21", "margin after increase 10+11")
	eq(t, r.MarginAdded, "11", "marginAdded increase 110/10")
}

func TestApplyFill_ReduceLongRealizes(t *testing.T) {
	p := longLev10()
	p.ApplyFill(Fill{Side: SideBuy, Price: d("100"), Qty: d("2")}) // long 2 @100, margin 20
	r := p.ApplyFill(Fill{Side: SideSell, Price: d("120"), Qty: d("1")})
	eq(t, r.Realized, "20", "realized (120-100)*1")
	eq(t, r.MarginReleased, "10", "released 20*1/2")
	eq(t, p.Size, "1", "size after reduce")
	eq(t, p.Entry, "100", "entry unchanged on reduce")
	eq(t, p.Margin, "10", "margin after reduce")
	eq(t, p.Realized, "20", "cumulative realized")
}

func TestApplyFill_ReduceShortRealizes(t *testing.T) {
	p := longLev10()
	p.ApplyFill(Fill{Side: SideSell, Price: d("100"), Qty: d("2")}) // short 2 @100, margin 20
	r := p.ApplyFill(Fill{Side: SideBuy, Price: d("90"), Qty: d("1")})
	eq(t, r.Realized, "10", "short realized (100-90)*1")
	eq(t, p.Size, "1", "size after reduce")
	if p.Side != SideSell {
		t.Fatalf("still short expected")
	}
}

func TestApplyFill_CloseToFlat(t *testing.T) {
	p := longLev10()
	p.ApplyFill(Fill{Side: SideBuy, Price: d("100"), Qty: d("1")})
	r := p.ApplyFill(Fill{Side: SideSell, Price: d("110"), Qty: d("1")})
	eq(t, r.Realized, "10", "realized on close")
	eq(t, r.MarginReleased, "10", "all margin released")
	if !p.IsFlat() {
		t.Fatalf("should be flat, size=%s", p.Size.String())
	}
	eq(t, p.Entry, "0", "entry reset on flat")
	eq(t, p.Margin, "0", "margin zero on flat")
	if p.Side != 0 {
		t.Fatalf("side should reset to 0 on flat, got %v", p.Side)
	}
}

func TestApplyFill_Flip(t *testing.T) {
	p := longLev10()
	p.ApplyFill(Fill{Side: SideBuy, Price: d("100"), Qty: d("1")}) // long 1 @100 margin 10
	r := p.ApplyFill(Fill{Side: SideSell, Price: d("110"), Qty: d("3")})
	// close 1 (@110): realized (110-100)*1=10, release 10. remaining 2 opens short @110.
	eq(t, r.Realized, "10", "realized on the closed leg")
	eq(t, r.MarginReleased, "10", "released full long margin")
	eq(t, r.MarginAdded, "22", "new short IM 110*2/10")
	eq(t, p.Size, "2", "flipped size")
	eq(t, p.Entry, "110", "flipped entry")
	eq(t, p.Margin, "22", "flipped margin")
	if p.Side != SideSell {
		t.Fatalf("should be short after flip")
	}
}

func TestUnrealizedPnL(t *testing.T) {
	long := &Position{Side: SideBuy, Size: d("2"), Entry: d("100"), Leverage: d("10")}
	eq(t, long.UnrealizedPnL(d("110")), "20", "long uPnL")
	eq(t, long.UnrealizedPnL(d("90")), "-20", "long uPnL down")
	short := &Position{Side: SideSell, Size: d("2"), Entry: d("100"), Leverage: d("10")}
	eq(t, short.UnrealizedPnL(d("90")), "20", "short uPnL")
	flat := &Position{}
	eq(t, flat.UnrealizedPnL(d("100")), "0", "flat uPnL")
}

func TestMarginRatio(t *testing.T) {
	p := &Position{Side: SideBuy, Size: d("1"), Entry: d("100"), Margin: d("10"), Leverage: d("10")}
	eq(t, p.MarginRatio(d("100")), "0.1", "ratio at entry (10+0)/100")
}

func TestLiqAndBankruptcyPrice(t *testing.T) {
	mmr := d("0.005")
	long := &Position{Side: SideBuy, Size: d("1"), Entry: d("100"), Margin: d("10")}
	// (100*1 - 10) / (1*(1-0.005)) = 90/0.995 = 90.45226...
	approx(t, long.LiqPrice(ConstantMMR(mmr)), "90.45226", "0.001", "long liq")
	eq(t, long.BankruptcyPrice(), "90", "long bankruptcy 100-10/1")

	short := &Position{Side: SideSell, Size: d("1"), Entry: d("100"), Margin: d("10")}
	// (100 + 10) / (1+0.005) = 110/1.005 = 109.45273...
	approx(t, short.LiqPrice(ConstantMMR(mmr)), "109.45273", "0.001", "short liq")
	eq(t, short.BankruptcyPrice(), "110", "short bankruptcy 100+10/1")
}

func TestApplyFunding(t *testing.T) {
	rate := d("0.0001")
	long := &Position{Side: SideBuy, Size: d("1"), Entry: d("100"), Margin: d("10")}
	delta := long.ApplyFunding(d("100"), rate) // notional 100 * 0.0001 = 0.01, long pays
	eq(t, delta, "-0.01", "long pays funding")
	eq(t, long.Margin, "9.99", "margin after funding")

	short := &Position{Side: SideSell, Size: d("1"), Entry: d("100"), Margin: d("10")}
	delta = short.ApplyFunding(d("100"), rate)
	eq(t, delta, "0.01", "short receives funding")
	eq(t, short.Margin, "10.01", "short margin after funding")

	// negative rate: long receives.
	long2 := &Position{Side: SideBuy, Size: d("1"), Entry: d("100"), Margin: d("10")}
	delta = long2.ApplyFunding(d("100"), d("-0.0001"))
	eq(t, delta, "0.01", "long receives on negative rate")
}

func TestPool_IsolatedHealthAndLiquidatable(t *testing.T) {
	mmr := d("0.005")
	p := &Position{Symbol: "BTC-USDT-PERP", Side: SideBuy, Size: d("1"), Entry: d("100"), Margin: d("10")}
	pool := Isolated(p)

	h := pool.Eval(map[string]dec.Decimal{"BTC-USDT-PERP": d("100")})
	eq(t, h.Equity, "10", "equity at entry")
	eq(t, h.Notional, "100", "notional at entry")
	eq(t, h.MarginRatio, "0.1", "ratio at entry")
	if pool.Liquidatable(map[string]dec.Decimal{"BTC-USDT-PERP": d("100")}, ConstantMMR(mmr)) {
		t.Fatal("healthy long should not be liquidatable at entry")
	}
	// Drop to 90: equity 10 + (-10) = 0, ratio 0 <= mmr → liquidatable.
	if !pool.Liquidatable(map[string]dec.Decimal{"BTC-USDT-PERP": d("90")}, ConstantMMR(mmr)) {
		t.Fatal("long at bankruptcy mark should be liquidatable")
	}
	// 95: equity 5, notional 95, ratio ~0.0526 > mmr → safe.
	if pool.Liquidatable(map[string]dec.Decimal{"BTC-USDT-PERP": d("95")}, ConstantMMR(mmr)) {
		t.Fatal("long at 95 should still be safe")
	}
}

// TestPool_CrossSeam proves the collateral-pool abstraction generalizes to a
// multi-position pool with a shared drawable (the future cross-margin shape),
// using the exact same Eval/Liquidatable code path as isolated — i.e. adding
// cross is a second constructor, not a rewrite (ADR-0068 §3.1 / invariant #6).
func TestPool_CrossSeam(t *testing.T) {
	a := &Position{Symbol: "BTC-USDT-PERP", Side: SideBuy, Size: d("1"), Entry: d("100"), Margin: d("10")}
	b := &Position{Symbol: "ETH-USDT-PERP", Side: SideSell, Size: d("1"), Entry: d("200"), Margin: d("20")}
	pool := CollateralPool{Drawable: d("50"), Positions: []*Position{a, b}}
	marks := map[string]dec.Decimal{"BTC-USDT-PERP": d("100"), "ETH-USDT-PERP": d("190")}
	// equity = 50 + (10 + 0) + (20 + (200-190)*1) = 50 + 10 + 30 = 90
	// notional = 100 + 190 = 290
	h := pool.Eval(marks)
	eq(t, h.Equity, "90", "cross pool equity")
	eq(t, h.Notional, "290", "cross pool notional")
	if pool.Liquidatable(marks, ConstantMMR(d("0.005"))) {
		t.Fatal("well-collateralized cross pool should be safe")
	}

	// Empty / flat pool is never liquidatable.
	empty := CollateralPool{Drawable: zero}
	if empty.Liquidatable(marks, ConstantMMR(d("0.005"))) {
		t.Fatal("empty pool must not be liquidatable")
	}
}

func TestRiskModel_TierSelectionAndReduceToTarget(t *testing.T) {
	model := NewRiskModel([]RiskTier{
		{TierMaxNotional: d("50"), MaintMarginRatio: d("0.01"), MaxLeverage: d("50")},
		{TierMaxNotional: d("0"), MaintMarginRatio: d("0.05"), MaxLeverage: d("20")},
	}, d("0.005"), d("100"), zero)

	eq(t, model.MMR(d("40")), "0.01", "first tier mmr")
	eq(t, model.MMR(d("60")), "0.05", "open-ended tier mmr")
	eq(t, model.MaxLeverage(d("60")), "20", "tier max leverage")

	p := &Position{Symbol: "BTC-USDT-PERP", Side: SideBuy, Size: d("1"), Entry: d("100"), Margin: d("10")}
	q := ReduceToTarget(Isolated(p), map[string]dec.Decimal{"BTC-USDT-PERP": d("94")},
		ConstantMMR(d("0.05")), d("0.01"))
	if q.Sign() <= 0 || q.Cmp(p.Size) >= 0 {
		t.Fatalf("reduce-to-target should return a partial qty, got %s", q)
	}
}
