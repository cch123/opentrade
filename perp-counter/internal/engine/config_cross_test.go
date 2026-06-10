package engine

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// tierModel: tier1 ≤10k → MMR 0.5% / 100x; tier2 ≤50k → 1% / 50x;
// tier3 open-ended → 2% / 20x.
func tierModel() perpstate.RiskModel {
	return perpstate.NewRiskModel([]perpstate.RiskTier{
		{TierMaxNotional: d("10000"), MaintMarginRatio: d("0.005"), MaxLeverage: d("100"), LiqFeeRate: d("0.0005")},
		{TierMaxNotional: d("50000"), MaintMarginRatio: d("0.01"), MaxLeverage: d("50"), LiqFeeRate: d("0.001")},
		{TierMaxNotional: d("0"), MaintMarginRatio: d("0.02"), MaxLeverage: d("20"), LiqFeeRate: d("0.002")},
	}, d("0.005"), d("100"), d("0.001"))
}

const sym = "BTC-USDT-PERP"

// openPosTight opens a position drawing IM from the EXISTING wallet balance
// (openPos deposits 100k, which masks balance-sensitive assertions).
func openPosTight(e *Engine, user uint64, symbol string, side perpstate.Side, price, qty, lev string) {
	e.Reserve(user, perpstate.InitMargin(d(price), d(qty), d(lev)))
	e.ApplyFill(user, symbol, 0, d(lev), perpstate.Fill{Side: side, Price: d(price), Qty: d(qty)})
}

func newConfiguredEngine() *Engine {
	e := New()
	e.SetRiskModel(tierModel())
	return e
}

// totalCash sums every cash bucket the engine tracks for one user plus the
// symbol insurance fund — the conservation check used across mutations.
func totalCash(e *Engine, user uint64) dec.Decimal {
	w := e.WalletOf(user)
	out := w.Available.Add(w.Reserved).Add(w.CrossReserved)
	if p, ok := e.PositionRaw(user, sym, 0); ok {
		out = out.Add(p.Margin)
	}
	return out
}

func TestAdjustIsolatedMargin_AddRemoveAndGuards(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "1", "10") // margin 10
	before := totalCash(e, 1001)

	out := e.AdjustIsolatedMargin(1001, sym, 0, "op-add", d("50"), zero)
	if !out.Accepted || out.MarginAfter.Cmp(d("60")) != 0 {
		t.Fatalf("add: %+v", out)
	}
	if got := totalCash(e, 1001); got.Cmp(before) != 0 {
		t.Fatalf("cash conservation broken: %s != %s", got, before)
	}

	// Removing down to below the entry-based IM requirement (100/10 = 10) is
	// rejected; removing the added 50 back is fine.
	if out := e.AdjustIsolatedMargin(1001, sym, 0, "op-too-much", d("-55"), zero); out.Accepted {
		t.Fatalf("removal below IM requirement must reject, got %+v", out)
	}
	out = e.AdjustIsolatedMargin(1001, sym, 0, "op-remove", d("-50"), zero)
	if !out.Accepted || out.MarginAfter.Cmp(d("10")) != 0 {
		t.Fatalf("remove: %+v", out)
	}

	// Idempotency: same op id returns the first outcome without re-applying.
	dup := e.AdjustIsolatedMargin(1001, sym, 0, "op-remove", d("-50"), zero)
	if !dup.Accepted || dup.MarginAfter.Cmp(d("10")) != 0 {
		t.Fatalf("duplicate op must replay first outcome: %+v", dup)
	}
	if p, _ := e.PositionOf(1001, sym, 0); p.Margin.Cmp(d("10")) != 0 {
		t.Fatalf("duplicate op re-applied: margin %s", p.Margin)
	}

	// Adding more than free balance rejects.
	if out := e.AdjustIsolatedMargin(1001, sym, 0, "op-over", d("99999"), zero); out.Accepted {
		t.Fatal("add beyond free balance must reject")
	}
}

func TestSetLeverage_ResizesIsolatedMargin(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "1", "10") // margin 10
	before := totalCash(e, 1001)

	// Lowering leverage 10 → 5 doubles the IM requirement: +10 from wallet.
	out := e.SetLeverage(1001, sym, "op-lev5", d("5"), zero)
	if !out.Accepted || out.MarginAfter.Cmp(d("20")) != 0 || out.Leverage.Cmp(d("5")) != 0 {
		t.Fatalf("lower leverage: %+v", out)
	}
	// Raising back to 10 releases the surplus.
	out = e.SetLeverage(1001, sym, "op-lev10", d("10"), zero)
	if !out.Accepted || out.MarginAfter.Cmp(d("10")) != 0 {
		t.Fatalf("raise leverage: %+v", out)
	}
	if got := totalCash(e, 1001); got.Cmp(before) != 0 {
		t.Fatalf("cash conservation broken: %s != %s", got, before)
	}

	// Tier cap: notional 100 → tier1 cap 100x; 150x rejects.
	if out := e.SetLeverage(1001, sym, "op-lev150", d("150"), zero); out.Accepted {
		t.Fatal("leverage above tier cap must reject")
	}
	// Customer cap overrides tier cap downward.
	e.SetCustomerLeverageLimit(1001, "", d("3"), "risk desk", "admin", 1)
	if out := e.SetLeverage(1001, sym, "op-lev5b", d("5"), zero); out.Accepted {
		t.Fatal("leverage above customer cap must reject")
	}
	e.SetCustomerLeverageLimit(1001, "", zero, "clear", "admin", 2)
	if out := e.SetLeverage(1001, sym, "op-lev5c", d("5"), zero); !out.Accepted {
		t.Fatalf("after cap removal 5x should pass: %+v", out)
	}
}

func TestSetRiskID_CapAndIndexShift(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "1", "10")

	if out := e.SetRiskID(1001, sym, "op-bad", 99, zero); out.Accepted {
		t.Fatal("riskID beyond table must reject")
	}
	// extraNotional pushes past tier1 cap (10k).
	if out := e.SetRiskID(1001, sym, "op-cap", 1, d("9950")); out.Accepted {
		t.Fatal("notional above selected tier cap must reject")
	}
	out := e.SetRiskID(1001, sym, "op-t2", 2, zero)
	if !out.Accepted || out.RiskID != 2 {
		t.Fatalf("set riskID 2: %+v", out)
	}
	// Effective MMR is now 1% — the liq index must shift accordingly: equity
	// at mark 90.95 ≈ 0.95, notional 90.95, ratio ~0.0104 > 0.01 (safe);
	// at 90.9 ratio ~0.0099 ≤ 0.01 → liquidatable.
	e.SetMark(sym, d("90.9"))
	if got := e.LiquidatablePositions(sym); len(got) != 1 {
		t.Fatalf("riskID-2 MMR should trigger at 90.9, got %d candidates", len(got))
	}
}

func TestSwitchToCrossAndBack_CashConservation(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "1", "10") // isolated margin 10
	before := totalCash(e, 1001)

	out := e.SwitchToCross(1001, sym, "op-cross", zero)
	if !out.Accepted || out.Mode != perpstate.MarginCross {
		t.Fatalf("to cross: %+v", out)
	}
	p, _ := e.PositionOf(1001, sym, 0)
	if p.Margin.Sign() != 0 || p.Mode != perpstate.MarginCross {
		t.Fatalf("cross position must hold no margin: %+v", p)
	}
	if got := totalCash(e, 1001); got.Cmp(before) != 0 {
		t.Fatalf("cash conservation broken after switch: %s != %s", got, before)
	}
	if users := e.CrossUsersWith(sym); len(users) != 1 || users[0] != 1001 {
		t.Fatalf("cross membership: %v", users)
	}
	// Cross position must leave the isolated liq index.
	e.SetMark(sym, d("1"))
	if got := e.LiquidatablePositions(sym); len(got) != 0 {
		t.Fatalf("cross position leaked into isolated liq index: %v", got)
	}
	e.SetMark(sym, d("100"))

	out = e.SwitchToIsolated(1001, sym, "op-iso", d("15"), zero, zero)
	if !out.Accepted || out.Mode != perpstate.MarginIsolated {
		t.Fatalf("to isolated: %+v", out)
	}
	p, _ = e.PositionOf(1001, sym, 0)
	if p.Margin.Cmp(d("15")) != 0 { // max(requested 15, IM 100/10=10)
		t.Fatalf("isolated margin = %s, want 15", p.Margin)
	}
	if got := totalCash(e, 1001); got.Cmp(before) != 0 {
		t.Fatalf("cash conservation broken after switch back: %s != %s", got, before)
	}
	if users := e.CrossUsersWith(sym); len(users) != 0 {
		t.Fatalf("cross membership not cleared: %v", users)
	}
}

func TestSwitchToCross_RejectsUnsafePool(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("110"))
	e.SetMark(sym, d("100"))
	// 20x position with thin margin: notional 2000, margin 100, free 10.
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "20", "20")
	// Crash the mark: deep loss. Candidate cross pool equity = free + margin
	// + uPnL << IM requirement → reject.
	e.SetMark(sym, d("96"))
	if out := e.SwitchToCross(1001, sym, "op-unsafe", zero); out.Accepted {
		t.Fatalf("unsafe switch must reject: %+v", out)
	}
}

func TestCrossOrderCheck_StackedOrdersConsumeFreeCash(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("250"))
	e.SetMark(sym, d("100"))
	e.SwitchToCross(1001, sym, "op-c", zero) // flat config flip

	// Order IM at 10x on notional 1000 = 100 = its post-fill requirement.
	// equity 250 >= candidate requirement 100 → pass; reserve consumes 100.
	if reason, ok := e.CrossOrderCheck(1001, sym, 0, perpstate.SideBuy, d("100"), d("10"), d("10"), d("100"), zero); !ok {
		t.Fatalf("first order should pass: %s", reason)
	}
	if !e.ReserveCross(1001, d("100")) {
		t.Fatal("reserve")
	}
	// Second identical order: Available 150 >= this order's requirement 100
	// → pass (the first order's future requirement is exactly offset by its
	// excluded reservation). Total committed = 200 <= 250 equity.
	if reason, ok := e.CrossOrderCheck(1001, sym, 0, perpstate.SideBuy, d("100"), d("10"), d("10"), d("100"), zero); !ok {
		t.Fatalf("second order should pass: %s", reason)
	}
	if !e.ReserveCross(1001, d("100")) {
		t.Fatal("reserve second")
	}
	// Third order: Available 50 < im 100 → free cash exhausted (rule #4).
	if _, ok := e.CrossOrderCheck(1001, sym, 0, perpstate.SideBuy, d("100"), d("10"), d("10"), d("100"), zero); ok {
		t.Fatal("third order must fail once free cash is consumed")
	}
}

func TestCrossFundingSettlesInWallet(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "1", "10")
	e.SwitchToCross(1001, sym, "op-c", zero)
	availBefore := e.WalletOf(1001).Available

	results := e.SettleFundingUser(1001, sym, 1000, d("0.01")) // long pays 1
	if len(results) != 1 || results[0].Payment.Cmp(d("-1")) != 0 {
		t.Fatalf("funding: %+v", results)
	}
	w := e.WalletOf(1001)
	if w.Available.Cmp(availBefore.Sub(d("1"))) != 0 {
		t.Fatalf("cross funding must hit free balance: %s", w.Available)
	}
	if p, _ := e.PositionOf(1001, sym, 0); p.Margin.Sign() != 0 {
		t.Fatalf("cross funding leaked into position margin: %s", p.Margin)
	}
}

func TestCrossForceCloseAndDeficitSettle(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("100"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "10", "10") // notional 1000, margin 100, free 0
	e.SwitchToCross(1001, sym, "op-c", zero)

	// Crash: uPnL = -150 → equity = 100 - 150 < 0. Pool is bankrupt.
	e.SetMark(sym, d("85"))
	h, ok := e.CrossPoolHealth(1001)
	if !ok || !h.Liquidatable() {
		t.Fatalf("pool must be liquidatable: %+v ok=%v", h, ok)
	}

	insBefore := e.InsuranceFund(sym)
	res, fee, ok := e.CrossForceClose(1001, sym, 0, d("85"), 9001, d("0.001"))
	if !ok {
		t.Fatal("force close failed")
	}
	if res.Realized.Cmp(d("-150")) != 0 {
		t.Fatalf("realized = %s, want -150", res.Realized)
	}
	if fee.Cmp(d("0.85")) != 0 { // 85*10*0.001
		t.Fatalf("fee = %s", fee)
	}
	// Wallet went negative: 100 - 150 - 0.85.
	if w := e.WalletOf(1001); w.Available.Cmp(d("-50.85")) != 0 {
		t.Fatalf("wallet after close = %s", w.Available)
	}
	covered, ok := e.CrossSettleDeficit(1001, sym)
	if !ok || covered.Cmp(d("50.85")) != 0 {
		t.Fatalf("deficit covered = %s ok=%v", covered, ok)
	}
	if w := e.WalletOf(1001); w.Available.Sign() != 0 {
		t.Fatalf("wallet must be zeroed, got %s", w.Available)
	}
	// Insurance: +fee (0.85) - deficit (50.85) = -50 net.
	if got := e.InsuranceFund(sym); got.Sub(insBefore).Cmp(d("-50")) != 0 {
		t.Fatalf("insurance delta = %s, want -50", got.Sub(insBefore))
	}
	// Backstop got the inventory.
	if bp, ok := e.PositionOf(9001, sym, 0); !ok || bp.Size.Cmp(d("10")) != 0 || bp.Side != perpstate.SideBuy {
		t.Fatalf("backstop inventory: %+v ok=%v", bp, ok)
	}
	// User position gone, membership cleared.
	if users := e.CrossUsersWith(sym); len(users) != 0 {
		t.Fatalf("cross membership not cleared: %v", users)
	}
}

func TestAutoAddMargin_TopsUpBeforeLiquidation(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "1", "10") // margin 10
	if out := e.SetAutoAdd(1001, sym, "op-aa", true, zero); !out.Accepted {
		t.Fatalf("enable auto add: %+v", out)
	}
	if users := e.AutoAddUsersWith(sym); len(users) != 1 {
		t.Fatalf("auto-add registry: %v", users)
	}

	// Healthy: no fire.
	if rs := e.AutoAddMargin(1001, sym, d("0.005"), d("0.01"), zero); len(rs) != 0 {
		t.Fatal("must not fire while healthy")
	}
	// Drop near the MMR line: equity 10-8=2, notional 92, ratio ~0.0217;
	// trigger = 0.005+0.02 = 0.025 → fires up to target 0.005+0.05=0.055:
	// need = 0.055*92 - 2 = 3.06.
	e.SetMark(sym, d("92"))
	rs := e.AutoAddMargin(1001, sym, d("0.02"), d("0.05"), zero)
	if len(rs) == 0 || rs[0].Out.Moved.Cmp(d("3.06")) != 0 {
		t.Fatalf("auto add: %+v", rs)
	}
	if p, _ := e.PositionOf(1001, sym, 0); p.Margin.Cmp(d("13.06")) != 0 {
		t.Fatalf("margin after auto add = %s", p.Margin)
	}

	// AutoAddMax caps the per-event amount. After the first top-up margin is
	// 13.06; at mark 88 equity = 1.06, ratio ≈ 0.012 ≤ trigger 0.025.
	e.SetAutoAdd(1001, sym, "op-aa2", true, d("0.5"))
	e.SetMark(sym, d("88"))
	rs = e.AutoAddMargin(1001, sym, d("0.02"), d("0.05"), zero)
	if len(rs) == 0 || rs[0].Out.Moved.Cmp(d("0.5")) != 0 {
		t.Fatalf("auto add cap: %+v", rs)
	}

	// Bankrupt position: no fire (liquidation path owns it).
	e.SetMark(sym, d("80"))
	if rs := e.AutoAddMargin(1001, sym, d("0.02"), d("0.05"), zero); len(rs) != 0 {
		t.Fatal("must not fire on bankrupt position")
	}
}

func TestWithdrawGate_CrossLossBlocksWithdrawal(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("200"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "10", "10") // notional 1000, IM req 100
	e.SwitchToCross(1001, sym, "op-c", zero)
	// Margin (100) returned to wallet on switch → Available = 200.
	// equity = 200, IM = 100 → withdrawable = 100.
	if e.Withdraw(1001, d("150")) {
		t.Fatal("withdrawal beyond cross IM headroom must reject")
	}
	if !e.Withdraw(1001, d("100")) {
		t.Fatal("withdrawal inside headroom should pass")
	}
	// Loss eats the rest of the headroom: equity = 100 - 50 = 50 < IM 95.
	e.SetMark(sym, d("95"))
	if e.Withdraw(1001, d("10")) {
		t.Fatal("withdrawal must reject once equity is below IM requirement")
	}
}

func TestSnapshotRoundTrip_ADR0074State(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(sym, d("100"))
	openPosTight(e, 1001, sym, perpstate.SideBuy, "100", "1", "10")
	e.SetRiskID(1001, sym, "op-r", 2, zero)
	e.SetAutoAdd(1001, sym, "op-a", true, d("5"))
	e.SetCustomerLeverageLimit(1001, sym, d("25"), "vip", "admin", 42)
	e.ReserveCross(1001, d("7"))
	e.AdjustIsolatedMargin(1001, sym, 0, "op-m", d("3"), zero)

	snap := e.Snapshot()
	restored := New()
	restored.SetRiskModel(tierModel())
	restored.Restore(snap)

	p, ok := restored.PositionRaw(1001, sym, 0)
	if !ok || p.RiskID != 2 || !p.AutoAddMargin || p.AutoAddMax.Cmp(d("5")) != 0 {
		t.Fatalf("position config lost: %+v", p)
	}
	w := restored.WalletOf(1001)
	if w.CrossReserved.Cmp(d("7")) != 0 {
		t.Fatalf("cross reserved lost: %s", w.CrossReserved)
	}
	limits := restored.CustomerLeverageLimits(1001)
	if len(limits) != 1 || limits[0].MaxLeverage.Cmp(d("25")) != 0 || limits[0].UpdatedMs != 42 {
		t.Fatalf("limits lost: %+v", limits)
	}
	// Op cache survives: replaying the margin op must NOT re-apply.
	out := restored.AdjustIsolatedMargin(1001, sym, 0, "op-m", d("3"), zero)
	if !out.Accepted {
		t.Fatalf("cached op must replay accepted: %+v", out)
	}
	if p2, _ := restored.PositionRaw(1001, sym, 0); p2.Margin.Cmp(p.Margin) != 0 {
		t.Fatalf("cached op re-applied after restore: %s != %s", p2.Margin, p.Margin)
	}
	// Membership indexes rebuilt.
	if users := restored.AutoAddUsersWith(sym); len(users) != 1 {
		t.Fatalf("auto-add registry not rebuilt: %v", users)
	}
}
