package engine

// ADR-0077 hedge-mode engine tests: mode switching, dual-leg state, per-leg
// funding/liquidation/ADL, cross+hedge pool membership, config uniformity,
// and the snapshot roundtrip with modes + legs.

import (
	"testing"

	"github.com/xargin/opentrade/pkg/perpstate"
)

const hedgeSym = "BTC-USDT-PERP"

func openLeg(e *Engine, user uint64, idx uint8, side perpstate.Side, price, qty, lev string) {
	e.Reserve(user, perpstate.InitMargin(d(price), d(qty), d(lev)))
	if _, excess := e.ApplyFill(user, hedgeSym, idx, d(lev),
		perpstate.Fill{Side: side, Price: d(price), Qty: d(qty)}); excess.Sign() != 0 {
		panic("unexpected excess in test open")
	}
}

func TestSetPositionMode_FlatOnlyAndIdempotent(t *testing.T) {
	e := New()
	e.Deposit(1001, d("1000"))

	out := e.SetPositionMode(1001, hedgeSym, "op1", perpstate.PositionHedge)
	if !out.Accepted || out.PosMode != perpstate.PositionHedge {
		t.Fatalf("flat switch should pass: %+v", out)
	}
	if e.PositionModeOf(1001, hedgeSym) != perpstate.PositionHedge {
		t.Fatal("mode not stored")
	}
	// Idempotent same-target.
	out = e.SetPositionMode(1001, hedgeSym, "op2", perpstate.PositionHedge)
	if !out.Accepted {
		t.Fatalf("same-mode switch should be a no-op accept: %+v", out)
	}
	// Open a leg → switching back must reject.
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "1", "10")
	out = e.SetPositionMode(1001, hedgeSym, "op3", perpstate.PositionOneWay)
	if out.Accepted || out.Reason != "position_not_flat" {
		t.Fatalf("non-flat switch must reject: %+v", out)
	}
	// op cache replays the first outcome.
	cached := e.SetPositionMode(1001, hedgeSym, "op1", perpstate.PositionOneWay)
	if !cached.Accepted || cached.PosMode != perpstate.PositionHedge {
		t.Fatalf("client_op_id replay should return the first outcome: %+v", cached)
	}
}

func TestHedge_DualLegsIndependent(t *testing.T) {
	e := New()
	e.Deposit(1001, d("1000"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)

	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "2", "10")   // IM 20
	openLeg(e, 1001, perpstate.IdxShort, perpstate.SideSell, "100", "1", "10") // IM 10

	long, ok := e.PositionOf(1001, hedgeSym, perpstate.IdxLong)
	if !ok || long.Side != perpstate.SideBuy {
		t.Fatalf("long leg missing: %+v", long)
	}
	short, ok := e.PositionOf(1001, hedgeSym, perpstate.IdxShort)
	if !ok || short.Side != perpstate.SideSell {
		t.Fatalf("short leg missing: %+v", short)
	}
	eq(t, long.Margin, "20", "long leg margin")
	eq(t, short.Margin, "10", "short leg margin")

	// Reducing the long leg leaves the short leg untouched.
	if _, excess := e.ApplyFill(1001, hedgeSym, perpstate.IdxLong, d("10"),
		perpstate.Fill{Side: perpstate.SideSell, Price: d("110"), Qty: d("1")}); excess.Sign() != 0 {
		t.Fatal("no excess expected")
	}
	long, _ = e.PositionOf(1001, hedgeSym, perpstate.IdxLong)
	short, _ = e.PositionOf(1001, hedgeSym, perpstate.IdxShort)
	eq(t, long.Size, "1", "long reduced")
	eq(t, short.Size, "1", "short untouched")

	if got := len(e.PositionsOf(1001)); got != 2 {
		t.Fatalf("want 2 legs, got %d", got)
	}
}

func TestHedge_FundingBothLegsOneStep(t *testing.T) {
	e := New()
	e.Deposit(1001, d("1000"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "10", "10")
	openLeg(e, 1001, perpstate.IdxShort, perpstate.SideSell, "100", "8", "10")
	e.SetMark(hedgeSym, d("100"))

	results := e.SettleFundingUser(1001, hedgeSym, 42, d("0.001"))
	if len(results) != 2 {
		t.Fatalf("want one result per leg, got %d", len(results))
	}
	eq(t, results[0].Payment, "-1", "long pays 1000*0.001")
	eq(t, results[1].Payment, "0.8", "short receives 800*0.001")
	if results[0].Position.PositionIdx != perpstate.IdxLong ||
		results[1].Position.PositionIdx != perpstate.IdxShort {
		t.Fatalf("results must be idx-ordered: %+v", results)
	}
	// Replay guard per leg: same round is a no-op.
	if again := e.SettleFundingUser(1001, hedgeSym, 42, d("0.001")); len(again) != 0 {
		t.Fatalf("round replay must settle nothing, got %d", len(again))
	}
}

// New legs inherit the sibling's config so margin mode / leverage / risk_id
// stay uniform per symbol regardless of creation order (ADR-0077 §7).
func TestHedge_NewLegInheritsSiblingConfig(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("100000"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "1", "10")
	if out := e.SetRiskID(1001, hedgeSym, "", 2, zero); !out.Accepted {
		t.Fatalf("set risk id: %+v", out)
	}

	// Creating the short leg afterwards must inherit leverage + risk id.
	openLeg(e, 1001, hedgeSymShortIdx, perpstate.SideSell, "100", "1", "10")
	short, _ := e.PositionRaw(1001, hedgeSym, perpstate.IdxShort)
	if short.RiskID != 2 {
		t.Fatalf("short leg should inherit risk_id 2, got %d", short.RiskID)
	}
	eq(t, short.Leverage, "10", "short leg inherits leverage")
}

const hedgeSymShortIdx = perpstate.IdxShort

// SetLeverage in hedge mode resizes BOTH legs atomically.
func TestHedge_SetLeverageBothLegs(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(hedgeSym, d("100"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "1", "10")   // IM 10
	openLeg(e, 1001, perpstate.IdxShort, perpstate.SideSell, "100", "1", "10") // IM 10

	out := e.SetLeverage(1001, hedgeSym, "op", d("5"), zero) // target IM 20 per leg
	if !out.Accepted {
		t.Fatalf("set leverage: %+v", out)
	}
	if len(out.LegMoves) != 2 {
		t.Fatalf("want per-leg moves, got %+v", out.LegMoves)
	}
	long, _ := e.PositionRaw(1001, hedgeSym, perpstate.IdxLong)
	short, _ := e.PositionRaw(1001, hedgeSym, perpstate.IdxShort)
	eq(t, long.Margin, "20", "long resized")
	eq(t, short.Margin, "20", "short resized")
	eq(t, long.Leverage, "5", "long leverage")
	eq(t, short.Leverage, "5", "short leverage")
}

// SwitchToCross in hedge mode flips both legs atomically and releases both
// margins; the pool admission evaluates both candidate legs GROSS (the same
// symbol's sibling leg is replaced by its candidate, never dropped).
func TestHedge_SwitchToCrossBothLegs(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(hedgeSym, d("100"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "1", "10")
	openLeg(e, 1001, perpstate.IdxShort, perpstate.SideSell, "100", "1", "10")
	availBefore := e.WalletOf(1001).Available

	out := e.SwitchToCross(1001, hedgeSym, "op", zero)
	if !out.Accepted {
		t.Fatalf("switch to cross: %+v", out)
	}
	eq(t, out.Moved, "20", "both leg margins released")
	eq(t, e.WalletOf(1001).Available, availBefore.Add(d("20")).String(), "cash back to wallet")
	for _, idx := range []uint8{perpstate.IdxLong, perpstate.IdxShort} {
		p, _ := e.PositionRaw(1001, hedgeSym, idx)
		if p.Mode != perpstate.MarginCross {
			t.Fatalf("leg %d not cross", idx)
		}
		eq(t, p.Margin, "0", "cross leg holds no margin bucket")
	}
	// Both legs are pool members: gross requirement reflects 2 legs.
	h, ok := e.CrossPoolHealth(1001)
	if !ok {
		t.Fatal("pool should exist")
	}
	// tier1 MMR 0.5% on 100 notional per leg → 0.5+0.5 = 1 maintenance.
	eq(t, h.MaintenanceRequirement, "1", "gross maintenance over both legs (no netting)")
}

// target_margin is a single-position concept; hedge mode rejects it.
func TestHedge_SwitchToIsolatedRejectsTargetMargin(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetMark(hedgeSym, d("100"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "1", "10")
	if out := e.SwitchToCross(1001, hedgeSym, "", zero); !out.Accepted {
		t.Fatalf("to cross: %+v", out)
	}
	out := e.SwitchToIsolated(1001, hedgeSym, "", d("50"), zero, zero)
	if out.Accepted || out.Reason != "target_margin_unsupported_in_hedge" {
		t.Fatalf("want target_margin reject: %+v", out)
	}
	// Without target_margin both legs come back isolated.
	out = e.SwitchToIsolated(1001, hedgeSym, "", zero, zero, zero)
	if !out.Accepted {
		t.Fatalf("to isolated: %+v", out)
	}
	long, _ := e.PositionRaw(1001, hedgeSym, perpstate.IdxLong)
	if long.Mode != perpstate.MarginIsolated || long.Margin.Sign() <= 0 {
		t.Fatalf("long leg should be isolated with margin: %+v", long)
	}
}

// Per-leg liquidation: only the breached leg is a candidate, keyed by idx.
func TestHedge_LiquidationPerLeg(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "1", "10")   // liq ≈ 90.x
	openLeg(e, 1001, perpstate.IdxShort, perpstate.SideSell, "100", "1", "10") // liq ≈ 109.x

	e.SetMark(hedgeSym, d("90")) // crashes through the long leg's liq price only
	cands := e.LiquidatablePositions(hedgeSym)
	if len(cands) != 1 {
		t.Fatalf("want exactly the long leg, got %+v", cands)
	}
	if cands[0].PositionIdx != perpstate.IdxLong || cands[0].Side != perpstate.SideBuy {
		t.Fatalf("wrong leg flagged: %+v", cands[0])
	}
	// The sequencer re-check resolves the same leg by idx.
	if _, ok := e.LiquidationCheck(1001, hedgeSym, perpstate.IdxLong); !ok {
		t.Fatal("re-check should confirm the long leg")
	}
	if _, ok := e.LiquidationCheck(1001, hedgeSym, perpstate.IdxShort); ok {
		t.Fatal("short leg must not be liquidatable at mark 90")
	}
}

// ADL task targets one leg; a wrong idx must not touch the sibling.
func TestHedge_AdlGuardedByIdx(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "1", "10")
	e.SetMark(hedgeSym, d("120")) // long leg profitable

	cands := e.SelectAnyAdlCandidates(hedgeSym, d("110"), 0)
	if len(cands) != 1 || cands[0].PositionIdx != perpstate.IdxLong {
		t.Fatalf("want the long leg as candidate: %+v", cands)
	}
	c := cands[0]
	// Wrong leg: short leg is flat/absent → not applied.
	if _, _, applied := e.ApplyAdlCloseGuarded(1001, hedgeSym, perpstate.IdxShort,
		c.Size, d("110"), c.Side, c.LastMatchSeq, c.PositionVersion, 1, true); applied {
		t.Fatal("wrong-leg task must not apply")
	}
	// Right leg applies.
	if _, factQty, applied := e.ApplyAdlCloseGuarded(1001, hedgeSym, perpstate.IdxLong,
		c.Size, d("110"), c.Side, c.LastMatchSeq, c.PositionVersion, 1, true); !applied || factQty.Cmp(d("1")) != 0 {
		t.Fatalf("right-leg task should apply fully, applied=%v fact=%s", applied, factQty)
	}
	if _, ok := e.PositionOf(1001, hedgeSym, perpstate.IdxLong); ok {
		t.Fatal("long leg should be closed by ADL")
	}
}

func TestHedge_SnapshotRoundtrip(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("1000"))
	e.SetPositionMode(1001, hedgeSym, "op-mode", perpstate.PositionHedge)
	openLeg(e, 1001, perpstate.IdxLong, perpstate.SideBuy, "100", "2", "10")
	openLeg(e, 1001, perpstate.IdxShort, perpstate.SideSell, "100", "1", "10")
	e.SetMark(hedgeSym, d("100"))
	e.SettleFundingUser(1001, hedgeSym, 7, d("0.001"))
	// A multi-leg op outcome with LegMoves must survive the roundtrip too.
	if out := e.SetLeverage(1001, hedgeSym, "op-lev", d("5"), zero); !out.Accepted {
		t.Fatalf("set leverage: %+v", out)
	}

	snap := e.Snapshot()
	if len(snap.PosModes) != 1 || snap.PosModes[0].Mode != uint8(perpstate.PositionHedge) {
		t.Fatalf("mode row missing: %+v", snap.PosModes)
	}

	restored := New()
	restored.SetRiskModel(tierModel())
	restored.Restore(snap)

	if restored.PositionModeOf(1001, hedgeSym) != perpstate.PositionHedge {
		t.Fatal("mode lost in roundtrip")
	}
	for _, idx := range []uint8{perpstate.IdxLong, perpstate.IdxShort} {
		orig, _ := e.PositionRaw(1001, hedgeSym, idx)
		got, ok := restored.PositionRaw(1001, hedgeSym, idx)
		if !ok {
			t.Fatalf("leg %d lost", idx)
		}
		if got.PositionIdx != idx || got.Size.Cmp(orig.Size) != 0 ||
			got.Margin.Cmp(orig.Margin) != 0 || got.FundingRoundSeen != orig.FundingRoundSeen {
			t.Fatalf("leg %d mismatch: got %+v want %+v", idx, got, orig)
		}
	}
	// Cached op (incl. LegMoves) replays identically after restore.
	out := restored.SetLeverage(1001, hedgeSym, "op-lev", d("99"), zero)
	if !out.Accepted || out.Leverage.Cmp(d("5")) != 0 || len(out.LegMoves) != 2 {
		t.Fatalf("op cache roundtrip broken: %+v", out)
	}
}

// CrossOrderCheck must keep the same symbol's OTHER leg in the candidate pool
// (exclusion is by (symbol, idx), not symbol — the ADR-0077 §4 gross rule).
func TestHedge_CrossOrderCheckKeepsSiblingLeg(t *testing.T) {
	e := newConfiguredEngine()
	e.Deposit(1001, d("23"))
	e.SetMark(hedgeSym, d("100"))
	e.SetPositionMode(1001, hedgeSym, "", perpstate.PositionHedge)
	// Live cross short leg: notional 100, IM at lev 10 = 10.
	openLeg(e, 1001, perpstate.IdxShort, perpstate.SideSell, "100", "1", "10")
	if out := e.SwitchToCross(1001, hedgeSym, "", zero); !out.Accepted {
		t.Fatalf("to cross: %+v", out)
	}
	// Wallet now 23 (margin came back: 13 + 10). A new long-leg order with
	// IM 10 leaves candidate pool needing IM 10 (long) + 10 (short) = 20 vs
	// drawable 23 → passes ONLY because the sibling counts; sanity-check the
	// tighter case below.
	if reason, ok := e.CrossOrderCheck(1001, hedgeSym, perpstate.IdxLong,
		perpstate.SideBuy, d("100"), d("1"), d("10"), d("10"), zero); !ok {
		t.Fatalf("order should pass with both legs counted: %s", reason)
	}
	// Withdraw 5 → drawable 18 < 20 gross requirement. If the sibling leg
	// were wrongly dropped (excluded by symbol), requirement would be 10 and
	// this would pass — the rejection proves the gross rule.
	if !e.Withdraw(1001, d("5")) {
		t.Fatal("withdraw setup failed")
	}
	if _, ok := e.CrossOrderCheck(1001, hedgeSym, perpstate.IdxLong,
		perpstate.SideBuy, d("100"), d("1"), d("10"), d("10"), zero); ok {
		t.Fatal("order must reject: sibling leg's requirement may not be dropped")
	}
}
