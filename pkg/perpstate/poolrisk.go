package perpstate

import (
	"sort"

	"github.com/xargin/opentrade/pkg/dec"
)

// poolrisk.go is the ADR-0074 §3 risk-model seam. CollateralPool stays the
// membership boundary (which positions + which drawable cash); PoolRiskModel
// owns the requirement formulas. Standard margin sums per-position tier
// requirements; a future portfolio-margin model replaces only this interface
// implementation (risk_model=STANDARD|PORTFOLIO), not the call sites.

// PoolHealth is the evaluated state of a pool under a risk model. Equity and
// Notional match CollateralPool.Eval; the requirement fields are what
// admission (initial) and liquidation (maintenance) key off. MarginRatio is
// retained for display compatibility — liquidation no longer compares the
// ratio to a scalar MMR, it compares Equity to MaintenanceRequirement.
type PoolHealth struct {
	Equity                 dec.Decimal
	Notional               dec.Decimal
	InitialRequirement     dec.Decimal
	MaintenanceRequirement dec.Decimal
	MarginRatio            dec.Decimal // Equity / Notional; zero when Notional == 0
}

// Liquidatable is the pool-level trigger: equity no longer covers the
// maintenance requirement (ADR-0074 §3). An empty pool is never liquidatable.
func (h PoolHealth) Liquidatable() bool {
	return h.Notional.Sign() > 0 && h.Equity.Cmp(h.MaintenanceRequirement) <= 0
}

// MeetsInitial reports whether equity covers the initial requirement plus
// buffer — the admission / mode-switch safety line (ADR-0074 §4/§5).
func (h PoolHealth) MeetsInitial(buffer dec.Decimal) bool {
	return h.Equity.Cmp(h.InitialRequirement.Add(buffer)) >= 0
}

// PoolRiskModel evaluates a pool's margin requirements (ADR-0074 §3).
type PoolRiskModel interface {
	Eval(pool CollateralPool, marks map[string]dec.Decimal) PoolHealth
}

// StandardRisk is the standard (non-portfolio) requirement model:
//
//	InitialRequirement     = Σ position_notional / effective_leverage
//	MaintenanceRequirement = Σ position_notional × effective_tier.MMR
//
// effective_leverage = min(position.Leverage, effective_tier.MaxLeverage);
// the effective tier is max(auto-by-notional, position.RiskID) — per
// position, so a multi-symbol cross pool sums per-symbol tier requirements
// rather than resolving one scalar MMR from the pool total (ADR-0074 §3).
type StandardRisk struct {
	Model RiskModel
}

// Eval computes pool health under standard margin rules. marks maps
// symbol → mark; a missing mark is treated as zero, matching
// CollateralPool.Eval (callers supply marks for every held symbol).
func (s StandardRisk) Eval(cp CollateralPool, marks map[string]dec.Decimal) PoolHealth {
	h := PoolHealth{
		Equity: cp.Drawable, Notional: zero,
		InitialRequirement: zero, MaintenanceRequirement: zero, MarginRatio: zero,
	}
	for _, p := range cp.Positions {
		if p == nil || p.IsFlat() {
			continue
		}
		mark := marks[p.Symbol]
		h.Equity = h.Equity.Add(p.Margin).Add(p.UnrealizedPnL(mark))
		n := p.Notional(mark)
		h.Notional = h.Notional.Add(n)
		h.MaintenanceRequirement = h.MaintenanceRequirement.Add(n.Mul(s.Model.EffectiveMMR(n, p.RiskID)))
		h.InitialRequirement = h.InitialRequirement.Add(s.initialRequirement(p, n))
	}
	if h.Notional.Sign() > 0 {
		h.MarginRatio = h.Equity.Div(h.Notional)
	}
	return h
}

// initialRequirement is one position's IM requirement at notional n. The
// effective leverage is the position's configured leverage clamped by the
// effective tier's cap; a missing/zero leverage degrades to 1x (full
// notional), the conservative floor.
func (s StandardRisk) initialRequirement(p *Position, n dec.Decimal) dec.Decimal {
	lev := p.Leverage
	if levCap := s.Model.EffectiveMaxLeverage(n, p.RiskID); levCap.Sign() > 0 && (lev.Sign() <= 0 || lev.Cmp(levCap) > 0) {
		lev = levCap
	}
	if lev.Sign() <= 0 {
		return n
	}
	return n.Div(lev)
}

// CrossClosePlan orders a cross pool's positions for forced reduction
// (ADR-0074 §4 rule #5 / open-question decision: v1 ranks by largest
// unrealized loss first, so each close removes the most distress per step;
// ties break by symbol for determinism). Flat entries are skipped. The
// caller closes entries in order, re-evaluating pool health after each, and
// stops as soon as the pool is healthy again.
func CrossClosePlan(cp CollateralPool, marks map[string]dec.Decimal) []*Position {
	out := make([]*Position, 0, len(cp.Positions))
	for _, p := range cp.Positions {
		if p == nil || p.IsFlat() {
			continue
		}
		out = append(out, p)
	}
	sort.SliceStable(out, func(i, j int) bool {
		ui := out[i].UnrealizedPnL(marks[out[i].Symbol])
		uj := out[j].UnrealizedPnL(marks[out[j].Symbol])
		if c := ui.Cmp(uj); c != 0 {
			return c < 0 // most negative (largest loss) first
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}
