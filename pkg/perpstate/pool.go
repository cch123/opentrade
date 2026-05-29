package perpstate

import "github.com/xargin/opentrade/pkg/dec"

// CollateralPool is the unit over which equity, health, and the liquidation
// trigger are computed (ADR-0068 §3.1) — the seam that keeps isolated→cross
// additive.
//
//	isolated (MVP) : one pool per position; Drawable = 0, Positions = {p}.
//	cross (future) : one pool per account; Drawable = wallet available,
//	                 Positions = all the account's cross positions.
//
// ADR-0068 invariant #6: equity / margin_ratio / the liquidation decision
// MUST go through this type. No caller may inline a per-position
// margin_ratio check, or adding cross later becomes a repo-wide rewrite
// instead of a second CollateralPool constructor.
type CollateralPool struct {
	Drawable  dec.Decimal // wallet equity the pool may draw (isolated: 0)
	Positions []*Position
}

// Isolated builds the degenerate one-position pool used by the MVP.
func Isolated(p *Position) CollateralPool {
	return CollateralPool{Drawable: zero, Positions: []*Position{p}}
}

// Health is the evaluated state of a pool at a set of mark prices.
type Health struct {
	Equity      dec.Decimal // Drawable + Σ(margin + unrealized)
	Notional    dec.Decimal // Σ |size|*mark
	MarginRatio dec.Decimal // Equity / Notional; zero when Notional == 0
}

// Eval computes pool health. marks maps symbol → mark price; a missing mark
// is treated as zero (callers must supply marks for every held symbol —
// perp-counter does, fed by the markprice topic).
func (cp CollateralPool) Eval(marks map[string]dec.Decimal) Health {
	equity := cp.Drawable
	notional := zero
	for _, p := range cp.Positions {
		mark := marks[p.Symbol]
		equity = equity.Add(p.Margin).Add(p.UnrealizedPnL(mark))
		notional = notional.Add(p.Notional(mark))
	}
	h := Health{Equity: equity, Notional: notional, MarginRatio: zero}
	if notional.Sign() > 0 {
		h.MarginRatio = equity.Div(notional)
	}
	return h
}

// Liquidatable reports whether the pool breaches the maintenance margin rate
// (margin_ratio <= mmr). The single source of the liquidation trigger:
// isolated calls it with a one-position pool, cross (future) with the
// account pool. An empty/flat pool (zero notional) is never liquidatable.
func (cp CollateralPool) Liquidatable(marks map[string]dec.Decimal, mmr dec.Decimal) bool {
	h := cp.Eval(marks)
	if h.Notional.Sign() == 0 {
		return false
	}
	return h.MarginRatio.Cmp(mmr) <= 0
}
