package perpstate

import "github.com/xargin/opentrade/pkg/dec"

// initMargin = notional / leverage = price*qty/leverage. Caller guarantees
// leverage > 0 (validated at order time against SymbolConfig.max_leverage).
func initMargin(price, qty, leverage dec.Decimal) dec.Decimal {
	return price.Mul(qty).Div(leverage)
}

// InitMargin is the exported initial-margin helper perp-counter uses to size
// the pre-trade reservation (ADR-0068 §4) before a fill exists.
func InitMargin(price, qty, leverage dec.Decimal) dec.Decimal {
	return initMargin(price, qty, leverage)
}

// MarginRatio per ADR-0068 §8: (margin + unrealized) / notional, evaluated
// at mark. Zero when flat. This is the per-position view; the pool view
// (pool.go) is what liquidation actually keys off so isolated→cross stays
// additive — see invariant #6.
func (p *Position) MarginRatio(mark dec.Decimal) dec.Decimal {
	if p.IsFlat() {
		return zero
	}
	notional := p.Notional(mark)
	if notional.Sign() == 0 {
		return zero
	}
	return p.Margin.Add(p.UnrealizedPnL(mark)).Div(notional)
}

// LiqPrice is the mark price at which this isolated position's margin_ratio
// equals mmr (maintenance margin rate). Derived by solving
// (margin + uPnL(mark)) / (mark*size) = mmr for mark:
//
//	long : (entry*size - margin) / (size*(1 - mmr))
//	short: (entry*size + margin) / (size*(1 + mmr))
//
// Zero when flat. Ignores fees/funding (MVP); the live liquidation trigger
// uses the exact pool health (pool.go), this is the observable estimate.
func (p *Position) LiqPrice(mmr dec.Decimal) dec.Decimal {
	if p.IsFlat() {
		return zero
	}
	one := dec.FromInt(1)
	entryNotional := p.Entry.Mul(p.Size)
	if p.Side == SideBuy {
		return entryNotional.Sub(p.Margin).Div(p.Size.Mul(one.Sub(mmr)))
	}
	return entryNotional.Add(p.Margin).Div(p.Size.Mul(one.Add(mmr)))
}

// BankruptcyPrice is the mark at which equity (margin + uPnL) hits zero —
// where the position has lost all its margin. The liquidation order is
// placed here (ADR-0068 §8). Zero when flat.
//
//	long : entry - margin/size
//	short: entry + margin/size
func (p *Position) BankruptcyPrice() dec.Decimal {
	if p.IsFlat() {
		return zero
	}
	marginPerUnit := p.Margin.Div(p.Size)
	if p.Side == SideBuy {
		return p.Entry.Sub(marginPerUnit)
	}
	return p.Entry.Add(marginPerUnit)
}
