package perpstate

import "github.com/xargin/opentrade/pkg/dec"

// Fill is one execution applied to a user's position. Price/Qty are the
// trade execution price/qty (ADR-0068 invariant #2: realized PnL uses the
// execution price, not mark). Side is the side of THIS user's order leg.
// Fee is the USDT taker/maker fee for this fill (>= 0).
//
// reduce_only validity (does this fill wrongly increase the position?) is
// the caller's pre-trade concern (perp-counter); ApplyFill assumes the fill
// is already authorized.
type Fill struct {
	Side  Side
	Price dec.Decimal
	Qty   dec.Decimal
	Fee   dec.Decimal
}

// FillResult reports the cash effects of a fill so the caller can move USDT
// between the wallet and the position's collateral. The algebra here owns
// position geometry + position margin; the service owns the wallet.
//
//	MarginAdded:    initial margin committed into the position (open/increase)
//	MarginReleased: margin returned to the wallet (reduce/close), proportional
//	Realized:       signed PnL booked by closing (USDT, pre-fee)
//	Fee:            echoed back; the service deducts it from the wallet
type FillResult struct {
	MarginAdded    dec.Decimal
	MarginReleased dec.Decimal
	Realized       dec.Decimal
	Fee            dec.Decimal
}

// ApplyFill mutates p by applying f and returns the cash effects. It handles
// all four cases: open, increase (same side), reduce/close (opposite side,
// qty <= size), and flip (opposite side, qty > size). Linear USDT math.
//
// Initial margin for the opened/increased portion is notional/leverage.
func (p *Position) ApplyFill(f Fill) FillResult {
	res := FillResult{MarginAdded: zero, MarginReleased: zero, Realized: zero, Fee: f.Fee}

	// Open or increase: flat, or same direction as the existing position.
	if p.IsFlat() || f.Side == p.Side {
		im := initMargin(f.Price, f.Qty, p.Leverage)
		newSize := p.Size.Add(f.Qty)
		// weighted-average entry = (entry*size + price*qty) / newSize
		p.Entry = p.Entry.Mul(p.Size).Add(f.Price.Mul(f.Qty)).Div(newSize)
		p.Size = newSize
		p.Side = f.Side
		p.Margin = p.Margin.Add(im)
		res.MarginAdded = im
		return res
	}

	// Opposite side: reduce / close / flip.
	closeQty := dec.Min(f.Qty, p.Size)
	var realized dec.Decimal
	if p.Side == SideBuy { // closing a long by selling
		realized = f.Price.Sub(p.Entry).Mul(closeQty)
	} else { // closing a short by buying
		realized = p.Entry.Sub(f.Price).Mul(closeQty)
	}
	// Release margin proportional to the closed fraction.
	released := p.Margin.Mul(closeQty).Div(p.Size)
	p.Margin = p.Margin.Sub(released)
	p.Size = p.Size.Sub(closeQty)
	p.Realized = p.Realized.Add(realized)
	res.Realized = realized
	res.MarginReleased = released

	if p.Size.Sign() == 0 {
		// Fully closed. Reset entry; if the fill overshoots, flip into a new
		// position on f.Side with the remaining qty at the fill price.
		p.Entry = zero
		remaining := f.Qty.Sub(closeQty)
		if remaining.Sign() > 0 {
			im := initMargin(f.Price, remaining, p.Leverage)
			p.Size = remaining
			p.Entry = f.Price
			p.Side = f.Side
			p.Margin = p.Margin.Add(im)
			res.MarginAdded = im
		} else {
			p.Side = 0 // flat
		}
	}
	return res
}

// ApplyFunding books one funding interval against the position (ADR-0068
// §7). rate > 0 ⇒ longs pay shorts; rate < 0 ⇒ reverse. Funding is computed
// on the mark notional and lands in the position margin (and accumulates
// into Realized for reporting). Returns the signed margin delta (negative =
// the position paid). Idempotency (funding_round_seen) is the caller's job.
func (p *Position) ApplyFunding(mark, rate dec.Decimal) dec.Decimal {
	if p.IsFlat() {
		return zero
	}
	payment := p.Notional(mark).Mul(rate) // amount a long pays at this rate
	var delta dec.Decimal
	if p.Side == SideBuy {
		delta = payment.Neg() // long pays
	} else {
		delta = payment // short receives
	}
	p.Margin = p.Margin.Add(delta)
	p.Realized = p.Realized.Add(delta)
	return delta
}
