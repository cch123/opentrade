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
// all four net-mode cases: open, increase (same side), reduce/close (opposite
// side, qty <= size), and flip (opposite side, qty > size). Linear USDT math.
// Hedge legs must use ApplyFillLeg instead — a leg never flips (ADR-0077 §2).
//
// Initial margin for the opened/increased portion is notional/leverage — for
// isolated positions only. A cross position holds no margin cash bucket
// (ADR-0074 §4 rule #2): MarginAdded / MarginReleased stay zero, Margin stays
// zero, and the caller settles realized PnL / fee against the account's free
// balance while the order reservation is released separately.
func (p *Position) ApplyFill(f Fill) FillResult {
	res, _ := p.applyFill(f, 0)
	return res
}

// ApplyFillLeg applies f to a hedge leg (ADR-0077 §2). The leg's canonical
// side derives from p.PositionIdx: fills on that side increase; opposite
// fills reduce, clamped at the leg size — a leg NEVER flips, because its
// direction is part of the position key. excess is the clamped-off quantity
// (> 0 only when the leg shrank between order admission and this fill — the
// TOCTOU residue); the caller must surface it as REDUCE_ONLY_INVARIANT_BREACH
// (ADR-0081 §2), never absorb or silently drop it.
func (p *Position) ApplyFillLeg(f Fill) (res FillResult, excess dec.Decimal) {
	legSide := LegSide(p.PositionIdx)
	if legSide == 0 {
		// Not a hedge leg — net-mode record misrouted here. Fail closed: do
		// not guess a direction; report the full qty as unapplied.
		return FillResult{MarginAdded: zero, MarginReleased: zero, Realized: zero, Fee: f.Fee}, f.Qty
	}
	return p.applyFill(f, legSide)
}

// applyFill is the shared settlement core. legSide == 0 is net mode (the
// fill's relation to the current side decides increase vs reduce, overshoot
// flips); legSide != 0 is a hedge leg (the leg's canonical side decides,
// overshoot is clamped and returned as excess).
func (p *Position) applyFill(f Fill, legSide Side) (FillResult, dec.Decimal) {
	res := FillResult{MarginAdded: zero, MarginReleased: zero, Realized: zero, Fee: f.Fee}
	isolated := p.Mode != MarginCross

	increase := p.IsFlat() || f.Side == p.Side
	if legSide != 0 {
		// A leg's direction is identity, not inferred from current state: an
		// opposite-side fill on a flat leg is a reduce of nothing (all
		// excess), never an open in the wrong direction.
		increase = f.Side == legSide
	}

	// Open or increase.
	if increase {
		newSize := p.Size.Add(f.Qty)
		// Increasing a linear perp position does not realize PnL; it only moves
		// the average entry. Realization is reserved for opposite-side fills so
		// wallet accounting can treat "add" and "reduce" as separate cash flows.
		p.Entry = p.Entry.Mul(p.Size).Add(f.Price.Mul(f.Qty)).Div(newSize)
		p.Size = newSize
		p.Side = f.Side
		if isolated {
			im := initMargin(f.Price, f.Qty, p.Leverage)
			p.Margin = p.Margin.Add(im)
			res.MarginAdded = im
		}
		return res, zero
	}

	// Opposite side: reduce / close / (net: flip | leg: clamp).
	closeQty := dec.Min(f.Qty, p.Size)
	if closeQty.Sign() > 0 {
		var realized dec.Decimal
		if p.Side == SideBuy { // closing a long by selling
			realized = f.Price.Sub(p.Entry).Mul(closeQty)
		} else { // closing a short by buying
			realized = p.Entry.Sub(f.Price).Mul(closeQty)
		}
		if isolated {
			// Release margin proportional to the closed fraction.
			released := p.Margin.Mul(closeQty).Div(p.Size)
			p.Margin = p.Margin.Sub(released)
			res.MarginReleased = released
		}
		p.Size = p.Size.Sub(closeQty)
		p.Realized = p.Realized.Add(realized)
		res.Realized = realized
	}

	remaining := f.Qty.Sub(closeQty)
	if p.Size.Sign() == 0 {
		p.Entry = zero
		if legSide == 0 && remaining.Sign() > 0 {
			// Net mode fully closed with overshoot: flip into a new position
			// on f.Side at the fill price. The new exposure gets fresh initial
			// margin; the closed side's realized PnL and released margin stay
			// separate in FillResult for the service to route.
			p.Size = remaining
			p.Entry = f.Price
			p.Side = f.Side
			if isolated {
				im := initMargin(f.Price, remaining, p.Leverage)
				p.Margin = p.Margin.Add(im)
				res.MarginAdded = im
			}
			return res, zero
		}
		p.Side = 0 // flat
	}
	if legSide == 0 {
		return res, zero
	}
	return res, remaining
}

// ApplyFunding books one funding interval against the position (ADR-0068
// §7). rate > 0 ⇒ longs pay shorts; rate < 0 ⇒ reverse. Funding is computed
// on the mark notional. For an isolated position the payment lands in the
// position margin; for a cross position it must settle in the account's free
// balance instead (ADR-0074 open-question decision), so Margin is untouched
// and the caller routes the returned delta to the wallet. Both modes
// accumulate into Realized for reporting. Returns the signed delta
// (negative = the position paid). Idempotency (funding_round_seen) is the
// caller's job.
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
	if p.Mode != MarginCross {
		p.Margin = p.Margin.Add(delta)
	}
	p.Realized = p.Realized.Add(delta)
	return delta
}
