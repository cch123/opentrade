// Package calc is the markprice computation core (ADR-0068 §5): it folds a
// stream of (index, perp-mid) observations into a manipulation-resistant mark
// price and a funding rate. Pure + deterministic; the Kafka consume (spot
// market-data) and produce (mark-price topic) wiring lives in cmd/markprice.
//
// mark  = index + clamp(EMA(perp_mid - index), ±basis_cap)
// fund  = clamp(TWAP(premium), ±rate_cap),  premium = (perp_mid - index)/index
//
// Running unrealized PnL / liquidation off the mark (not the perp last price)
// is what blunts "slam your own perp book to trigger liquidations" — the mark
// only drifts from the spot index by the smoothed, capped basis.
package calc

import "github.com/xargin/opentrade/pkg/dec"

var (
	zero = dec.FromInt(0)
	one  = dec.FromInt(1)
)

// Config tunes the mark/funding computation.
type Config struct {
	Alpha          dec.Decimal // EMA smoothing for the basis, in (0,1]
	BasisCap       dec.Decimal // clamp on |mark - index| (absolute USDT); 0 = no clamp
	FundingRateCap dec.Decimal // clamp on |funding_rate|; 0 = no clamp
}

// Calc holds the running EMA basis and the funding-interval premium
// accumulator. Not safe for concurrent use — the markprice worker is
// single-goroutine per symbol.
type Calc struct {
	cfg          Config
	emaBasis     dec.Decimal
	hasEMA       bool
	premiumSum   dec.Decimal
	premiumCount int64
}

// New returns a fresh Calc.
func New(cfg Config) *Calc {
	return &Calc{cfg: cfg, emaBasis: zero, premiumSum: zero}
}

// Tick folds one (index, perpMid) observation and returns the current mark
// price and the running funding-rate estimate. index should be > 0; a
// non-positive index skips the premium accumulation (mark still tracks the
// basis EMA).
func (c *Calc) Tick(index, perpMid dec.Decimal) (mark, fundingEstimate dec.Decimal) {
	basis := perpMid.Sub(index)
	if c.hasEMA {
		c.emaBasis = c.cfg.Alpha.Mul(basis).Add(one.Sub(c.cfg.Alpha).Mul(c.emaBasis))
	} else {
		c.emaBasis = basis
		c.hasEMA = true
	}
	mark = index.Add(clampAbs(c.emaBasis, c.cfg.BasisCap))
	if index.Sign() > 0 {
		c.premiumSum = c.premiumSum.Add(basis.Div(index))
		c.premiumCount++
	}
	return mark, c.currentFunding()
}

func (c *Calc) currentFunding() dec.Decimal {
	if c.premiumCount == 0 {
		return zero
	}
	avg := c.premiumSum.Div(dec.FromInt(c.premiumCount))
	return clampAbs(avg, c.cfg.FundingRateCap)
}

// FundingRate returns the settled rate for the interval (clamped TWAP premium)
// and resets the accumulator for the next interval.
func (c *Calc) FundingRate() dec.Decimal {
	r := c.currentFunding()
	c.premiumSum = zero
	c.premiumCount = 0
	return r
}

// clampAbs limits v to [-limit, +limit] when limit > 0.
func clampAbs(v, limit dec.Decimal) dec.Decimal {
	if limit.Sign() <= 0 {
		return v
	}
	if v.Cmp(limit) > 0 {
		return limit
	}
	if neg := limit.Neg(); v.Cmp(neg) < 0 {
		return neg
	}
	return v
}
