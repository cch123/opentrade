// Package calc is the markprice computation core (ADR-0068 §5): it folds a
// stream of market-data observations into a manipulation-resistant mark price
// and a Binance-style funding rate. Pure + deterministic; the Kafka consume
// (market-data) and produce (mark-price) wiring lives in cmd/markprice.
//
// Mark price (for unrealized PnL + liquidation):
//
//	mark = index + clamp(EMA(perp_mid - index), ±basis_cap)
//
// Funding rate (ADR-0068 §7, Binance method) — sampled across the interval then
// settled at the boundary:
//
//	premium_index = ( Max(0, impact_bid - index) - Max(0, index - impact_ask) ) / index
//	avg_premium   = TWAP(premium_index over the interval)
//	funding_rate  = avg_premium + clamp(interest_per_interval - avg_premium, ±band)
//	              = clamp(funding_rate, ±funding_cap)
//
// impact_bid/ask are depth-weighted (the average fill price over a fixed
// notional, computed in internal/journal.Book) — NOT top-of-book — so a thin
// best quote cannot move the rate. interest_per_interval = daily_interest ×
// interval_min / 1440. Running unrealized PnL / liquidation off the mark, and
// funding off the impact-based premium, is what blunts self-book manipulation.
package calc

import "github.com/xargin/opentrade/pkg/dec"

var (
	zero    = dec.FromInt(0)
	one     = dec.FromInt(1)
	minsDay = dec.FromInt(1440)
)

// Config tunes the mark/funding computation.
type Config struct {
	// Mark.
	Alpha    dec.Decimal // EMA smoothing for the basis, in (0,1]
	BasisCap dec.Decimal // clamp on |mark - index| (absolute USDT); 0 = no clamp

	// Funding (Binance method).
	InterestDaily dec.Decimal // daily interest-rate component (e.g. 0.0003 = 0.03%/day)
	IntervalMin   int64       // funding interval in minutes (for interest-per-interval); 0 disables the interest term
	PremiumBand   dec.Decimal // ± band on (interest - avg_premium); Binance uses 0.0005 (±0.05%); 0 = no band clamp
	FundingCap    dec.Decimal // upper clamp on the settled funding rate; 0 = no clamp
	// FundingFloor is the lower clamp (<= 0). Zero mirrors -FundingCap (the
	// legacy symmetric behavior); an ADR-0075 catalog can set an asymmetric
	// floor per symbol.
	FundingFloor dec.Decimal
}

// Calc holds the running mark-basis EMA and the funding-interval premium-index
// accumulator. Not safe for concurrent use — the markprice tick loop is a
// single goroutine.
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

// Mark folds one (index, perpMid) observation into the basis EMA and returns
// the current mark price.
func (c *Calc) Mark(index, perpMid dec.Decimal) dec.Decimal {
	basis := perpMid.Sub(index)
	if c.hasEMA {
		c.emaBasis = c.cfg.Alpha.Mul(basis).Add(one.Sub(c.cfg.Alpha).Mul(c.emaBasis))
	} else {
		c.emaBasis = basis
		c.hasEMA = true
	}
	return index.Add(clampAbs(c.emaBasis, c.cfg.BasisCap))
}

// SamplePremium accumulates one premium-index observation from the perp impact
// prices vs the index (ADR-0068 §7). Non-positive index is skipped (no division
// guard needed downstream).
func (c *Calc) SamplePremium(impactBid, impactAsk, index dec.Decimal) {
	if index.Sign() <= 0 {
		return
	}
	c.premiumSum = c.premiumSum.Add(premiumIndex(impactBid, impactAsk, index))
	c.premiumCount++
}

// ForecastFundingRate returns the funding-rate estimate from the premium samples
// gathered so far this interval, WITHOUT resetting (for the MarkTick estimate).
func (c *Calc) ForecastFundingRate() dec.Decimal {
	return c.fundingFromAvg(c.avgPremium())
}

// SettleFundingRate returns the funding rate for the interval and resets the
// premium accumulator for the next one (for the boundary FundingTick).
func (c *Calc) SettleFundingRate() dec.Decimal {
	r := c.fundingFromAvg(c.avgPremium())
	c.premiumSum = zero
	c.premiumCount = 0
	return r
}

// premiumIndex is the per-sample premium: how far the depth-weighted impact
// quotes sit outside the index, normalised by the index. Positive ⇒ perp trades
// rich (longs pay), negative ⇒ perp trades cheap (shorts pay).
func premiumIndex(impactBid, impactAsk, index dec.Decimal) dec.Decimal {
	buyDiff := maxDec(zero, impactBid.Sub(index))  // perp bid above index
	sellDiff := maxDec(zero, index.Sub(impactAsk)) // perp ask below index
	return buyDiff.Sub(sellDiff).Div(index)
}

func (c *Calc) avgPremium() dec.Decimal {
	if c.premiumCount == 0 {
		return zero
	}
	return c.premiumSum.Div(dec.FromInt(c.premiumCount))
}

// fundingFromAvg applies the Binance interest-band + cap/floor to an average
// premium: funding = avg + clamp(interest - avg, ±band), then clamped into
// [floor (or -cap), cap].
func (c *Calc) fundingFromAvg(avg dec.Decimal) dec.Decimal {
	f := avg.Add(clampBand(c.interestPerInterval().Sub(avg), c.cfg.PremiumBand))
	return clampRange(f, c.cfg.FundingFloor, c.cfg.FundingCap)
}

// clampRange limits v to [floor, cap]. cap <= 0 disables the upper clamp;
// floor 0 mirrors -cap (legacy symmetric clamp); floor > 0 never appears
// (rejected at config validation).
func clampRange(v, floor, cap dec.Decimal) dec.Decimal {
	if cap.Sign() > 0 && v.Cmp(cap) > 0 {
		return cap
	}
	lower := floor
	if lower.Sign() == 0 && cap.Sign() > 0 {
		lower = cap.Neg()
	}
	if lower.Sign() < 0 && v.Cmp(lower) < 0 {
		return lower
	}
	return v
}

// interestPerInterval = daily_interest × interval_min / 1440 (the interval's
// share of the daily interest-rate term). Zero when the interval is unset.
func (c *Calc) interestPerInterval() dec.Decimal {
	if c.cfg.IntervalMin <= 0 {
		return zero
	}
	return c.cfg.InterestDaily.Mul(dec.FromInt(c.cfg.IntervalMin)).Div(minsDay)
}

// clampAbs limits v to [-limit, +limit] when limit > 0; limit <= 0 disables the
// clamp (used for the optional basis / funding caps).
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

// clampBand limits v to [-band, +band]. Unlike clampAbs a non-positive band is a
// ZERO-WIDTH band (returns 0): with band 0 the interest term is fully
// suppressed and the funding rate is the pure average premium.
func clampBand(v, band dec.Decimal) dec.Decimal {
	if band.Sign() <= 0 {
		return zero
	}
	if v.Cmp(band) > 0 {
		return band
	}
	if neg := band.Neg(); v.Cmp(neg) < 0 {
		return neg
	}
	return v
}

func maxDec(a, b dec.Decimal) dec.Decimal {
	if a.Cmp(b) >= 0 {
		return a
	}
	return b
}
