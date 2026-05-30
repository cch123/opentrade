package calc

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func d(s string) dec.Decimal { return dec.New(s) }

func eq(t *testing.T, got dec.Decimal, want, what string) {
	t.Helper()
	if got.Cmp(d(want)) != 0 {
		t.Fatalf("%s: got %s want %s", what, got.String(), want)
	}
}

// --- mark price (unchanged EMA-basis logic) ---------------------------------

func TestMark_TracksBasis(t *testing.T) {
	c := New(Config{Alpha: d("1"), BasisCap: d("50")}) // alpha 1 → ema = latest basis
	eq(t, c.Mark(d("100"), d("101")), "101", "mark = index + basis(+1)")
	eq(t, c.Mark(d("100"), d("99")), "99", "mark tracks negative basis")
}

func TestMark_BasisCapClamps(t *testing.T) {
	c := New(Config{Alpha: d("1"), BasisCap: d("5")})
	eq(t, c.Mark(d("100"), d("200")), "105", "basis +100 clamped to +5")
	eq(t, c.Mark(d("100"), d("0")), "95", "basis -100 clamped to -5")
}

func TestMark_EMASmoothing(t *testing.T) {
	c := New(Config{Alpha: d("0.5"), BasisCap: d("50")})
	eq(t, c.Mark(d("100"), d("102")), "102", "first tick ema = basis 2")
	eq(t, c.Mark(d("100"), d("100")), "101", "ema = 0.5*0 + 0.5*2 = 1 → mark 101")
}

// --- funding rate (Binance impact-premium method) ---------------------------

// premiumIndex = (Max(0, impactBid-index) - Max(0, index-impactAsk)) / index
func TestPremiumIndex_RichCheapAndInsideSpread(t *testing.T) {
	eq(t, premiumIndex(d("102"), d("103"), d("100")), "0.02", "perp rich → +premium")
	eq(t, premiumIndex(d("97"), d("98"), d("100")), "-0.02", "perp cheap → -premium")
	// Index sits inside the impact spread → no premium (the key anti-noise property).
	eq(t, premiumIndex(d("99"), d("101"), d("100")), "0", "index inside spread → 0")
}

func TestFunding_PremiumOnly_BandZero(t *testing.T) {
	// Band 0 ⇒ interest term suppressed ⇒ funding = pure average premium.
	c := New(Config{PremiumBand: d("0")})
	c.SamplePremium(d("102"), d("103"), d("100")) // 0.02
	c.SamplePremium(d("104"), d("105"), d("100")) // 0.04
	eq(t, c.ForecastFundingRate(), "0.03", "forecast = TWAP premium (0.02,0.04)")
	eq(t, c.SettleFundingRate(), "0.03", "settle = TWAP premium")
	eq(t, c.SettleFundingRate(), "0", "accumulator resets after settle")
}

func TestFunding_InterestBand(t *testing.T) {
	// interestPerInterval = 0.0003 * 480/1440 = 0.0001; band ±0.0005.
	cfg := Config{InterestDaily: d("0.0003"), IntervalMin: 480, PremiumBand: d("0.0005")}

	// Small/zero premium → the interest term shows through.
	c := New(cfg)
	c.SamplePremium(d("99"), d("101"), d("100")) // premium 0
	eq(t, c.SettleFundingRate(), "0.0001", "zero premium → funding ≈ interest")

	// Large premium → interest pull is clamped to -band.
	c2 := New(cfg)
	c2.SamplePremium(d("102"), d("103"), d("100")) // premium 0.02
	// 0.02 + clamp(0.0001 - 0.02, ±0.0005) = 0.02 + (-0.0005) = 0.0195
	eq(t, c2.SettleFundingRate(), "0.0195", "rich premium pulled toward interest by at most band")
}

func TestFunding_CapClamps(t *testing.T) {
	c := New(Config{PremiumBand: d("0.0005"), FundingCap: d("0.0075")})
	c.SamplePremium(d("105"), d("106"), d("100")) // premium 0.05
	// 0.05 + clamp(0-0.05, ±0.0005) = 0.0495 → clamp(±0.0075) = 0.0075
	eq(t, c.SettleFundingRate(), "0.0075", "funding clamped to cap")
}

func TestFunding_NonPositiveIndexSkipped(t *testing.T) {
	c := New(Config{PremiumBand: d("0")})
	c.SamplePremium(d("102"), d("103"), d("0")) // index 0 → skipped
	eq(t, c.SettleFundingRate(), "0", "no samples → zero funding")
}
