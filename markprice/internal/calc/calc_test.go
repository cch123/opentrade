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

func TestCalc_MarkTracksBasis(t *testing.T) {
	// alpha=1 → ema == latest basis; cap loose.
	c := New(Config{Alpha: d("1"), BasisCap: d("50"), FundingRateCap: d("1")})
	mark, _ := c.Tick(d("100"), d("101"))
	eq(t, mark, "101", "mark = index + basis(1)")
	mark, _ = c.Tick(d("100"), d("99"))
	eq(t, mark, "99", "mark tracks negative basis")
}

func TestCalc_BasisCapClamps(t *testing.T) {
	c := New(Config{Alpha: d("1"), BasisCap: d("5"), FundingRateCap: d("1")})
	mark, _ := c.Tick(d("100"), d("200")) // basis 100, clamped to +5
	eq(t, mark, "105", "mark clamped to index + cap")
	mark, _ = c.Tick(d("100"), d("0")) // basis -100, clamped to -5
	eq(t, mark, "95", "mark clamped to index - cap")
}

func TestCalc_EMASmoothing(t *testing.T) {
	c := New(Config{Alpha: d("0.5"), BasisCap: d("50"), FundingRateCap: d("1")})
	mark, _ := c.Tick(d("100"), d("102")) // first → ema = basis = 2 → mark 102
	eq(t, mark, "102", "first tick ema = basis")
	mark, _ = c.Tick(d("100"), d("100")) // basis 0 → ema = 0.5*0 + 0.5*2 = 1 → mark 101
	eq(t, mark, "101", "ema smooths toward new basis")
}

func TestCalc_FundingRateTWAPAndReset(t *testing.T) {
	c := New(Config{Alpha: d("1"), BasisCap: d("50"), FundingRateCap: d("1")})
	c.Tick(d("100"), d("101")) // premium 0.01
	c.Tick(d("100"), d("103")) // premium 0.03
	// TWAP = (0.01 + 0.03)/2 = 0.02
	eq(t, c.FundingRate(), "0.02", "funding = mean premium")
	// Accumulator reset.
	eq(t, c.FundingRate(), "0", "funding resets after settle")
}

func TestCalc_FundingRateCapClamps(t *testing.T) {
	c := New(Config{Alpha: d("1"), BasisCap: d("50"), FundingRateCap: d("0.0075")})
	c.Tick(d("100"), d("105")) // premium 0.05 → clamped to 0.0075
	eq(t, c.FundingRate(), "0.0075", "funding clamped to cap")
}
