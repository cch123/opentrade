package etcdcfg

import (
	"strings"
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func d(s string) dec.Decimal { return dec.New(s) }

func TestFloorToStep(t *testing.T) {
	cases := []struct {
		v, step, want string
	}{
		{"0.00012345", "0.00001", "0.00012"},
		{"0.00012000", "0.00001", "0.00012"}, // already a multiple
		{"0", "0.00001", "0"},
		{"100.004999", "0.01", "100"},
		{"100.015", "0.01", "100.01"},
		{"3", "1", "3"},
	}
	for _, c := range cases {
		got := FloorToStep(d(c.v), d(c.step))
		if !dec.Equal(got, d(c.want)) {
			t.Errorf("FloorToStep(%s, %s) = %s; want %s", c.v, c.step, got.String(), c.want)
		}
	}
}

func TestFloorToStepPanicsOnNonPositiveStep(t *testing.T) {
	for _, step := range []string{"0", "-0.01"} {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic for step=%s", step)
				}
			}()
			FloorToStep(d("1"), d(step))
		}()
	}
}

func TestCeilToStep(t *testing.T) {
	cases := []struct {
		v, step, want string
	}{
		{"0.00012001", "0.00001", "0.00013"},
		{"0.00012000", "0.00001", "0.00012"},
		{"0", "0.01", "0"},
		{"100.001", "0.01", "100.01"},
	}
	for _, c := range cases {
		got := CeilToStep(d(c.v), d(c.step))
		if !dec.Equal(got, d(c.want)) {
			t.Errorf("CeilToStep(%s, %s) = %s; want %s", c.v, c.step, got.String(), c.want)
		}
	}
}

func TestRoundToTick(t *testing.T) {
	cases := []struct {
		v, tick, want string
	}{
		{"100.004", "0.01", "100"},
		{"100.005", "0.01", "100.01"}, // half-up
		{"100.015", "0.01", "100.02"},
	}
	for _, c := range cases {
		got := RoundToTick(d(c.v), d(c.tick))
		if !dec.Equal(got, d(c.want)) {
			t.Errorf("RoundToTick(%s, %s) = %s; want %s", c.v, c.tick, got.String(), c.want)
		}
	}
}

func TestIsMultipleOfStep(t *testing.T) {
	cases := []struct {
		v, step string
		want    bool
	}{
		{"0.00012", "0.00001", true},
		{"0.000123", "0.00001", false},
		{"0", "0.00001", true}, // zero is a multiple of any step
		{"100", "0.01", true},
		{"100.01", "0.01", true},
		{"100.015", "0.01", false},
		// non-positive step → false (not panic)
		{"1", "0", false},
		{"1", "-0.01", false},
	}
	for _, c := range cases {
		got := IsMultipleOfStep(d(c.v), d(c.step))
		if got != c.want {
			t.Errorf("IsMultipleOfStep(%s, %s) = %v; want %v", c.v, c.step, got, c.want)
		}
	}
}

func TestFindTier(t *testing.T) {
	tiers := []PrecisionTier{
		{PriceFrom: d("0"), PriceTo: d("1"), TickSize: d("0.0001"), StepSize: d("1")},
		{PriceFrom: d("1"), PriceTo: d("100"), TickSize: d("0.001"), StepSize: d("0.01")},
		{PriceFrom: d("100"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("0.0001")},
	}
	cases := []struct {
		price string
		want  int // index into tiers; -1 = nil
	}{
		{"0", 0},
		{"0.5", 0},
		{"0.999", 0},
		{"1", 1},
		{"50", 1},
		{"99.999", 1},
		{"100", 2},
		{"1000000", 2},
		{"-1", -1}, // negative
	}
	for _, c := range cases {
		got := FindTier(tiers, d(c.price))
		if c.want == -1 {
			if got != nil {
				t.Errorf("FindTier(%s) = %+v; want nil", c.price, *got)
			}
			continue
		}
		if got == nil {
			t.Errorf("FindTier(%s) = nil; want tier[%d]", c.price, c.want)
			continue
		}
		want := &tiers[c.want]
		if !dec.Equal(got.PriceFrom, want.PriceFrom) || !dec.Equal(got.PriceTo, want.PriceTo) {
			t.Errorf("FindTier(%s) = [%s, %s); want [%s, %s)",
				c.price, got.PriceFrom.String(), got.PriceTo.String(),
				want.PriceFrom.String(), want.PriceTo.String())
		}
	}
}

func TestFindTierEmpty(t *testing.T) {
	if got := FindTier(nil, d("1")); got != nil {
		t.Errorf("FindTier(nil) = %+v; want nil", got)
	}
}

func validSingleTier() []PrecisionTier {
	return []PrecisionTier{
		{PriceFrom: d("0"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("0.00001")},
	}
}

func validTwoTiers() []PrecisionTier {
	return []PrecisionTier{
		{PriceFrom: d("0"), PriceTo: d("1"), TickSize: d("0.0001"), StepSize: d("1")},
		{PriceFrom: d("1"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("0.00001")},
	}
}

func TestValidateTiersOK(t *testing.T) {
	if err := ValidateTiers(validSingleTier()); err != nil {
		t.Errorf("single tier: %v", err)
	}
	if err := ValidateTiers(validTwoTiers()); err != nil {
		t.Errorf("two tiers: %v", err)
	}
}

func TestValidateTiersFailures(t *testing.T) {
	cases := []struct {
		name  string
		tiers []PrecisionTier
		want  string // substring match on error
	}{
		{"empty", nil, "at least one tier"},
		{"first-from-not-zero", []PrecisionTier{
			{PriceFrom: d("1"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("1")},
		}, "first tier PriceFrom must be 0"},
		{"tick-non-positive", []PrecisionTier{
			{PriceFrom: d("0"), PriceTo: d("0"), TickSize: d("0"), StepSize: d("1")},
		}, "TickSize must be positive"},
		{"step-non-positive", []PrecisionTier{
			{PriceFrom: d("0"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("0")},
		}, "StepSize must be positive"},
		{"last-priceto-not-zero", []PrecisionTier{
			{PriceFrom: d("0"), PriceTo: d("100"), TickSize: d("0.01"), StepSize: d("1")},
		}, "last tier PriceTo must be 0"},
		{"gap", []PrecisionTier{
			{PriceFrom: d("0"), PriceTo: d("1"), TickSize: d("0.01"), StepSize: d("1")},
			{PriceFrom: d("2"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("1")},
		}, "must equal"},
		{"non-last-tier-open-end", []PrecisionTier{
			{PriceFrom: d("0"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("1")},
			{PriceFrom: d("10"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("1")},
		}, "must be positive for non-last tier"},
	}
	for _, c := range cases {
		if c.want == "" {
			continue
		}
		err := ValidateTiers(c.tiers)
		if err == nil {
			t.Errorf("%s: expected error containing %q; got nil", c.name, c.want)
			continue
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %q does not contain %q", c.name, err.Error(), c.want)
		}
	}
}

func TestValidateTierEvolutionFirstInstall(t *testing.T) {
	if err := ValidateTierEvolution(nil, validTwoTiers()); err != nil {
		t.Errorf("first install: %v", err)
	}
}

func TestValidateTierEvolutionSplit(t *testing.T) {
	old := validTwoTiers() // [0,1), [1,+∞)
	newT := []PrecisionTier{
		{PriceFrom: d("0"), PriceTo: d("1"), TickSize: d("0.0001"), StepSize: d("1")},
		{PriceFrom: d("1"), PriceTo: d("100"), TickSize: d("0.001"), StepSize: d("0.01")},
		{PriceFrom: d("100"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("0.00001")},
	}
	if err := ValidateTierEvolution(old, newT); err != nil {
		t.Errorf("split [1,+∞) into [1,100) + [100,+∞) should be allowed: %v", err)
	}
}

func TestValidateTierEvolutionAppend(t *testing.T) {
	// Appending a tier beyond the existing open end is disallowed because
	// the previous last tier's PriceTo was 0 (+∞) and the new list must
	// still cover 0..+∞ with a new open end. Appending at the low end is
	// also impossible (first tier PriceFrom must be 0). So the only legal
	// "append" is splitting the last open tier — which is covered by the
	// Split test. This test asserts that a clumsy append (dropping the
	// open end) is rejected.
	old := validSingleTier() // [0,+∞)
	bad := []PrecisionTier{
		{PriceFrom: d("0"), PriceTo: d("1000"), TickSize: d("0.01"), StepSize: d("1")},
		// missing open tier at end
	}
	err := ValidateTierEvolution(old, bad)
	if err == nil {
		t.Fatal("expected error: new tiers don't cover +∞")
	}
	if !strings.Contains(err.Error(), "last tier PriceTo must be 0") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestValidateTierEvolutionMergeRejected(t *testing.T) {
	// old has boundary at 1; new tries to merge [0,1) + [1,+∞) into [0,+∞)
	old := validTwoTiers()
	merged := validSingleTier()
	err := ValidateTierEvolution(old, merged)
	if err == nil {
		t.Fatal("expected merge rejection")
	}
	if !strings.Contains(err.Error(), "drop old boundary") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestValidateTierEvolutionShiftRejected(t *testing.T) {
	// old has boundary at 1; new shifts it to 2 — old boundary 1 disappears
	old := validTwoTiers()
	shifted := []PrecisionTier{
		{PriceFrom: d("0"), PriceTo: d("2"), TickSize: d("0.0001"), StepSize: d("1")},
		{PriceFrom: d("2"), PriceTo: d("0"), TickSize: d("0.01"), StepSize: d("0.00001")},
	}
	err := ValidateTierEvolution(old, shifted)
	if err == nil {
		t.Fatal("expected shift rejection")
	}
}

// ---- ValidateOrderAgainstTier ----

func tierForTest() PrecisionTier {
	return PrecisionTier{
		PriceFrom:                     d("0"),
		PriceTo:                       d("0"),
		TickSize:                      d("0.01"),
		StepSize:                      d("0.00001"),
		QuoteStepSize:                 d("0.01"),
		MinQty:                        d("0.0001"),
		MaxQty:                        d("1000"),
		MinQuoteQty:                   d("1"),
		MinQuoteAmount:                d("5"),
		MarketMinQty:                  d("0.001"),
		MarketMaxQty:                  d("100"),
		EnforceMinQuoteAmountOnMarket: true,
		AvgPriceMins:                  5,
	}
}

func cfgWithTier(t PrecisionTier) SymbolConfig {
	return SymbolConfig{
		Shard:            "match-0",
		Trading:          true,
		BaseAsset:        "BTC",
		QuoteAsset:       "USDT",
		PrecisionVersion: 1,
		Tiers:            []PrecisionTier{t},
	}
}

func TestValidateOrder_CompatibilityMode(t *testing.T) {
	// Empty tiers => no validation (M0 contract).
	cfg := SymbolConfig{Shard: "match-0", Trading: true}
	reason, ok := ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindLimit, Price: d("0.123456789"), Qty: d("0.000000001"),
	})
	if !ok || reason != RejectNone {
		t.Errorf("compat mode should pass: reason=%s ok=%v", reason, ok)
	}
}

func TestValidateOrder_LimitAcceptsValid(t *testing.T) {
	cfg := cfgWithTier(tierForTest())
	// Price 100.00 (multiple of 0.01), qty 0.1 (multiple of 0.00001), amount 10 > min 5
	reason, ok := ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindLimit, Price: d("100.00"), Qty: d("0.1"),
	})
	if !ok || reason != RejectNone {
		t.Errorf("valid limit: reason=%s ok=%v", reason, ok)
	}
}

func TestValidateOrder_LimitRejects(t *testing.T) {
	cfg := cfgWithTier(tierForTest())
	cases := []struct {
		name       string
		price, qty string
		want       RejectReason
	}{
		{"bad-tick", "100.001", "0.1", RejectInvalidPriceTick},
		{"bad-step", "100.00", "0.000001", RejectInvalidLotSize},
		{"below-min-qty", "100.00", "0.00001", RejectInvalidLotSize}, // 0.00001 is a step multiple but < min... actually 0.00001 == step so multiple. But MinQty=0.0001 so < min. Need step-valid qty that's below min.
		{"min-quote-amount", "1.00", "0.0001", RejectMinQuoteAmount}, // amount = 0.0001 < 5
	}
	// Fix up the "below-min-qty" case: use qty=0.00005 which is below 0.0001 min but 0.00005 is 5*step (step=0.00001), so multiple.
	cases[2].qty = "0.00005"
	cases[2].want = RejectMinQty

	for _, c := range cases {
		reason, ok := ValidateOrderAgainstTier(cfg, OrderValidation{
			Kind: OrderKindLimit, Price: d(c.price), Qty: d(c.qty),
		})
		if ok {
			t.Errorf("%s: expected reject %s, got pass", c.name, c.want)
			continue
		}
		if reason != c.want {
			t.Errorf("%s: reason=%s want=%s", c.name, reason, c.want)
		}
	}
}

func TestValidateOrder_MarketByBase(t *testing.T) {
	cfg := cfgWithTier(tierForTest())
	// Valid: qty=0.01 (multiple of step, >= MarketMinQty 0.001, price*qty = 1 < MinQuoteAmount 5 → reject when enforce=true)
	reason, ok := ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindMarketByBase, Price: d("100"), Qty: d("0.01"),
	})
	if ok || reason != RejectMinQuoteAmount {
		t.Errorf("expected RejectMinQuoteAmount, got reason=%s ok=%v", reason, ok)
	}
	// Valid: qty=0.1, amount=10
	reason, ok = ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindMarketByBase, Price: d("100"), Qty: d("0.1"),
	})
	if !ok {
		t.Errorf("valid market: reason=%s", reason)
	}
	// Reject below MarketMinQty (0.001)
	reason, ok = ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindMarketByBase, Price: d("100"), Qty: d("0.0001"),
	})
	if ok || reason != RejectMinQty {
		t.Errorf("expected RejectMinQty, got %s ok=%v", reason, ok)
	}
}

func TestValidateOrder_MarketBuyByQuote(t *testing.T) {
	cfg := cfgWithTier(tierForTest())
	// Valid: quote=10 (multiple of 0.01, >= 1, >= 5)
	reason, ok := ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindMarketBuyByQuote, Price: d("100"), QuoteQty: d("10.00"),
	})
	if !ok {
		t.Errorf("valid quote buy: reason=%s", reason)
	}
	// Bad quote step
	reason, ok = ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindMarketBuyByQuote, Price: d("100"), QuoteQty: d("10.001"),
	})
	if ok || reason != RejectInvalidQuoteStep {
		t.Errorf("expected RejectInvalidQuoteStep, got %s", reason)
	}
	// Below MinQuoteQty
	reason, ok = ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindMarketBuyByQuote, Price: d("100"), QuoteQty: d("0.50"),
	})
	if ok || reason != RejectMinQuoteQty {
		t.Errorf("expected RejectMinQuoteQty, got %s", reason)
	}
	// Below MinQuoteAmount (with enforce=true)
	reason, ok = ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindMarketBuyByQuote, Price: d("100"), QuoteQty: d("2.00"),
	})
	// quote=2 >= MinQuoteQty=1 but < MinQuoteAmount=5 → RejectMinQuoteAmount
	if ok || reason != RejectMinQuoteAmount {
		t.Errorf("expected RejectMinQuoteAmount, got %s", reason)
	}
}

func TestValidateOrder_NoTier(t *testing.T) {
	cfg := cfgWithTier(tierForTest())
	reason, ok := ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindLimit, Price: d("-1"), Qty: d("1"),
	})
	if ok || reason != RejectNoTier {
		t.Errorf("expected RejectNoTier for negative price, got %s ok=%v", reason, ok)
	}
}

func TestValidateOrder_UnknownKind(t *testing.T) {
	cfg := cfgWithTier(tierForTest())
	reason, ok := ValidateOrderAgainstTier(cfg, OrderValidation{
		Kind: OrderKindUnknown, Price: d("100"), Qty: d("1"),
	})
	if ok {
		t.Errorf("unknown kind should reject, got reason=%s", reason)
	}
}

func TestHasPrecision(t *testing.T) {
	if (SymbolConfig{}).HasPrecision() {
		t.Error("empty cfg should not have precision")
	}
	cfg := cfgWithTier(tierForTest())
	if !cfg.HasPrecision() {
		t.Error("cfg with tier should have precision")
	}
}
