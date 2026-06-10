package perpcfg

import (
	"strings"
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func validSpec() PerpSymbol {
	return PerpSymbol{
		Symbol:       "BTC-USDT-PERP",
		ContractType: ContractLinearPerp,
		BaseAsset:    "BTC",
		QuoteAsset:   "USDT",
		SettleAsset:  "USDT",
		ContractSize: dec.FromInt(1),
		PriceScale:   2,
		QtyScale:     3,
	}
}

func validConfig() PerpSymbolConfig {
	return PerpSymbolConfig{
		Symbol:        "BTC-USDT-PERP",
		ConfigVersion: 1,
		Status:        StatusTrading,
		Precision:     Precision{TickSize: dec.New("0.5"), QtyStep: dec.New("0.001")},
		OrderLimits: OrderLimits{
			MinPrice: dec.New("0.5"), MaxPrice: dec.New("1000000"),
			MinOrderQty: dec.New("0.001"), MaxOrderQty: dec.New("100"),
			MinNotional: dec.New("5"),
		},
		RiskTiers: []RiskTier{
			{RiskID: 1, MaxNotional: dec.New("50000"), MaintMarginRatio: dec.New("0.005"), MaxLeverage: dec.New("100"), LiqFeeRate: dec.New("0.0005")},
			{RiskID: 2, MaxNotional: zero, MaintMarginRatio: dec.New("0.01"), MaxLeverage: dec.New("50"), LiqFeeRate: dec.New("0.001")},
		},
		Funding: FundingParams{
			IntervalSeconds: 28800, InterestRate: dec.New("0.0003"),
			Cap: dec.New("0.0075"), Floor: dec.New("-0.0075"), Clamp: dec.New("0.0005"),
		},
		Pricing: PricingParams{
			SpotSymbol:   "BTC-USDT",
			MarkEmaAlpha: dec.New("0.1"), MarkBasisCap: zero,
			ImpactNotional: dec.New("20000"), IndexDeviationBand: dec.New("0.05"),
		},
		Fees:      FeeParams{MakerFeeRate: dec.New("0.0002"), TakerFeeRate: dec.New("0.00055")},
		RiskApply: RiskApplyStaged,
	}
}

func TestValidateSpec(t *testing.T) {
	if err := ValidateSpec(validSpec()); err != nil {
		t.Fatalf("valid spec rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*PerpSymbol)
		want   string
	}{
		{"empty symbol", func(s *PerpSymbol) { s.Symbol = "" }, "symbol is required"},
		{"illegal char", func(s *PerpSymbol) { s.Symbol = "BTC USDT" }, "illegal character"},
		{"reserved contract type", func(s *PerpSymbol) { s.ContractType = ContractLinearFuture }, "reserved"},
		{"unknown contract type", func(s *PerpSymbol) { s.ContractType = "PERP" }, "unknown contract_type"},
		{"settle != quote", func(s *PerpSymbol) { s.SettleAsset = "BTC" }, "settle"},
		{"zero contract size", func(s *PerpSymbol) { s.ContractSize = zero }, "contract_size"},
		{"negative scale", func(s *PerpSymbol) { s.PriceScale = -1 }, "price_scale"},
	}
	for _, tc := range cases {
		s := validSpec()
		tc.mutate(&s)
		err := ValidateSpec(s)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want contains %q", tc.name, err, tc.want)
		}
	}
}

func TestValidateConfig(t *testing.T) {
	if err := ValidateConfig(validSpec(), validConfig()); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*PerpSymbolConfig)
		want   string
	}{
		{"zero version", func(c *PerpSymbolConfig) { c.ConfigVersion = 0 }, "config_version"},
		{"bad status", func(c *PerpSymbolConfig) { c.Status = "PAUSED" }, "invalid status"},
		{"zero tick", func(c *PerpSymbolConfig) { c.Precision.TickSize = zero }, "tick_size"},
		{"tick too fine for scale", func(c *PerpSymbolConfig) { c.Precision.TickSize = dec.New("0.001") }, "price_scale"},
		{"step too fine for scale", func(c *PerpSymbolConfig) { c.Precision.QtyStep = dec.New("0.00001") }, "qty_scale"},
		{"min>max price", func(c *PerpSymbolConfig) { c.OrderLimits.MinPrice = dec.New("2000000") }, "min_price > max_price"},
		{"no tiers", func(c *PerpSymbolConfig) { c.RiskTiers = nil }, "risk tier"},
		{"bad risk_id", func(c *PerpSymbolConfig) { c.RiskTiers[1].RiskID = 3 }, "risk_id"},
		{"open-ended non-last", func(c *PerpSymbolConfig) { c.RiskTiers[0].MaxNotional = zero }, "open-ended"},
		{"caps not ascending", func(c *PerpSymbolConfig) {
			c.RiskTiers[1].MaxNotional = dec.New("40000")
		}, "ascending"},
		{"mmr decreases across tiers", func(c *PerpSymbolConfig) {
			c.RiskTiers[1].MaintMarginRatio = dec.New("0.004")
		}, "decreases"},
		{"leverage increases across tiers", func(c *PerpSymbolConfig) {
			// Keep MMR×lev < 1 so the per-tier check passes and the
			// cross-tier monotonicity check is what fires.
			c.RiskTiers[1].MaintMarginRatio = dec.New("0.005")
			c.RiskTiers[1].MaxLeverage = dec.New("101")
		}, "increases"},
		{"mmr*lev >= 1", func(c *PerpSymbolConfig) {
			c.RiskTiers[0].MaintMarginRatio = dec.New("0.01")
			c.RiskTiers[1].MaintMarginRatio = dec.New("0.01")
		}, "born liquidatable"},
		{"zero funding interval", func(c *PerpSymbolConfig) { c.Funding.IntervalSeconds = 0 }, "interval_seconds"},
		{"positive floor", func(c *PerpSymbolConfig) { c.Funding.Floor = dec.New("0.001") }, "floor"},
		{"bad premium source", func(c *PerpSymbolConfig) { c.Funding.PremiumSource = "oracle" }, "premium_source"},
		{"alpha out of range", func(c *PerpSymbolConfig) { c.Pricing.MarkEmaAlpha = dec.New("1.5") }, "mark_ema_alpha"},
		{"quorum exceeds sources", func(c *PerpSymbolConfig) {
			c.Pricing.IndexSources = []IndexSource{{Name: "self:BTC-USDT", Weight: dec.FromInt(1)}}
			c.Pricing.IndexQuorum = 2
		}, "index_quorum"},
		{"duplicate index source", func(c *PerpSymbolConfig) {
			c.Pricing.IndexSources = []IndexSource{
				{Name: "self:BTC-USDT", Weight: dec.FromInt(1)},
				{Name: "self:BTC-USDT", Weight: dec.FromInt(1)},
			}
		}, "duplicate"},
		{"taker fee negative", func(c *PerpSymbolConfig) { c.Fees.TakerFeeRate = dec.New("-0.001") }, "taker_fee_rate"},
		{"maker above taker", func(c *PerpSymbolConfig) {
			c.Fees.MakerFeeRate = dec.New("0.001")
		}, "maker_fee_rate must be <="},
		{"taker fee above tier mmr", func(c *PerpSymbolConfig) {
			// ADR-0079 §4: closing a healthy position must release enough
			// equity (≥ MM) to pay its closing fee.
			c.Fees.TakerFeeRate = dec.New("0.006")
		}, "below fees.taker_fee_rate"},
		{"bad reference source", func(c *PerpSymbolConfig) { c.PriceProtection.ReferencePriceSource = "oracle" }, "reference_price_source"},
		{"bad risk apply", func(c *PerpSymbolConfig) { c.RiskApply = "LAZY" }, "risk_apply"},
		{"policy without immediate", func(c *PerpSymbolConfig) {
			c.RepricePolicy = &RiskRepricePolicy{PolicyID: "p1"}
		}, "reprice_policy only applies"},
		{"policy without id", func(c *PerpSymbolConfig) {
			c.RiskApply = RiskApplyImmediate
			c.RepricePolicy = &RiskRepricePolicy{}
		}, "policy_id"},
	}
	for _, tc := range cases {
		c := validConfig()
		tc.mutate(&c)
		err := ValidateConfig(validSpec(), c)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want contains %q", tc.name, err, tc.want)
		}
	}
}

func TestTightensRisk(t *testing.T) {
	base := validConfig().RiskTiers
	if TightensRisk(nil, base) {
		t.Error("first table must not count as tightening")
	}
	if TightensRisk(base, validConfig().RiskTiers) {
		t.Error("identical tables must not tighten")
	}
	loosen := validConfig().RiskTiers
	loosen[0].MaxLeverage = dec.New("125")
	if TightensRisk(base, loosen) {
		t.Error("raising max_leverage must not tighten")
	}
	for name, mutate := range map[string]func([]RiskTier){
		"raise mmr":       func(ts []RiskTier) { ts[0].MaintMarginRatio = dec.New("0.006") },
		"lower leverage":  func(ts []RiskTier) { ts[0].MaxLeverage = dec.New("75") },
		"shrink cap":      func(ts []RiskTier) { ts[0].MaxNotional = dec.New("40000") },
		"bound open tier": func(ts []RiskTier) { ts[1].MaxNotional = dec.New("99999999") },
	} {
		next := validConfig().RiskTiers
		mutate(next)
		if !TightensRisk(base, next) {
			t.Errorf("%s: not detected as tightening", name)
		}
	}
	// Structural change (tier count) is conservatively tightening.
	if !TightensRisk(base, base[:1]) {
		t.Error("dropping a tier must count as tightening")
	}
	// The guardrail: IMMEDIATE tightening demands a policy.
	c := validConfig()
	c.RiskApply = RiskApplyImmediate
	c.RiskTiers[0].MaxLeverage = dec.New("75")
	if err := ValidateRiskApply(base, c); err == nil {
		t.Fatal("IMMEDIATE tightening without policy must fail")
	}
	c.RepricePolicy = &RiskRepricePolicy{PolicyID: "p-1", MaxAffectedAccounts: 10}
	if err := ValidateRiskApply(base, c); err != nil {
		t.Fatalf("IMMEDIATE tightening with policy rejected: %v", err)
	}
	// Loosening IMMEDIATE needs no policy.
	c2 := validConfig()
	c2.RiskApply = RiskApplyImmediate
	c2.RiskTiers[0].MaxLeverage = dec.New("125")
	if err := ValidateRiskApply(base, c2); err != nil {
		t.Fatalf("IMMEDIATE loosening rejected: %v", err)
	}
}

func TestCheckOrder(t *testing.T) {
	c := validConfig()
	pass := func(price, qty string, isMarket bool) string {
		return c.CheckOrder(dec.New(price), dec.New(qty), isMarket)
	}
	if r := pass("50000", "0.01", false); r != "" {
		t.Fatalf("valid order rejected: %s", r)
	}
	cases := []struct {
		price, qty string
		isMarket   bool
		want       string
	}{
		{"50000.3", "0.01", false, RejectInvalidPriceTick},
		{"50000", "0.0015", false, RejectInvalidLotSize},
		{"0.5", "0.001", false, RejectMinNotional}, // on grid, above min price, but 0.0005 notional
		{"2000000", "0.01", false, RejectMaxPrice},
		{"50000", "0.0001", false, RejectInvalidLotSize},
		{"50000", "101", false, RejectMaxOrderQty},
		// Market orders skip the price grid / bounds, keep qty checks.
		{"0", "0.0015", true, RejectInvalidLotSize},
		{"0", "0.01", true, ""},
	}
	for _, tc := range cases {
		if got := pass(tc.price, tc.qty, tc.isMarket); got != tc.want {
			t.Errorf("CheckOrder(%s, %s, market=%v) = %q, want %q", tc.price, tc.qty, tc.isMarket, got, tc.want)
		}
	}
	// Sub-min qty that is ON the step grid hits the explicit min check.
	c2 := validConfig()
	c2.OrderLimits.MinOrderQty = dec.New("0.01")
	if got := c2.CheckOrder(dec.New("50000"), dec.New("0.005"), false); got != RejectMinOrderQty {
		t.Errorf("min qty: got %q", got)
	}
}

func TestCheckVersion(t *testing.T) {
	if v := CheckVersion(0, false, 3); v != VersionUnknown {
		t.Errorf("missing local = %v", v)
	}
	if v := CheckVersion(3, true, 3); v != VersionMatch {
		t.Errorf("equal = %v", v)
	}
	if v := CheckVersion(2, true, 3); v != VersionTooNew {
		t.Errorf("local behind = %v", v)
	}
	if v := CheckVersion(4, true, 3); v != VersionStale {
		t.Errorf("local ahead = %v", v)
	}
}

func TestRiskModelConversion(t *testing.T) {
	c := validConfig()
	m := c.RiskModel()
	if m.TierCount() != 2 {
		t.Fatalf("tier count = %d", m.TierCount())
	}
	// Tier 1 territory.
	if got := m.MMR(dec.New("10000")); !dec.Equal(got, dec.New("0.005")) {
		t.Errorf("MMR(10k) = %s", got)
	}
	// Open-ended tier 2 territory.
	if got := m.MMR(dec.New("100000")); !dec.Equal(got, dec.New("0.01")) {
		t.Errorf("MMR(100k) = %s", got)
	}
	if got := m.MaxLeverage(dec.New("100000")); !dec.Equal(got, dec.New("50")) {
		t.Errorf("MaxLeverage(100k) = %s", got)
	}
}
