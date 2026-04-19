package main

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

func TestRecommend_RejectsNonPositivePrice(t *testing.T) {
	for _, p := range []string{"0", "-1"} {
		_, err := Recommend(RecommendInput{Symbol: "X", BaseAsset: "B", QuoteAsset: "Q", Price: dec.New(p)})
		if err == nil {
			t.Errorf("Recommend(price=%s) expected error", p)
		}
	}
}

// TestRecommend_KnownSymbols exercises the recommender on realistic price
// points and asserts the output (a) passes ValidateTiers and (b) accepts a
// "typical" order at that price.
func TestRecommend_KnownSymbols(t *testing.T) {
	cases := []struct {
		name    string
		price   string
		typQty  string // a qty the recommender should accept at `price`
	}{
		{"BTC-USDT", "50000", "0.01"},
		{"ETH-USDT", "3000", "0.1"},
		{"DOGE-USDT", "0.1", "1000"},
		{"SHIB-USDT", "0.00002", "10000000"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := Recommend(RecommendInput{
				Symbol: c.name, BaseAsset: "X", QuoteAsset: "USDT",
				Price: dec.New(c.price),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := etcdcfg.ValidateTiers(cfg.Tiers); err != nil {
				t.Fatalf("ValidateTiers: %v", err)
			}
			tier := cfg.Tiers[0]
			// Sanity: TickSize <= price (cannot have tick larger than price).
			if tier.TickSize.Cmp(dec.New(c.price)) > 0 {
				t.Errorf("TickSize %s > price %s", tier.TickSize.String(), c.price)
			}
			// Sanity: MinQty * price >= MinQuoteAmount (i.e. a min-sized
			// order should pass MinQuoteAmount).
			minAmt := tier.MinQty.Mul(dec.New(c.price))
			if minAmt.Cmp(tier.MinQuoteAmount) < 0 {
				t.Errorf("MinQty*price (%s) < MinQuoteAmount (%s) — recommender sizing off",
					minAmt.String(), tier.MinQuoteAmount.String())
			}

			// Feed a typical order through validator — must pass.
			price := etcdcfg.CeilToStep(dec.New(c.price), tier.TickSize)
			typ := etcdcfg.CeilToStep(dec.New(c.typQty), tier.StepSize)
			if typ.Cmp(tier.MinQty) < 0 {
				typ = tier.MinQty
			}
			reason, ok := etcdcfg.ValidateOrderAgainstTier(cfg, etcdcfg.OrderValidation{
				Kind: etcdcfg.OrderKindLimit, Price: price, Qty: typ,
			})
			if !ok {
				t.Errorf("typical order (price=%s qty=%s) should pass, got %s",
					price.String(), typ.String(), reason)
			}
		})
	}
}

// TestRecommend_OutputShape verifies structural expectations.
func TestRecommend_OutputShape(t *testing.T) {
	cfg, err := Recommend(RecommendInput{
		Symbol: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT",
		Price: dec.New("50000"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PrecisionVersion != 1 {
		t.Errorf("PrecisionVersion=%d", cfg.PrecisionVersion)
	}
	if len(cfg.Tiers) != 1 {
		t.Fatalf("tiers=%d, want 1", len(cfg.Tiers))
	}
	tier := cfg.Tiers[0]
	if !tier.EnforceMinQuoteAmountOnMarket {
		t.Error("EnforceMinQuoteAmountOnMarket default should be true")
	}
	if tier.AvgPriceMins != 5 {
		t.Errorf("AvgPriceMins=%d, want 5", tier.AvgPriceMins)
	}
	// Quote step defaulted.
	if tier.QuoteStepSize.String() != "0.01" {
		t.Errorf("QuoteStepSize=%s", tier.QuoteStepSize.String())
	}
	// MinQuoteAmount defaulted.
	if tier.MinQuoteAmount.String() != "5" {
		t.Errorf("MinQuoteAmount=%s", tier.MinQuoteAmount.String())
	}
}

// TestRecommend_CustomMinAmount ensures overrides flow through.
func TestRecommend_CustomMinAmount(t *testing.T) {
	cfg, err := Recommend(RecommendInput{
		Symbol: "X", BaseAsset: "B", QuoteAsset: "Q",
		Price:          dec.New("100"),
		MinQuoteAmount: dec.New("20"),
		QuoteStepSize:  dec.New("0.001"),
	})
	if err != nil {
		t.Fatal(err)
	}
	tier := cfg.Tiers[0]
	if tier.MinQuoteAmount.String() != "20" {
		t.Errorf("MinQuoteAmount=%s", tier.MinQuoteAmount.String())
	}
	if tier.QuoteStepSize.String() != "0.001" {
		t.Errorf("QuoteStepSize=%s", tier.QuoteStepSize.String())
	}
}
