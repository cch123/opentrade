// Package main — opentrade-cli precision recommend subcommand (ADR-0053 M2).
//
// This is an offline recommender: given a symbol's typical price (e.g.
// 7-day median spot mid-price supplied by ops), it emits a single-tier
// PrecisionTier JSON block that can be pasted into a PUT /admin/symbols
// body. It does NOT reach out to quote / MySQL — keeping ops in the loop
// for the final value.
//
// Recommendation rules (informed by BN / OKX / Bybit spot config):
//   - TickSize = 10^(⌊log10(price)⌋ - 5):  5 significant digits of price
//   - StepSize = 10^(⌊log10(minQty_raw)⌋ - 1): one extra digit beyond MinQty
//     where minQty_raw = MinQuoteAmount / price
//   - MinQty   = CeilToStep(minQty_raw, StepSize)
//   - QuoteStepSize = 0.01 (USDT / USD stablecoin convention)
//   - MinQuoteQty   = max(1, MinQuoteAmount) — order's quote budget must at
//     least clear the order amount
//   - MinQuoteAmount = input (defaults to 5, aligned with BN spot)
//   - MarketMinQty / MarketMaxQty = MinQty / (MaxQty*0.5) — stricter than
//     limit to dampen MARKET slippage
//   - MaxQty = 10^12 (effectively unlimited; tighten per symbol if needed)
package main

import (
	"fmt"
	"math"

	"github.com/shopspring/decimal"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// RecommendInput bundles the required inputs.
type RecommendInput struct {
	Symbol     string
	BaseAsset  string
	QuoteAsset string
	// Price is the typical / median spot price. Must be > 0.
	Price dec.Decimal
	// MinQuoteAmount is the desired floor order amount in quote units.
	// Defaults to 5 (BN spot convention) when zero.
	MinQuoteAmount dec.Decimal
	// QuoteStepSize defaults to 0.01 when zero.
	QuoteStepSize dec.Decimal
}

// Recommend produces a single-tier SymbolConfig fragment sized around
// price. Returns an error on non-positive price.
func Recommend(in RecommendInput) (etcdcfg.SymbolConfig, error) {
	if !dec.IsPositive(in.Price) {
		return etcdcfg.SymbolConfig{}, fmt.Errorf("price must be positive, got %s", in.Price.String())
	}
	minAmt := in.MinQuoteAmount
	if !dec.IsPositive(minAmt) {
		minAmt = dec.New("5")
	}
	quoteStep := in.QuoteStepSize
	if !dec.IsPositive(quoteStep) {
		quoteStep = dec.New("0.01")
	}

	priceF, _ := in.Price.Float64()
	// TickSize: 5 significant-digit price resolution → 10^(floor(log10(p)) - 5)
	tickExp := int(math.Floor(math.Log10(priceF))) - 5
	tickSize := decimal.New(1, int32(tickExp))

	// MinQty "raw" target: minAmt / price
	rawMinQty := minAmt.Div(in.Price)
	rawF, _ := rawMinQty.Float64()
	if rawF <= 0 {
		return etcdcfg.SymbolConfig{}, fmt.Errorf("derived min_qty non-positive")
	}
	// StepSize: one digit finer than MinQty magnitude
	stepExp := int(math.Floor(math.Log10(rawF))) - 1
	stepSize := decimal.New(1, int32(stepExp))
	minQty := etcdcfg.CeilToStep(rawMinQty, stepSize)

	// MaxQty: generous default; ops should tighten per symbol.
	maxQty := dec.New("1000000000000") // 10^12

	// MarketMinQty = MinQty (same); MarketMaxQty = 50% of MaxQty to
	// dampen MARKET slippage vs LIMIT's wider ceiling.
	marketMaxQty := maxQty.Div(dec.New("2"))

	// MinQuoteQty: floor for MARKET-buy-by-quote quote budget.
	minQuoteQty := dec.Max(dec.New("1"), minAmt)

	tier := etcdcfg.PrecisionTier{
		PriceFrom:                     dec.New("0"),
		PriceTo:                       dec.New("0"),
		TickSize:                      tickSize,
		StepSize:                      stepSize,
		QuoteStepSize:                 quoteStep,
		MinQty:                        minQty,
		MaxQty:                        maxQty,
		MinQuoteQty:                   minQuoteQty,
		MinQuoteAmount:                minAmt,
		MarketMinQty:                  minQty,
		MarketMaxQty:                  marketMaxQty,
		EnforceMinQuoteAmountOnMarket: true,
		AvgPriceMins:                  5,
	}
	return etcdcfg.SymbolConfig{
		Shard:            "match-0", // placeholder; ops overrides
		Trading:          true,
		BaseAsset:        in.BaseAsset,
		QuoteAsset:       in.QuoteAsset,
		PrecisionVersion: 1,
		Tiers:            []etcdcfg.PrecisionTier{tier},
	}, nil
}
