package etcdcfg

// Precision helpers (ADR-0053 M0).
//
// Pure functions over dec.Decimal + the PrecisionTier / PrecisionChange
// structs declared in etcdcfg.go. Kept separate from the etcd I/O surface so
// counter / match can depend on validation logic without dragging in
// clientv3.

import (
	"errors"
	"fmt"

	"github.com/xargin/opentrade/pkg/dec"
)

// RejectReason is the per-order validation outcome surfaced upwards. Counter
// converts these into its wire-level reject codes; match re-validates as a
// defensive layer (see ADR-0053 § 4 "Defense in Depth"). Kept in pkg/etcdcfg
// rather than match/internal/orderbook so counter can import it without
// pulling in the match module.
type RejectReason string

const (
	// RejectNone means validation passed.
	RejectNone RejectReason = ""
	// RejectNoTier: the supplied reference price falls outside every tier
	// covered by Tiers. Indicates a misconfiguration (first tier should
	// start at 0 and last tier should be open) or a negative price.
	RejectNoTier RejectReason = "no_tier_for_price"
	// RejectInvalidPriceTick: limit-order Price is not a multiple of
	// tier.TickSize.
	RejectInvalidPriceTick RejectReason = "invalid_price_tick"
	// RejectPriceOutOfRange: Price falls outside [MinPrice, MaxPrice] when
	// such fields are added (reserved — currently only NoTier fires).
	RejectPriceOutOfRange RejectReason = "price_out_of_range"
	// RejectInvalidLotSize: base Qty is not a multiple of tier.StepSize.
	RejectInvalidLotSize RejectReason = "invalid_lot_size"
	// RejectInvalidQuoteStep: QuoteQty (market buy by quote) is not a
	// multiple of tier.QuoteStepSize.
	RejectInvalidQuoteStep RejectReason = "invalid_quote_step"
	// RejectMinQty: base Qty below tier.MinQty / MarketMinQty.
	RejectMinQty RejectReason = "min_qty"
	// RejectMaxQty: base Qty above tier.MaxQty / MarketMaxQty.
	RejectMaxQty RejectReason = "max_qty"
	// RejectMinQuoteQty: QuoteQty below tier.MinQuoteQty.
	RejectMinQuoteQty RejectReason = "min_quote_qty"
	// RejectMinQuoteAmount: price*qty (or reference*qty for MARKET with
	// EnforceMinQuoteAmountOnMarket=true) below tier.MinQuoteAmount.
	RejectMinQuoteAmount RejectReason = "min_quote_amount"
	// RejectMaxOpenLimitOrders: the caller would exceed the per-(user,
	// symbol) active-LIMIT-order cap (ADR-0054, SymbolConfig
	// MaxOpenLimitOrders or service default).
	RejectMaxOpenLimitOrders RejectReason = "max_open_limit_orders_exceeded"
)

// FloorToStep returns the largest multiple of step that does not exceed v.
// Step must be positive; a non-positive step is a programming error and
// panics.
//
// Example: FloorToStep(0.00012345, 0.00001) = 0.00012.
func FloorToStep(v, step dec.Decimal) dec.Decimal {
	mustPositiveStep(step)
	// Truncate(0) drops the fractional part of v/step, giving floor for
	// non-negative v. shopspring/decimal's Truncate rounds toward zero, so
	// for negative v we'd need explicit Floor; ADR callers always pass
	// non-negative (price/qty). Keep it simple here.
	q := v.Div(step).Truncate(0)
	return q.Mul(step)
}

// CeilToStep returns the smallest multiple of step >= v. Step must be
// positive.
//
// Example: CeilToStep(0.00012001, 0.00001) = 0.00013.
func CeilToStep(v, step dec.Decimal) dec.Decimal {
	mustPositiveStep(step)
	q := v.Div(step).Ceil()
	return q.Mul(step)
}

// RoundToTick returns the multiple of tick nearest to v (half-up, matching
// Binance's documented behaviour for price display). Tick must be positive.
func RoundToTick(v, tick dec.Decimal) dec.Decimal {
	mustPositiveStep(tick)
	// shopspring/decimal's Round takes fractional places, not a tick. Use
	// divide-round-multiply to stay generic.
	q := v.Div(tick).Round(0)
	return q.Mul(tick)
}

// IsMultipleOfStep reports whether v is an exact integer multiple of step.
// Returns false when step is non-positive rather than panicking, because
// this predicate is used in validation paths where a bad step indicates a
// bad config, not a programming bug in the caller.
func IsMultipleOfStep(v, step dec.Decimal) bool {
	if !dec.IsPositive(step) {
		return false
	}
	return v.Mod(step).Sign() == 0
}

func mustPositiveStep(step dec.Decimal) {
	if !dec.IsPositive(step) {
		panic(fmt.Sprintf("etcdcfg: step must be positive, got %s", step.String()))
	}
}

// FindTier returns the tier whose [PriceFrom, PriceTo) contains price, or
// nil when no tier matches (e.g. negative price, or misconfigured tiers
// that don't cover 0..+∞). Caller should treat nil as RejectNoTier.
//
// Tiers are expected to be validated (monotonic, no gaps) — FindTier does a
// linear scan in declaration order, which is correct for up to a handful of
// tiers (typical case) and keeps the implementation trivial.
func FindTier(tiers []PrecisionTier, price dec.Decimal) *PrecisionTier {
	if len(tiers) == 0 {
		return nil
	}
	if price.Sign() < 0 {
		return nil
	}
	for i := range tiers {
		t := &tiers[i]
		if price.Cmp(t.PriceFrom) < 0 {
			continue
		}
		// PriceTo == 0 marks the open (last) tier, matches any price >= From.
		if dec.IsZero(t.PriceTo) {
			return t
		}
		if price.Cmp(t.PriceTo) < 0 {
			return t
		}
	}
	return nil
}

// ValidateTiers checks that tiers describe a valid monotonic cover from 0
// upward. Returns the first offence.
//
// Rules (ADR-0053 § 3):
//   - at least one tier
//   - first tier's PriceFrom == 0
//   - every non-last tier has PriceTo > PriceFrom and PriceTo == next.PriceFrom
//   - last tier has PriceTo == 0 (open upper bound, "+∞")
//   - every tier has positive TickSize and StepSize
func ValidateTiers(tiers []PrecisionTier) error {
	if len(tiers) == 0 {
		return errors.New("etcdcfg: at least one tier required")
	}
	if !dec.IsZero(tiers[0].PriceFrom) {
		return fmt.Errorf("etcdcfg: first tier PriceFrom must be 0, got %s", tiers[0].PriceFrom.String())
	}
	for i := range tiers {
		t := &tiers[i]
		if !dec.IsPositive(t.TickSize) {
			return fmt.Errorf("etcdcfg: tier[%d] TickSize must be positive, got %s", i, t.TickSize.String())
		}
		if !dec.IsPositive(t.StepSize) {
			return fmt.Errorf("etcdcfg: tier[%d] StepSize must be positive, got %s", i, t.StepSize.String())
		}
		if dec.IsNegative(t.PriceFrom) {
			return fmt.Errorf("etcdcfg: tier[%d] PriceFrom negative: %s", i, t.PriceFrom.String())
		}
		if i < len(tiers)-1 {
			if !dec.IsPositive(t.PriceTo) {
				return fmt.Errorf("etcdcfg: tier[%d] PriceTo must be positive for non-last tier", i)
			}
			if t.PriceTo.Cmp(t.PriceFrom) <= 0 {
				return fmt.Errorf("etcdcfg: tier[%d] PriceTo=%s must exceed PriceFrom=%s",
					i, t.PriceTo.String(), t.PriceFrom.String())
			}
			next := tiers[i+1]
			if t.PriceTo.Cmp(next.PriceFrom) != 0 {
				return fmt.Errorf("etcdcfg: tier[%d] PriceTo=%s must equal tier[%d] PriceFrom=%s (no gap/overlap)",
					i, t.PriceTo.String(), i+1, next.PriceFrom.String())
			}
		} else {
			if !dec.IsZero(t.PriceTo) {
				return fmt.Errorf("etcdcfg: last tier PriceTo must be 0 (open upper bound), got %s", t.PriceTo.String())
			}
		}
	}
	return nil
}

// ValidateTierEvolution checks that newTiers is a legal successor of
// oldTiers under the ADR-0053 "monotonic evolution" rule: tier splits and
// end-appends are allowed; merges / deletes / shifts of existing boundaries
// are rejected.
//
// Concretely, every boundary price that appears in oldTiers (PriceFrom or
// PriceTo) must also appear in newTiers. Both lists must independently pass
// ValidateTiers.
func ValidateTierEvolution(oldTiers, newTiers []PrecisionTier) error {
	if err := ValidateTiers(newTiers); err != nil {
		return fmt.Errorf("etcdcfg: new tiers invalid: %w", err)
	}
	if len(oldTiers) == 0 {
		return nil
	}
	if err := ValidateTiers(oldTiers); err != nil {
		return fmt.Errorf("etcdcfg: old tiers invalid (cannot evolve from invalid): %w", err)
	}

	newBounds := make(map[string]struct{}, len(newTiers)*2)
	for _, t := range newTiers {
		newBounds[t.PriceFrom.String()] = struct{}{}
		if !dec.IsZero(t.PriceTo) {
			newBounds[t.PriceTo.String()] = struct{}{}
		}
	}
	for i, t := range oldTiers {
		if _, ok := newBounds[t.PriceFrom.String()]; !ok {
			return fmt.Errorf("etcdcfg: new tiers drop old boundary PriceFrom=%s (tier[%d]) — merge/delete not allowed",
				t.PriceFrom.String(), i)
		}
		if !dec.IsZero(t.PriceTo) {
			if _, ok := newBounds[t.PriceTo.String()]; !ok {
				return fmt.Errorf("etcdcfg: new tiers drop old boundary PriceTo=%s (tier[%d]) — merge/delete not allowed",
					t.PriceTo.String(), i)
			}
		}
	}
	return nil
}

// OrderKind discriminates validation paths. Counter maps its internal order
// types into these before calling ValidateOrderAgainstTier.
type OrderKind uint8

const (
	// OrderKindUnknown is the zero value and fails validation outright.
	OrderKindUnknown OrderKind = 0
	// OrderKindLimit: limit order, Price and Qty (base) both required.
	OrderKindLimit OrderKind = 1
	// OrderKindMarketByBase: MARKET order with Qty in base asset (MARKET
	// sell, or MARKET buy where the client passed `quantity`). Needs a
	// reference price for MinQuoteAmount estimation when
	// EnforceMinQuoteAmountOnMarket is set.
	OrderKindMarketByBase OrderKind = 2
	// OrderKindMarketBuyByQuote: MARKET buy with QuoteQty in quote asset
	// (ADR-0035 `quoteOrderQty`). Checks QuoteStepSize / MinQuoteQty /
	// MinQuoteAmount.
	OrderKindMarketBuyByQuote OrderKind = 3
)

// OrderValidation bundles the minimal inputs needed by
// ValidateOrderAgainstTier. Caller (counter) is responsible for sourcing
// Price — for MARKET orders this is the reference (mid / rolling average)
// price supplied by the quote service; pkg/etcdcfg does no I/O.
type OrderValidation struct {
	Kind OrderKind
	// Price: limit price (Limit), or reference price (Market*). Must be
	// positive for Tier lookup to succeed.
	Price dec.Decimal
	// Qty: base-asset quantity (Limit / MarketByBase). Unused for
	// MarketBuyByQuote.
	Qty dec.Decimal
	// QuoteQty: quote-asset quantity (MarketBuyByQuote). Unused for other
	// kinds.
	QuoteQty dec.Decimal
}

// ValidateOrderAgainstTier applies the tier's filters to v and returns the
// first reject reason, or RejectNone on success. Compatibility mode (empty
// cfg.Tiers) returns RejectNone without inspecting anything — this is the
// M0 contract that keeps the runtime unchanged until M3.
//
// The boolean return is true iff the result is RejectNone; callers can
// conveniently branch on it without comparing to the empty string.
func ValidateOrderAgainstTier(cfg SymbolConfig, v OrderValidation) (RejectReason, bool) {
	if !cfg.HasPrecision() {
		return RejectNone, true
	}
	tier := FindTier(cfg.Tiers, v.Price)
	if tier == nil {
		return RejectNoTier, false
	}

	switch v.Kind {
	case OrderKindLimit:
		if !IsMultipleOfStep(v.Price, tier.TickSize) {
			return RejectInvalidPriceTick, false
		}
		if !IsMultipleOfStep(v.Qty, tier.StepSize) {
			return RejectInvalidLotSize, false
		}
		if dec.IsPositive(tier.MinQty) && v.Qty.Cmp(tier.MinQty) < 0 {
			return RejectMinQty, false
		}
		if dec.IsPositive(tier.MaxQty) && v.Qty.Cmp(tier.MaxQty) > 0 {
			return RejectMaxQty, false
		}
		if dec.IsPositive(tier.MinQuoteAmount) {
			amount := v.Price.Mul(v.Qty)
			if amount.Cmp(tier.MinQuoteAmount) < 0 {
				return RejectMinQuoteAmount, false
			}
		}
		return RejectNone, true

	case OrderKindMarketByBase:
		if !IsMultipleOfStep(v.Qty, tier.StepSize) {
			return RejectInvalidLotSize, false
		}
		minQty := tier.MarketMinQty
		if !dec.IsPositive(minQty) {
			minQty = tier.MinQty
		}
		if dec.IsPositive(minQty) && v.Qty.Cmp(minQty) < 0 {
			return RejectMinQty, false
		}
		maxQty := tier.MarketMaxQty
		if !dec.IsPositive(maxQty) {
			maxQty = tier.MaxQty
		}
		if dec.IsPositive(maxQty) && v.Qty.Cmp(maxQty) > 0 {
			return RejectMaxQty, false
		}
		if tier.EnforceMinQuoteAmountOnMarket && dec.IsPositive(tier.MinQuoteAmount) {
			amount := v.Price.Mul(v.Qty) // Price = reference supplied by caller
			if amount.Cmp(tier.MinQuoteAmount) < 0 {
				return RejectMinQuoteAmount, false
			}
		}
		return RejectNone, true

	case OrderKindMarketBuyByQuote:
		if dec.IsPositive(tier.QuoteStepSize) && !IsMultipleOfStep(v.QuoteQty, tier.QuoteStepSize) {
			return RejectInvalidQuoteStep, false
		}
		if dec.IsPositive(tier.MinQuoteQty) && v.QuoteQty.Cmp(tier.MinQuoteQty) < 0 {
			return RejectMinQuoteQty, false
		}
		if tier.EnforceMinQuoteAmountOnMarket && dec.IsPositive(tier.MinQuoteAmount) &&
			v.QuoteQty.Cmp(tier.MinQuoteAmount) < 0 {
			// For buy-by-quote, the quote budget IS the order amount —
			// no price multiplication needed.
			return RejectMinQuoteAmount, false
		}
		return RejectNone, true
	}
	return RejectNoTier, false
}
