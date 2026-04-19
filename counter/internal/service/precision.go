package service

// ADR-0053 M3 — Counter-side precision enforcement.
//
// validatePrecision consults the symbol's PrecisionTier (via the injected
// SymbolLookup) and rejects orders that fall outside tick / step / min-qty
// / min-amount guardrails. Runs *after* ComputeFreeze in PlaceOrder,
// *before* the sequencer — reject is cheap and doesn't burn a counter
// sequence number (ADR-0018 rule: rejected PlaceOrder paths short-circuit
// outside the sequencer).
//
// Scope (M3 minimal):
//   - LIMIT: full validation (tick, lot, min/max qty, min notional)
//   - MARKET buy by quote (ADR-0035 quoteOrderQty): quote_step_size /
//     min_quote_qty / min_quote_amount. Tier[0] is used for lookup because
//     Counter doesn't subscribe to market data (no mid-price). For
//     single-tier symbols (M2 default) this is exact; multi-tier symbols
//     will be revisited in M3.b when we wire in a mid-price source.
//   - MARKET by base: SKIPPED — requires a reference price to locate the
//     tier, which Counter lacks today. Ops should keep StepSize +
//     MarketMinQty / MarketMaxQty tight enough that the match engine's
//     deterministic rules are acceptable, or push the client through the
//     ADR-0035 Path B (BFF-translated LIMIT+IOC) which *does* carry Price
//     and therefore goes through the LIMIT branch.

import (
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// SymbolLookup returns (cfg, true) when the symbol is registered (admin
// has PUT it), or (zero, false) when unknown. Unknown = skip precision
// enforcement (backward compat); ops should treat "unknown symbol with
// orders" as a monitoring alert.
type SymbolLookup func(symbol string) (etcdcfg.SymbolConfig, bool)

// mapOrderKind translates Counter's (orderType, side, quoteQty) triple into
// the OrderKind expected by etcdcfg.ValidateOrderAgainstTier.
func mapOrderKind(otype engine.OrderType, side engine.Side, quoteQty dec.Decimal) etcdcfg.OrderKind {
	switch otype {
	case engine.OrderTypeLimit:
		return etcdcfg.OrderKindLimit
	case engine.OrderTypeMarket:
		if side == engine.SideBid && dec.IsPositive(quoteQty) {
			return etcdcfg.OrderKindMarketBuyByQuote
		}
		return etcdcfg.OrderKindMarketByBase
	}
	return etcdcfg.OrderKindUnknown
}

// validatePrecision runs the filter chain against a PlaceOrderRequest.
// Returns RejectNone + true on pass (including compatibility mode). Returns
// a non-empty RejectReason + false on reject.
func validatePrecision(cfg etcdcfg.SymbolConfig, req PlaceOrderRequest) (etcdcfg.RejectReason, bool) {
	if !cfg.HasPrecision() {
		return etcdcfg.RejectNone, true
	}
	kind := mapOrderKind(req.OrderType, req.Side, req.QuoteQty)
	switch kind {
	case etcdcfg.OrderKindLimit:
		return etcdcfg.ValidateOrderAgainstTier(cfg, etcdcfg.OrderValidation{
			Kind:  etcdcfg.OrderKindLimit,
			Price: req.Price,
			Qty:   req.Qty,
		})
	case etcdcfg.OrderKindMarketBuyByQuote:
		// Tier[0] (PriceFrom=0) matches price=0 — see etcdcfg.FindTier.
		// M3.b will swap in real mid-price for multi-tier symbols.
		return etcdcfg.ValidateOrderAgainstTier(cfg, etcdcfg.OrderValidation{
			Kind:     etcdcfg.OrderKindMarketBuyByQuote,
			Price:    dec.Zero,
			QuoteQty: req.QuoteQty,
		})
	case etcdcfg.OrderKindMarketByBase:
		// Intentionally skipped in M3. See file-level docstring.
		return etcdcfg.RejectNone, true
	}
	return etcdcfg.RejectNone, true
}
