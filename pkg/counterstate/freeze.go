package counterstate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xargin/opentrade/pkg/dec"
)

// Errors surfaced by freeze / settlement computation.
var (
	ErrInvalidSymbol       = errors.New("symbol must be BASE-QUOTE (e.g. BTC-USDT)")
	ErrInvalidSide         = errors.New("invalid order side")
	ErrInvalidQty          = errors.New("qty must be > 0")
	ErrInvalidPrice        = errors.New("price must be > 0 for limit orders")
	ErrMarketBuyNeedsQuote = errors.New("market buy requires quote_qty (ADR-0035)")
	// ADR-0083 protected market order shape errors.
	ErrInvalidSlippage      = errors.New("slippage_bps must be in (0, 10000] and only on market orders (ADR-0083)")
	ErrProtectedBuyNeedsCap = errors.New("protected market buy by qty requires quote_cap (ADR-0083)")
	ErrQuoteCapNotAllowed   = errors.New("quote_cap is only valid on a protected market buy by qty (ADR-0083)")
	ErrProtectedQuoteHasCap = errors.New("market buy by quote_qty must not carry quote_cap — the budget is the cap (ADR-0083)")
)

// SymbolAssets extracts (base, quote) from a "BASE-QUOTE" symbol. Returns
// ErrInvalidSymbol if the format is wrong.
func SymbolAssets(symbol string) (base, quote string, err error) {
	parts := strings.Split(symbol, "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("%w: %q", ErrInvalidSymbol, symbol)
	}
	return parts[0], parts[1], nil
}

// ComputeFreeze returns (asset, amount) that an order must freeze on placement.
//
// Supported shapes (ADR-0035 + ADR-0083):
//
//	Limit Buy:                          freeze quote = price × qty
//	Limit Sell:                         freeze base  = qty
//	Market Sell:                        freeze base  = qty  (no price; taker eats asks)
//	Market Buy + quoteQty:              freeze quote = quoteQty  (BN quoteOrderQty form)
//	Protected Market Buy + qty + cap:   freeze quote = quoteCap  (ADR-0083; slippageBps > 0)
//
// Market Buy with only `qty` (no quoteQty) is rejected unless it is an
// ADR-0083 protected order with an explicit quoteCap: Counter deliberately
// does not keep price / order-book context (ADR-0035 §备选方案 Z), so the
// user must commit to a max quote spend up-front; Match then bounds the
// execution by min(collar, quoteCap/qty) — INV-1 — so the actual spend never
// exceeds this freeze.
//
// slippageBps is validated here (single source of truth for order-shape
// rules): it must be 0 on non-market orders and within (0, 10000] when set.
func ComputeFreeze(symbol string, side Side, typ OrderType, price, qty, quoteQty, quoteCap dec.Decimal, slippageBps uint32) (asset string, amount dec.Decimal, err error) {
	base, quote, err := SymbolAssets(symbol)
	if err != nil {
		return "", dec.Zero, err
	}
	if slippageBps > 10_000 || (slippageBps > 0 && typ != OrderTypeMarket) {
		return "", dec.Zero, ErrInvalidSlippage
	}
	switch typ {
	case OrderTypeLimit:
		if dec.IsPositive(quoteCap) {
			return "", dec.Zero, ErrQuoteCapNotAllowed
		}
		if !dec.IsPositive(qty) {
			return "", dec.Zero, ErrInvalidQty
		}
		if !dec.IsPositive(price) {
			return "", dec.Zero, ErrInvalidPrice
		}
		switch side {
		case SideBid:
			return quote, price.Mul(qty), nil
		case SideAsk:
			return base, qty, nil
		default:
			return "", dec.Zero, ErrInvalidSide
		}
	case OrderTypeMarket:
		switch side {
		case SideAsk:
			if dec.IsPositive(quoteCap) {
				return "", dec.Zero, ErrQuoteCapNotAllowed
			}
			if !dec.IsPositive(qty) {
				return "", dec.Zero, ErrInvalidQty
			}
			return base, qty, nil
		case SideBid:
			if dec.IsPositive(quoteQty) {
				if dec.IsPositive(quoteCap) {
					return "", dec.Zero, ErrProtectedQuoteHasCap
				}
				return quote, quoteQty, nil
			}
			// ADR-0083: market buy by base qty — only as a protected order
			// with an explicit quote_cap to freeze.
			if slippageBps > 0 {
				if !dec.IsPositive(qty) {
					return "", dec.Zero, ErrInvalidQty
				}
				if !dec.IsPositive(quoteCap) {
					return "", dec.Zero, ErrProtectedBuyNeedsCap
				}
				return quote, quoteCap, nil
			}
			if dec.IsPositive(quoteCap) {
				return "", dec.Zero, ErrQuoteCapNotAllowed
			}
			return "", dec.Zero, ErrMarketBuyNeedsQuote
		default:
			return "", dec.Zero, ErrInvalidSide
		}
	default:
		return "", dec.Zero, fmt.Errorf("unsupported order type: %d", typ)
	}
}
