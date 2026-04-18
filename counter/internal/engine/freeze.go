package engine

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
// Supported shapes (ADR-0035):
//
//	Limit Buy:             freeze quote = price × qty
//	Limit Sell:            freeze base  = qty
//	Market Sell:           freeze base  = qty  (no price; taker eats asks)
//	Market Buy + quoteQty: freeze quote = quoteQty  (BN quoteOrderQty form)
//
// Market Buy with only `qty` (no quoteQty) is explicitly rejected — estimating
// a freeze cap would require Counter to subscribe to market-data; we keep
// Counter state-machine-only (see ADR-0035 §备选方案 Z).
func ComputeFreeze(symbol string, side Side, typ OrderType, price, qty, quoteQty dec.Decimal) (asset string, amount dec.Decimal, err error) {
	base, quote, err := SymbolAssets(symbol)
	if err != nil {
		return "", dec.Zero, err
	}
	switch typ {
	case OrderTypeLimit:
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
			if !dec.IsPositive(qty) {
				return "", dec.Zero, ErrInvalidQty
			}
			return base, qty, nil
		case SideBid:
			if !dec.IsPositive(quoteQty) {
				return "", dec.Zero, ErrMarketBuyNeedsQuote
			}
			return quote, quoteQty, nil
		default:
			return "", dec.Zero, ErrInvalidSide
		}
	default:
		return "", dec.Zero, fmt.Errorf("unsupported order type: %d", typ)
	}
}
