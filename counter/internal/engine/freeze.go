package engine

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xargin/opentrade/pkg/dec"
)

// Errors surfaced by freeze / settlement computation.
var (
	ErrInvalidSymbol = errors.New("symbol must be BASE-QUOTE (e.g. BTC-USDT)")
	ErrInvalidSide   = errors.New("invalid order side")
	ErrInvalidQty    = errors.New("qty must be > 0")
	ErrInvalidPrice  = errors.New("price must be > 0 for limit orders")
	ErrMarketInMVP   = errors.New("market orders not supported yet")
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
// MVP-3 supports Limit orders only. For:
//   - Limit Buy:  freeze quote = price * qty
//   - Limit Sell: freeze base  = qty
//
// Market orders will need an estimator (e.g. best price * buffer) and are
// deferred to a later MVP.
func ComputeFreeze(symbol string, side Side, typ OrderType, price, qty dec.Decimal) (asset string, amount dec.Decimal, err error) {
	base, quote, err := SymbolAssets(symbol)
	if err != nil {
		return "", dec.Zero, err
	}
	if !dec.IsPositive(qty) {
		return "", dec.Zero, ErrInvalidQty
	}
	if typ == OrderTypeMarket {
		return "", dec.Zero, ErrMarketInMVP
	}
	if typ != OrderTypeLimit {
		return "", dec.Zero, fmt.Errorf("unsupported order type: %d", typ)
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
}
