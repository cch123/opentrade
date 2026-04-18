// Package orderbook implements a price-time priority limit order book for a
// single symbol. The book is NOT thread-safe — per-symbol single-threaded
// access is enforced by the SymbolWorker (see ADR-0016, 0019).
package orderbook

import "github.com/xargin/opentrade/pkg/dec"

// Side designates which side of the book an order sits on.
type Side uint8

const (
	// Bid is the BUY side (quotes to buy).
	Bid Side = 1
	// Ask is the SELL side (quotes to sell).
	Ask Side = 2
)

// String returns a short human-readable form.
func (s Side) String() string {
	switch s {
	case Bid:
		return "bid"
	case Ask:
		return "ask"
	default:
		return "unknown"
	}
}

// Opposite returns the opposite side.
func (s Side) Opposite() Side {
	if s == Bid {
		return Ask
	}
	return Bid
}

// OrderType is a spot-trading order type.
type OrderType uint8

const (
	// Limit is a standard limit order.
	Limit OrderType = 1
	// Market is a market order (no price; consume liquidity until filled or
	// book exhausted).
	Market OrderType = 2
)

// TIF is the time-in-force policy.
type TIF uint8

const (
	// GTC — Good till cancel.
	GTC TIF = 1
	// IOC — Immediate or cancel (take what you can, cancel remainder).
	IOC TIF = 2
	// FOK — Fill or kill (all or nothing).
	FOK TIF = 3
	// PostOnly — Reject if the order would cross the book (i.e. take liquidity).
	PostOnly TIF = 4
)

// Order is the in-memory representation of a live order in the book.
// Fields are intentionally exported so the engine can update Remaining during
// matching.
type Order struct {
	ID        uint64
	UserID    string
	ClientID  string
	Symbol    string
	Side      Side
	Type      OrderType
	TIF       TIF
	Price     dec.Decimal // zero for Market
	Qty       dec.Decimal
	Remaining dec.Decimal // starts equal to Qty, decreases as the order fills
	CreatedAt int64       // nanoseconds (used for monitoring/log; priority is by list position)

	// QuoteQty / RemainingQuote are populated **only** for BN-style market
	// buy orders submitted with quoteOrderQty (ADR-0035). The taker caps
	// how much quote currency it is willing to spend, not how many base
	// units it wants; the engine consumes ask-side liquidity until
	// RemainingQuote reaches zero (or the book is exhausted). Zero for
	// every other shape — matching then drives off Remaining as before.
	QuoteQty       dec.Decimal
	RemainingQuote dec.Decimal
}

// IsQuoteDriven reports whether matching should consume quote currency
// rather than base qty. True only for market buys submitted with
// quoteOrderQty (ADR-0035).
func (o *Order) IsQuoteDriven() bool {
	return o.Type == Market && o.Side == Bid && dec.IsPositive(o.QuoteQty)
}

// IsLive reports whether the order still has unfilled quantity.
func (o *Order) IsLive() bool {
	if o.IsQuoteDriven() {
		return dec.IsPositive(o.RemainingQuote)
	}
	return dec.IsPositive(o.Remaining)
}

// Reject reasons used by the engine (see ADR-0020).
type RejectReason uint8

const (
	RejectNone               RejectReason = 0
	RejectInvalidPriceTick   RejectReason = 1
	RejectInvalidLotSize     RejectReason = 2
	RejectPostOnlyWouldTake  RejectReason = 3
	RejectSelfTradePrevented RejectReason = 4
	RejectSymbolNotTrading   RejectReason = 5
	RejectDuplicateOrderID   RejectReason = 6
	RejectFOKNotFilled       RejectReason = 7
	RejectInternal           RejectReason = 99
)

func (r RejectReason) String() string {
	switch r {
	case RejectNone:
		return "none"
	case RejectInvalidPriceTick:
		return "invalid_price_tick"
	case RejectInvalidLotSize:
		return "invalid_lot_size"
	case RejectPostOnlyWouldTake:
		return "post_only_would_take"
	case RejectSelfTradePrevented:
		return "self_trade_prevented"
	case RejectSymbolNotTrading:
		return "symbol_not_trading"
	case RejectDuplicateOrderID:
		return "duplicate_order_id"
	case RejectFOKNotFilled:
		return "fok_not_filled"
	case RejectInternal:
		return "internal"
	default:
		return "unknown"
	}
}
