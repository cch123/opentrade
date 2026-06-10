package counterstate

import "github.com/xargin/opentrade/pkg/dec"

// Side is the side an order sits on.
type Side uint8

const (
	SideBid Side = 1 // BUY
	SideAsk Side = 2 // SELL
)

func (s Side) String() string {
	switch s {
	case SideBid:
		return "bid"
	case SideAsk:
		return "ask"
	default:
		return "unknown"
	}
}

// OrderType classifies an order's matching behavior.
type OrderType uint8

const (
	OrderTypeLimit  OrderType = 1
	OrderTypeMarket OrderType = 2
)

// TIF (Time-In-Force) policy.
type TIF uint8

const (
	// GTC keeps any unfilled remainder live on the book. It is the default
	// resting-order policy, so cancellation or terminal settlement must be
	// driven by a later user or matching-engine event.
	TIFGTC TIF = 1
	// IOC takes immediately available liquidity and expires the unfilled
	// remainder instead of inserting it into the book.
	TIFIOC TIF = 2
	// FOK requires the full quantity to fill immediately; partial execution
	// is rejected so callers do not observe a partially-filled order.
	TIFFOK TIF = 3
	// PostOnly must rest as maker liquidity. If it would cross the book and
	// take liquidity, the order is rejected rather than executed as taker.
	TIFPostOnly TIF = 4
)

// OrderStatus is the internal 8-state order status (ADR-0020).
type OrderStatus uint8

const (
	// OrderStatusUnspecified is the zero-value guardrail. It is not a valid
	// lifecycle state for accepted orders, but keeps proto / restore defaults
	// distinguishable from a real pending order.
	OrderStatusUnspecified OrderStatus = 0
	// OrderStatusPendingNew means Counter accepted and journaled the freeze,
	// but the matching engine has not acknowledged the order yet. Keeping
	// this separate from NEW lets recovery and admin flows tell "not on book
	// yet" apart from "live on book".
	OrderStatusPendingNew OrderStatus = 1
	// OrderStatusNew means the matching engine accepted the order and any
	// remaining quantity is active. This is still non-terminal, so COID dedup
	// and active-limit indexes must continue treating the order as live.
	OrderStatusNew OrderStatus = 2
	// OrderStatusPartiallyFilled keeps execution history visible while the
	// unfilled remainder remains active. It stays non-terminal because later
	// fills, cancels, or expiries may still arrive.
	OrderStatusPartiallyFilled OrderStatus = 3
	// OrderStatusFilled is terminal: no remaining quantity can execute, and
	// active-order indexes can release the clientOrderId / limit slot.
	OrderStatusFilled OrderStatus = 4
	// OrderStatusPendingCancel records that Counter has published a cancel
	// request but final matching-engine resolution has not arrived. The prior
	// active state is kept on the Order so external status can remain stable.
	OrderStatusPendingCancel OrderStatus = 5
	// OrderStatusCanceled is terminal after cancellation wins the race with
	// fills. Any residual frozen funds should be released by the paired
	// unfreeze path before consumers observe the final state.
	OrderStatusCanceled OrderStatus = 6
	// OrderStatusRejected is terminal for orders rejected after Counter
	// accepted the request, so replay can close the order and release active
	// indexes without treating it as a user-side validation reject.
	OrderStatusRejected OrderStatus = 7
	// OrderStatusExpired is terminal for time-in-force or engine expiry. It
	// is separate from CANCELED because the user did not request removal.
	OrderStatusExpired OrderStatus = 8
)

// IsTerminal reports whether status is a final state (FILLED / CANCELED /
// REJECTED / EXPIRED).
func (s OrderStatus) IsTerminal() bool {
	switch s {
	case OrderStatusFilled, OrderStatusCanceled, OrderStatusRejected, OrderStatusExpired:
		return true
	}
	return false
}

func (s OrderStatus) String() string {
	switch s {
	case OrderStatusPendingNew:
		return "pending_new"
	case OrderStatusNew:
		return "new"
	case OrderStatusPartiallyFilled:
		return "partially_filled"
	case OrderStatusFilled:
		return "filled"
	case OrderStatusPendingCancel:
		return "pending_cancel"
	case OrderStatusCanceled:
		return "canceled"
	case OrderStatusRejected:
		return "rejected"
	case OrderStatusExpired:
		return "expired"
	default:
		return "unspecified"
	}
}

// ExternalStatus (ADR-0020) folds PENDING_* into their observable counterparts
// so API / WS consumers see Binance-style statuses.
type ExternalOrderStatus uint8

const (
	ExternalStatusNew             ExternalOrderStatus = 1
	ExternalStatusPartiallyFilled ExternalOrderStatus = 2
	ExternalStatusFilled          ExternalOrderStatus = 3
	ExternalStatusCanceled        ExternalOrderStatus = 4
	ExternalStatusRejected        ExternalOrderStatus = 5
	ExternalStatusExpired         ExternalOrderStatus = 6
)

// Order is the authoritative Counter-side view of a single order.
type Order struct {
	ID            uint64
	ClientOrderID string
	UserID        uint64
	Symbol        string
	Side          Side
	Type          OrderType
	TIF           TIF
	Price         dec.Decimal // empty for Market
	Qty           dec.Decimal
	FilledQty     dec.Decimal

	// QuoteQty is the client-supplied quote budget for BN-style market buy
	// (ADR-0035). Non-zero only for market-buy orders submitted via
	// quoteOrderQty. Used by unfreezeResidual on terminal transitions to
	// refund the unused quote.
	QuoteQty dec.Decimal

	// SlippageBps marks an ADR-0083 protected market order (>0). Counter
	// only needs it at placement (shape validation + the order-event wire
	// stamp); settlement keys off the order shape, not the bps value. For a
	// protected market buy by base qty the user's quote_cap is FrozenAmount.
	SlippageBps uint32

	// Funds reserved for this order.
	FrozenAsset  string
	FrozenAmount dec.Decimal
	// FrozenSpent tracks how much of FrozenAmount has been consumed by
	// fills so far. Settlement adds to it per trade; unfreezeResidual on
	// a terminal transition credits back FrozenAmount − FrozenSpent.
	// Works uniformly for limit / market-sell / market-buy-by-quote
	// (ADR-0035).
	FrozenSpent dec.Decimal

	Status OrderStatus
	// PreCancelStatus captures the status before transitioning to PENDING_CANCEL;
	// used by ExternalStatus so callers see NEW / PARTIALLY_FILLED while a
	// cancel is in flight (ADR-0020).
	PreCancelStatus OrderStatus

	CreatedAt int64
	UpdatedAt int64
}

// IsMarketBuyByQuote reports whether this is a BN-style quote-budget market
// buy. Settlement / freeze refund uses this to pick the right residual
// formula.
func (o *Order) IsMarketBuyByQuote() bool {
	return o.Type == OrderTypeMarket && o.Side == SideBid && dec.IsPositive(o.QuoteQty)
}

// IsMarketBuyByBase reports whether this is an ADR-0083 protected market buy
// by base qty (frozen amount = the user's quote_cap). Like the by-quote
// shape, per-fill settlement consumes match_price × qty from the frozen
// quote — there is no user price to refund improvement against.
func (o *Order) IsMarketBuyByBase() bool {
	return o.Type == OrderTypeMarket && o.Side == SideBid && dec.IsPositive(o.Qty)
}

// Clone returns a deep copy suitable for snapshotting or returning to API
// callers without exposing internal mutation.
func (o *Order) Clone() *Order {
	c := *o
	return &c
}

// RemainingQty = Qty - FilledQty.
func (o *Order) RemainingQty() dec.Decimal { return o.Qty.Sub(o.FilledQty) }

// ExternalStatus maps the internal status to the API-visible status
// (ADR-0020).
func (o *Order) ExternalStatus() ExternalOrderStatus {
	switch o.Status {
	case OrderStatusPendingNew, OrderStatusNew:
		return ExternalStatusNew
	case OrderStatusPartiallyFilled:
		return ExternalStatusPartiallyFilled
	case OrderStatusFilled:
		return ExternalStatusFilled
	case OrderStatusPendingCancel:
		if o.PreCancelStatus == OrderStatusPartiallyFilled {
			return ExternalStatusPartiallyFilled
		}
		return ExternalStatusNew
	case OrderStatusCanceled:
		return ExternalStatusCanceled
	case OrderStatusRejected:
		return ExternalStatusRejected
	case OrderStatusExpired:
		return ExternalStatusExpired
	}
	return ExternalStatusNew
}
