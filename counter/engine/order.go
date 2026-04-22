package engine

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
	TIFGTC      TIF = 1
	TIFIOC      TIF = 2
	TIFFOK      TIF = 3
	TIFPostOnly TIF = 4
)

// OrderStatus is the internal 8-state order status (ADR-0020).
type OrderStatus uint8

const (
	OrderStatusUnspecified     OrderStatus = 0
	OrderStatusPendingNew      OrderStatus = 1
	OrderStatusNew             OrderStatus = 2
	OrderStatusPartiallyFilled OrderStatus = 3
	OrderStatusFilled          OrderStatus = 4
	OrderStatusPendingCancel   OrderStatus = 5
	OrderStatusCanceled        OrderStatus = 6
	OrderStatusRejected        OrderStatus = 7
	OrderStatusExpired         OrderStatus = 8
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
	UserID        string
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

	// TerminatedAt is the ms timestamp when Status first transitioned to
	// a terminal state (Filled / Canceled / Rejected / Expired). Zero
	// means non-terminal. Written once by OrderStore.UpdateStatus on
	// the terminal transition and never updated afterwards. Used by the
	// ADR-0062 evictor to decide when an order has aged out of the
	// retention window and can be removed from byID.
	TerminatedAt int64
}

// IsMarketBuyByQuote reports whether this is a BN-style quote-budget market
// buy. Settlement / freeze refund uses this to pick the right residual
// formula.
func (o *Order) IsMarketBuyByQuote() bool {
	return o.Type == OrderTypeMarket && o.Side == SideBid && dec.IsPositive(o.QuoteQty)
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
