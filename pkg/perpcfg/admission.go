package perpcfg

import (
	"github.com/xargin/opentrade/pkg/dec"
)

// Order-admission reject reasons (string form). perp-counter returns these
// verbatim in PlaceOrderResponse.reject_reason; Match maps the subset it
// re-checks onto the trade-event RejectReason enum. Keeping one vocabulary
// here so the two admission layers never describe the same failure
// differently.
const (
	RejectInvalidPriceTick = "invalid_price_tick"
	RejectInvalidLotSize   = "invalid_lot_size"
	RejectMinPrice         = "min_price"
	RejectMaxPrice         = "max_price"
	RejectMinOrderQty      = "min_order_qty"
	RejectMaxOrderQty      = "max_order_qty"
	RejectMinNotional      = "min_notional"
	RejectSymbolStatus     = "symbol_status_forbids" // status machine refused the op
)

// CheckOrder validates an order's shape against this config version's
// precision and order limits (ADR-0075 §1/§2). price is zero for market
// orders — price-grid and price-bound checks are skipped for them, notional
// checks use the supplied reference price when positive. Returns "" on pass
// or one of the Reject* constants.
func (c *PerpSymbolConfig) CheckOrder(price, qty dec.Decimal, isMarket bool) string {
	if !isMarket {
		if !multipleOf(price, c.Precision.TickSize) {
			return RejectInvalidPriceTick
		}
		if c.OrderLimits.MinPrice.Sign() > 0 && price.Cmp(c.OrderLimits.MinPrice) < 0 {
			return RejectMinPrice
		}
		if c.OrderLimits.MaxPrice.Sign() > 0 && price.Cmp(c.OrderLimits.MaxPrice) > 0 {
			return RejectMaxPrice
		}
	}
	if !multipleOf(qty, c.Precision.QtyStep) {
		return RejectInvalidLotSize
	}
	if c.OrderLimits.MinOrderQty.Sign() > 0 && qty.Cmp(c.OrderLimits.MinOrderQty) < 0 {
		return RejectMinOrderQty
	}
	if c.OrderLimits.MaxOrderQty.Sign() > 0 && qty.Cmp(c.OrderLimits.MaxOrderQty) > 0 {
		return RejectMaxOrderQty
	}
	if c.OrderLimits.MinNotional.Sign() > 0 && price.Sign() > 0 &&
		price.Mul(qty).Cmp(c.OrderLimits.MinNotional) < 0 {
		return RejectMinNotional
	}
	return ""
}

func multipleOf(v, step dec.Decimal) bool {
	if step.Sign() <= 0 {
		return true
	}
	return v.Mod(step).Sign() == 0
}

// VersionCheck is the four-way outcome of the ADR-0075 §1 config_version
// handshake Match runs on every version-stamped order.
type VersionCheck int

const (
	// VersionMatch — local active version equals the order's stamp; continue.
	VersionMatch VersionCheck = iota
	// VersionTooNew — the order was admitted under a NEWER version than the
	// local cache holds (local < order). Reject config_version_too_new; the
	// cache will catch up.
	VersionTooNew
	// VersionStale — the order was admitted under an OLDER version (local >
	// order). Reject stale_order_config; the counter must re-admit.
	VersionStale
	// VersionUnknown — the symbol has no config in the local cache. Reject
	// unknown_symbol_config (fail-closed).
	VersionUnknown
)

// CheckVersion implements the handshake branch:
//
//	local == order → VersionMatch
//	local <  order → VersionTooNew
//	local >  order → VersionStale
//	no local cfg   → VersionUnknown
func CheckVersion(localVersion uint64, haveLocal bool, orderVersion uint64) VersionCheck {
	switch {
	case !haveLocal:
		return VersionUnknown
	case localVersion == orderVersion:
		return VersionMatch
	case localVersion < orderVersion:
		return VersionTooNew
	default:
		return VersionStale
	}
}
