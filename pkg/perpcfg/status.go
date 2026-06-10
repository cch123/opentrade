// Package perpcfg is the perp contract catalog model (ADR-0075): versioned
// SymbolConfig for perp symbols — contract spec, trading status, precision,
// order limits, risk tiers, funding / pricing params, fees, and price
// protection — authoritatively stored in MySQL and consumed by services
// through a polled, per-symbol atomically swapped cache.
//
// config_version is the cross-service consistency anchor: perp-counter stamps
// the version it admitted an order under into the OrderEvent, and Match
// cross-checks it against its own cache before matching (ADR-0075 §1). The
// version is the generalization and replacement of ADR-0053's
// PrecisionVersion for perp symbols.
package perpcfg

import (
	"fmt"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// Status is the perp symbol trading state machine (ADR-0075 §2). The string
// values are the canonical storage / API form; the semantic anchor is the
// shared proto enum eventpb.PerpSymbolStatus — TestStatusProtoMapping pins the
// two sets 1:1 so they cannot drift.
type Status string

const (
	StatusPreopen        Status = "PREOPEN"
	StatusTrading        Status = "TRADING"
	StatusPostOnly       Status = "POST_ONLY"
	StatusCancelOnly     Status = "CANCEL_ONLY"
	StatusPreDelivery    Status = "PRE_DELIVERY"    // ADR-0076 delivery flow
	StatusSettling       Status = "SETTLING"        // ADR-0076
	StatusSettlingHalted Status = "SETTLING_HALTED" // ADR-0076
	StatusDelivered      Status = "DELIVERED"       // ADR-0076
	StatusDelisted       Status = "DELISTED"
)

// AllStatuses lists every member of the status set, in proto enum order.
func AllStatuses() []Status {
	return []Status{
		StatusPreopen, StatusTrading, StatusPostOnly, StatusCancelOnly,
		StatusPreDelivery, StatusSettling, StatusSettlingHalted,
		StatusDelivered, StatusDelisted,
	}
}

// Valid reports membership in the status set.
func (s Status) Valid() bool {
	switch s {
	case StatusPreopen, StatusTrading, StatusPostOnly, StatusCancelOnly,
		StatusPreDelivery, StatusSettling, StatusSettlingHalted,
		StatusDelivered, StatusDelisted:
		return true
	default:
		return false
	}
}

// legalTransitions is the ADR-0075 §2 migration matrix. Anything not listed
// here must go through the admin emergency path (force + reason/ticket, fully
// audited) — see CanTransition.
var legalTransitions = map[Status][]Status{
	StatusPreopen:        {StatusTrading},
	StatusTrading:        {StatusPostOnly, StatusCancelOnly, StatusPreDelivery},
	StatusPostOnly:       {StatusTrading, StatusCancelOnly},
	StatusCancelOnly:     {StatusTrading, StatusSettling, StatusDelisted},
	StatusPreDelivery:    {StatusSettling, StatusSettlingHalted},
	StatusSettling:       {StatusSettlingHalted, StatusDelivered},
	StatusSettlingHalted: {StatusSettling},
	StatusDelivered:      {StatusDelisted},
	StatusDelisted:       {},
}

// CanTransition reports whether from → to is in the legal migration matrix.
// A self-transition is always allowed (republishing parameters without a
// status change).
func CanTransition(from, to Status) bool {
	if from == to {
		return true
	}
	for _, next := range legalTransitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// CanPlaceOrder is the perp-counter-side admission predicate (ADR-0075 §2):
// it sees the full order shape including reduce_only, which is an account
// concern Match never receives on the wire.
func (s Status) CanPlaceOrder(postOnly, reduceOnly bool) bool {
	switch s {
	case StatusTrading:
		return true
	case StatusPostOnly:
		return postOnly
	case StatusPreDelivery:
		return reduceOnly
	default:
		return false
	}
}

// BookAllowsPlace is the Match-side admission predicate. Match validates only
// orderbook-scope rules (ADR-0075 §2): in PRE_DELIVERY the reduce-only gate is
// perp-counter's responsibility, and the config_version handshake guarantees
// perp-counter admitted under the same status — so the book accepts what the
// counter let through.
func (s Status) BookAllowsPlace(postOnly bool) bool {
	return s.CanPlaceOrder(postOnly, true)
}

// CanCancelOrder reports whether a user cancel is admissible. SETTLING and
// later states only accept system ops — the settlement flow cancels resting
// orders itself; user cancels are rejected to keep the book deterministic.
func (s Status) CanCancelOrder() bool {
	switch s {
	case StatusTrading, StatusPostOnly, StatusCancelOnly, StatusPreDelivery:
		return true
	default:
		return false
	}
}

// Tradable is the legacy `Trading bool` two-state projection (ADR-0075 §2):
// TRADING / POST_ONLY map to tradable, every other status blocks new
// exposure. Derived view only — never authoritative for new features.
func (s Status) Tradable() bool {
	return s == StatusTrading || s == StatusPostOnly
}

// ToProto maps to the shared wire enum.
func (s Status) ToProto() eventpb.PerpSymbolStatus {
	switch s {
	case StatusPreopen:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_PREOPEN
	case StatusTrading:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_TRADING
	case StatusPostOnly:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_POST_ONLY
	case StatusCancelOnly:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_CANCEL_ONLY
	case StatusPreDelivery:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_PRE_DELIVERY
	case StatusSettling:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_SETTLING
	case StatusSettlingHalted:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_SETTLING_HALTED
	case StatusDelivered:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_DELIVERED
	case StatusDelisted:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_DELISTED
	default:
		return eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_UNSPECIFIED
	}
}

// StatusFromProto maps the wire enum back; ok=false for UNSPECIFIED or
// unknown values.
func StatusFromProto(p eventpb.PerpSymbolStatus) (Status, bool) {
	switch p {
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_PREOPEN:
		return StatusPreopen, true
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_TRADING:
		return StatusTrading, true
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_POST_ONLY:
		return StatusPostOnly, true
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_CANCEL_ONLY:
		return StatusCancelOnly, true
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_PRE_DELIVERY:
		return StatusPreDelivery, true
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_SETTLING:
		return StatusSettling, true
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_SETTLING_HALTED:
		return StatusSettlingHalted, true
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_DELIVERED:
		return StatusDelivered, true
	case eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_DELISTED:
		return StatusDelisted, true
	default:
		return "", false
	}
}

// ParseStatus parses the canonical string form.
func ParseStatus(s string) (Status, error) {
	st := Status(s)
	if !st.Valid() {
		return "", fmt.Errorf("perpcfg: unknown status %q", s)
	}
	return st, nil
}
