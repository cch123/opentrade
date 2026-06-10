package mysqlstore

import (
	"math"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// TestRejectReasonToString_CoversAllEnumValues sweeps the generated
// eventpb.RejectReason_name map so that adding a new enum value without
// extending rejectReasonToString fails this test instead of silently
// rendering an empty reject reason in history rows.
func TestRejectReasonToString_CoversAllEnumValues(t *testing.T) {
	for v, name := range eventpb.RejectReason_name {
		if v == int32(eventpb.RejectReason_REJECT_REASON_UNSPECIFIED) {
			if got := rejectReasonToString(int8(v)); got != "" {
				t.Errorf("UNSPECIFIED → %q, want empty string", got)
			}
			continue
		}
		if v > math.MaxInt8 {
			t.Fatalf("%s = %d exceeds int8; widen rejectReasonToString and the orders.reject_reason column type", name, v)
		}
		if got := rejectReasonToString(int8(v)); got == "" {
			t.Errorf("%s (%d) → empty string, want a snake_case reason", name, v)
		}
	}
}

// TestRejectReasonToString_MatchCanonStrings pins the exact strings to the
// match-side canon (match/internal/orderbook RejectReason.String()). That
// package is unimportable from here (internal boundary), so the expected
// values are duplicated literally.
func TestRejectReasonToString_MatchCanonStrings(t *testing.T) {
	cases := []struct {
		in   eventpb.RejectReason
		want string
	}{
		{eventpb.RejectReason_REJECT_REASON_INVALID_PRICE_TICK, "invalid_price_tick"},
		{eventpb.RejectReason_REJECT_REASON_INVALID_LOT_SIZE, "invalid_lot_size"},
		{eventpb.RejectReason_REJECT_REASON_POST_ONLY_WOULD_TAKE, "post_only_would_take"},
		{eventpb.RejectReason_REJECT_REASON_SELF_TRADE_PREVENTED, "self_trade_prevented"},
		{eventpb.RejectReason_REJECT_REASON_SYMBOL_NOT_TRADING, "symbol_not_trading"},
		{eventpb.RejectReason_REJECT_REASON_DUPLICATE_ORDER_ID, "duplicate_order_id"},
		{eventpb.RejectReason_REJECT_REASON_FOK_NOT_FILLED, "fok_not_filled"},
		{eventpb.RejectReason_REJECT_REASON_CONFIG_VERSION_TOO_NEW, "config_version_too_new"},
		{eventpb.RejectReason_REJECT_REASON_STALE_ORDER_CONFIG, "stale_order_config"},
		{eventpb.RejectReason_REJECT_REASON_UNKNOWN_SYMBOL_CONFIG, "unknown_symbol_config"},
		{eventpb.RejectReason_REJECT_REASON_SYMBOL_STATUS_FORBIDS, "symbol_status_forbids"},
		{eventpb.RejectReason_REJECT_REASON_PRICE_OUT_OF_RANGE, "price_out_of_range"},
		{eventpb.RejectReason_REJECT_REASON_NO_BOOK_REFERENCE, "no_book_reference"},
		{eventpb.RejectReason_REJECT_REASON_INTERNAL, "internal"},
	}
	for _, c := range cases {
		if got := rejectReasonToString(int8(c.in)); got != c.want {
			t.Errorf("%v → %q, want %q", c.in, got, c.want)
		}
	}
}
