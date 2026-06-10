package perpcfg

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// TestStatusProtoMapping pins the Go status set 1:1 onto the shared proto
// enum (ADR-0075 §2: one status set, no per-service drift).
func TestStatusProtoMapping(t *testing.T) {
	// Every Go status maps to a distinct, non-UNSPECIFIED proto value and back.
	seen := map[eventpb.PerpSymbolStatus]bool{}
	for _, s := range AllStatuses() {
		p := s.ToProto()
		if p == eventpb.PerpSymbolStatus_PERP_SYMBOL_STATUS_UNSPECIFIED {
			t.Fatalf("%s maps to UNSPECIFIED", s)
		}
		if seen[p] {
			t.Fatalf("%s maps to duplicate proto value %v", s, p)
		}
		seen[p] = true
		back, ok := StatusFromProto(p)
		if !ok || back != s {
			t.Fatalf("round trip %s -> %v -> %s (ok=%v)", s, p, back, ok)
		}
	}
	// Every non-zero proto value maps to a Go status — additions must touch both.
	for v, name := range eventpb.PerpSymbolStatus_name {
		if v == 0 {
			continue
		}
		if _, ok := StatusFromProto(eventpb.PerpSymbolStatus(v)); !ok {
			t.Fatalf("proto value %s has no Go status", name)
		}
	}
	if len(seen) != len(eventpb.PerpSymbolStatus_name)-1 {
		t.Fatalf("Go set has %d statuses, proto enum has %d non-zero values",
			len(seen), len(eventpb.PerpSymbolStatus_name)-1)
	}
}

func TestCanTransitionMatrix(t *testing.T) {
	allowed := map[[2]Status]bool{}
	add := func(from Status, tos ...Status) {
		for _, to := range tos {
			allowed[[2]Status{from, to}] = true
		}
	}
	// The ADR-0075 §2 matrix, written out edge by edge.
	add(StatusPreopen, StatusTrading)
	add(StatusTrading, StatusPostOnly, StatusCancelOnly, StatusPreDelivery)
	add(StatusPostOnly, StatusTrading, StatusCancelOnly)
	add(StatusCancelOnly, StatusTrading, StatusSettling, StatusDelisted)
	add(StatusPreDelivery, StatusSettling, StatusSettlingHalted)
	add(StatusSettling, StatusSettlingHalted, StatusDelivered)
	add(StatusSettlingHalted, StatusSettling)
	add(StatusDelivered, StatusDelisted)

	for _, from := range AllStatuses() {
		for _, to := range AllStatuses() {
			want := from == to || allowed[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestStatusAdmissionPredicates(t *testing.T) {
	type want struct {
		place           bool // plain GTC order
		placePostOnly   bool
		placeReduceOnly bool
		cancel          bool
		tradable        bool
	}
	cases := map[Status]want{
		StatusPreopen:        {false, false, false, false, false},
		StatusTrading:        {true, true, true, true, true},
		StatusPostOnly:       {false, true, false, true, true},
		StatusCancelOnly:     {false, false, false, true, false},
		StatusPreDelivery:    {false, false, true, true, false},
		StatusSettling:       {false, false, false, false, false},
		StatusSettlingHalted: {false, false, false, false, false},
		StatusDelivered:      {false, false, false, false, false},
		StatusDelisted:       {false, false, false, false, false},
	}
	for s, w := range cases {
		if got := s.CanPlaceOrder(false, false); got != w.place {
			t.Errorf("%s.CanPlaceOrder(plain) = %v, want %v", s, got, w.place)
		}
		if got := s.CanPlaceOrder(true, false); got != w.placePostOnly {
			t.Errorf("%s.CanPlaceOrder(post_only) = %v, want %v", s, got, w.placePostOnly)
		}
		if got := s.CanPlaceOrder(false, true); got != w.placeReduceOnly {
			t.Errorf("%s.CanPlaceOrder(reduce_only) = %v, want %v", s, got, w.placeReduceOnly)
		}
		if got := s.CanCancelOrder(); got != w.cancel {
			t.Errorf("%s.CanCancelOrder() = %v, want %v", s, got, w.cancel)
		}
		if got := s.Tradable(); got != w.tradable {
			t.Errorf("%s.Tradable() = %v, want %v", s, got, w.tradable)
		}
	}
	// Match-side: PRE_DELIVERY admits book orders (reduce-only is the
	// counter's gate, pinned by the version handshake).
	if !StatusPreDelivery.BookAllowsPlace(false) {
		t.Error("PRE_DELIVERY.BookAllowsPlace(plain) = false, want true")
	}
	if StatusCancelOnly.BookAllowsPlace(true) {
		t.Error("CANCEL_ONLY.BookAllowsPlace(post_only) = true, want false")
	}
}

func TestParseStatus(t *testing.T) {
	if s, err := ParseStatus("TRADING"); err != nil || s != StatusTrading {
		t.Fatalf("ParseStatus(TRADING) = %v, %v", s, err)
	}
	if _, err := ParseStatus("trading"); err == nil {
		t.Fatal("lowercase status must not parse")
	}
	if _, err := ParseStatus("HALTED"); err == nil {
		t.Fatal("unknown status must not parse")
	}
}
