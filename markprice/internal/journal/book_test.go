package journal

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
)

func lvl(price, size string) *eventpb.OrderBookLevel {
	return &eventpb.OrderBookLevel{Price: price, Qty: size}
}

func TestBook_Mid(t *testing.T) {
	b := NewBook()
	// Bids descending, asks ascending → mid from the best of each.
	b.ApplyFull("BTC-USDT", &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{lvl("100", "1"), lvl("99", "1")},
		Asks: []*eventpb.OrderBookLevel{lvl("102", "1"), lvl("103", "1")},
	})
	if m, ok := b.Mid("BTC-USDT"); !ok || m.String() != "101" {
		t.Fatalf("two-sided mid = %s ok=%v, want 101", m.String(), ok)
	}
	b.ApplyFull("ETH", &eventpb.OrderBookFull{Bids: []*eventpb.OrderBookLevel{lvl("50", "1")}})
	if m, ok := b.Mid("ETH"); !ok || m.String() != "50" {
		t.Fatalf("bid-only mid = %s, want 50", m.String())
	}
	if _, ok := b.Mid("NOPE"); ok {
		t.Fatal("unknown symbol should report ok=false")
	}
}

func TestBook_EmptyFullKeepsLast(t *testing.T) {
	b := NewBook()
	b.ApplyFull("BTC", &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{lvl("100", "1")}, Asks: []*eventpb.OrderBookLevel{lvl("100", "1")}})
	b.ApplyFull("BTC", &eventpb.OrderBookFull{}) // empty → ignored
	if m, ok := b.Mid("BTC"); !ok || m.String() != "100" {
		t.Fatalf("empty Full should keep last book, got %s ok=%v", m.String(), ok)
	}
}

func TestBook_ImpactPrices_FullFillIsVWAP(t *testing.T) {
	b := NewBook()
	// Asks 100×1 + 120×1 (value 220); bids 90×1 + 80×1 (value 170).
	b.ApplyFull("BTC-USDT-PERP", &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{lvl("90", "1"), lvl("80", "1")},
		Asks: []*eventpb.OrderBookLevel{lvl("100", "1"), lvl("120", "1")},
	})
	// notional 220 consumes both asks → impactAsk = 220/2 = 110;
	// notional 170 consumes both bids → impactBid = 170/2 = 85.
	bid, ask, ok := b.ImpactPrices("BTC-USDT-PERP", dec.New("220"))
	if !ok {
		t.Fatal("expected ok with two-sided book")
	}
	if ask.String() != "110" {
		t.Fatalf("impactAsk = %s, want 110 (VWAP of 100,120)", ask.String())
	}
	// bid side: notional 220 exceeds bid depth (170) → VWAP of all bids = 85.
	if bid.String() != "85" {
		t.Fatalf("impactBid = %s, want 85 (VWAP of 90,80)", bid.String())
	}
}

func TestBook_ImpactPrices_PartialAndSingleLevel(t *testing.T) {
	b := NewBook()
	b.ApplyFull("X", &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{lvl("100", "5")},
		Asks: []*eventpb.OrderBookLevel{lvl("100", "5")},
	})
	// Single level at 100, notional 200 < level value 500 → impact = 100.
	bid, ask, ok := b.ImpactPrices("X", dec.New("200"))
	if !ok || ask.String() != "100" || bid.String() != "100" {
		t.Fatalf("single-level impact = bid %s / ask %s ok=%v, want 100/100", bid, ask, ok)
	}
}

func TestBook_ImpactPrices_OneSidedNotOK(t *testing.T) {
	b := NewBook()
	b.ApplyFull("X", &eventpb.OrderBookFull{Asks: []*eventpb.OrderBookLevel{lvl("100", "1")}})
	if _, _, ok := b.ImpactPrices("X", dec.New("50")); ok {
		t.Fatal("one-sided book should report ok=false (no premium sample)")
	}
	if _, _, ok := b.ImpactPrices("NOPE", dec.New("50")); ok {
		t.Fatal("unknown symbol should report ok=false")
	}
}
