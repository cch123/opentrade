package journal

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func lvl(price string) *eventpb.OrderBookLevel {
	return &eventpb.OrderBookLevel{Price: price, Qty: "1"}
}

func TestMidBook_ApplyFull(t *testing.T) {
	b := NewMidBook()

	// Both sides: mid = (bestBid + bestAsk) / 2. Bids descending, asks ascending.
	b.ApplyFull("BTC-USDT", &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{lvl("100"), lvl("99")},
		Asks: []*eventpb.OrderBookLevel{lvl("102"), lvl("103")},
	})
	if m, ok := b.Mid("BTC-USDT"); !ok || m.String() != "101" {
		t.Fatalf("two-sided mid = %s ok=%v, want 101", m.String(), ok)
	}

	// Bid-only: falls back to best bid.
	b.ApplyFull("ETH-USDT", &eventpb.OrderBookFull{Bids: []*eventpb.OrderBookLevel{lvl("50")}})
	if m, ok := b.Mid("ETH-USDT"); !ok || m.String() != "50" {
		t.Fatalf("bid-only mid = %s ok=%v, want 50", m.String(), ok)
	}

	// Ask-only: falls back to best ask.
	b.ApplyFull("SOL-USDT", &eventpb.OrderBookFull{Asks: []*eventpb.OrderBookLevel{lvl("8")}})
	if m, ok := b.Mid("SOL-USDT"); !ok || m.String() != "8" {
		t.Fatalf("ask-only mid = %s ok=%v, want 8", m.String(), ok)
	}
}

func TestMidBook_EmptyAndUnknown(t *testing.T) {
	b := NewMidBook()
	if _, ok := b.Mid("NOPE"); ok {
		t.Fatal("unknown symbol should report ok=false")
	}
	// Seed a price, then an empty Full must NOT erase it (transient empty book).
	b.ApplyFull("BTC-USDT", &eventpb.OrderBookFull{Bids: []*eventpb.OrderBookLevel{lvl("100")}, Asks: []*eventpb.OrderBookLevel{lvl("100")}})
	b.ApplyFull("BTC-USDT", &eventpb.OrderBookFull{})
	if m, ok := b.Mid("BTC-USDT"); !ok || m.String() != "100" {
		t.Fatalf("empty Full should keep last mid, got %s ok=%v", m.String(), ok)
	}
	// nil/empty-symbol Full is a no-op.
	b.ApplyFull("", &eventpb.OrderBookFull{Bids: []*eventpb.OrderBookLevel{lvl("1")}})
	b.ApplyFull("X", nil)
	if _, ok := b.Mid(""); ok {
		t.Fatal("empty symbol must not be stored")
	}
}
