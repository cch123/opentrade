package sequencer

import (
	"testing"
	"time"

	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/pkg/dec"
)

// ADR-0083: the worker derives the collar from the opposite best price in
// the same handle() pass and stamps the audit fields onto the expired
// emission.

func newProtectedMarketOrder(id, user uint64, side orderbook.Side, qty string, bps uint32, freezeCap string) *orderbook.Order {
	q := dec.New(qty)
	cap := dec.Zero
	if freezeCap != "" {
		cap = dec.New(freezeCap)
	}
	return &orderbook.Order{
		ID:          id,
		UserID:      user,
		Symbol:      "BTC-USDT",
		Side:        side,
		Type:        orderbook.Market,
		TIF:         orderbook.GTC,
		Qty:         q,
		Remaining:   q,
		SlippageBps: bps,
		FreezeCap:   cap,
		CreatedAt:   int64(id),
	}
}

func TestWorkerProtectedBuyCollarFromBestAsk(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	// Asks: 1 @ 100 (best), 1 @ 102. Collar = 100 × 1.01 = 101 → only the
	// 100 level fills; the rest expires with the audit fields stamped.
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, 2001, orderbook.Ask, "100", "1")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(2, 2002, orderbook.Ask, "102", "1")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newProtectedMarketOrder(3, 3001, orderbook.Bid, "2", 100, "300")})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	// accepted(1), accepted(2), trade(3), expired(3)
	if len(got) != 4 {
		t.Fatalf("emissions = %d, want 4: %+v", len(got), got)
	}
	if got[2].Kind != OutputTrade || got[2].Price.String() != "100" || got[2].Qty.String() != "1" {
		t.Fatalf("got[2] = %+v, want trade 1@100", got[2])
	}
	exp := got[3]
	if exp.Kind != OutputOrderExpired || exp.OrderID != 3 {
		t.Fatalf("got[3] = %+v, want expired for order 3", exp)
	}
	if exp.FilledQty.String() != "1" {
		t.Fatalf("expired filled = %s, want 1", exp.FilledQty)
	}
	if exp.ProtectRef.String() != "100" || exp.ProtectLimit.String() != "101" {
		t.Fatalf("protect ref/limit = %s/%s, want 100/101", exp.ProtectRef, exp.ProtectLimit)
	}
}

func TestWorkerProtectedSellCollarFromBestBid(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	// Bids: 1 @ 100 (best), 1 @ 98. Floor = 100 × 0.99 = 99 → the 98 level
	// is below the floor.
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, 2001, orderbook.Bid, "100", "1")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(2, 2002, orderbook.Bid, "98", "1")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newProtectedMarketOrder(3, 3001, orderbook.Ask, "2", 100, "")})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 4 {
		t.Fatalf("emissions = %d, want 4: %+v", len(got), got)
	}
	exp := got[3]
	if exp.Kind != OutputOrderExpired || exp.FilledQty.String() != "1" {
		t.Fatalf("got[3] = %+v, want expired with filled 1", exp)
	}
	if exp.ProtectRef.String() != "100" || exp.ProtectLimit.String() != "99" {
		t.Fatalf("protect ref/limit = %s/%s, want 100/99", exp.ProtectRef, exp.ProtectLimit)
	}
}

func TestWorkerProtectedEmptyBookRejects(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	w.Submit(&Event{Kind: EventOrderPlaced, Order: newProtectedMarketOrder(1, 3001, orderbook.Bid, "1", 50, "1000")})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 1 {
		t.Fatalf("emissions = %d, want 1: %+v", len(got), got)
	}
	if got[0].Kind != OutputOrderRejected || got[0].RejectReason != orderbook.RejectNoBookReference {
		t.Fatalf("got[0] = %+v, want rejected/no_book_reference", got[0])
	}
}

func TestWorkerProtectedBuyFreezeCapTightensLimit(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	// Ask 2 @ 100; collar = 100 × 1.01 = 101 but freeze_cap 150 over qty 2
	// caps the limit at 75 (INV-1: spend can never exceed the freeze) → no
	// level is reachable, the order expires unfilled.
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, 2001, orderbook.Ask, "100", "2")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newProtectedMarketOrder(2, 3001, orderbook.Bid, "2", 100, "150")})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 2 {
		t.Fatalf("emissions = %d, want 2: %+v", len(got), got)
	}
	exp := got[1]
	if exp.Kind != OutputOrderExpired || exp.FilledQty.String() != "0" {
		t.Fatalf("got[1] = %+v, want expired with filled 0", exp)
	}
	if exp.ProtectLimit.String() != "75" || exp.ProtectRef.String() != "100" {
		t.Fatalf("protect limit/ref = %s/%s, want 75/100", exp.ProtectLimit, exp.ProtectRef)
	}
}

func TestWorkerProtectedQuoteDrivenBuyGetsCollar(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	// Quote-driven protected buy: budget 300, asks 1 @ 100 and 1 @ 200.
	// Collar = 100 × 1.005 = 100.5 → only the 100 level is reachable; the
	// leftover budget expires (with the audit stamp).
	q := dec.New("300")
	o := &orderbook.Order{
		ID: 2, UserID: 3001, Symbol: "BTC-USDT",
		Side: orderbook.Bid, Type: orderbook.Market, TIF: orderbook.GTC,
		QuoteQty: q, RemainingQuote: q,
		SlippageBps: 50, FreezeCap: q,
	}
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, 2001, orderbook.Ask, "100", "1")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(3, 2002, orderbook.Ask, "200", "1")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: o})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 4 {
		t.Fatalf("emissions = %d, want 4: %+v", len(got), got)
	}
	if got[2].Kind != OutputTrade || got[2].Price.String() != "100" {
		t.Fatalf("got[2] = %+v, want trade @100", got[2])
	}
	exp := got[3]
	if exp.Kind != OutputOrderExpired {
		t.Fatalf("got[3] = %+v, want expired", exp)
	}
	if exp.ProtectLimit.String() != "100.5" || exp.ProtectRef.String() != "100" {
		t.Fatalf("protect limit/ref = %s/%s, want 100.5/100", exp.ProtectLimit, exp.ProtectRef)
	}
}
