package sequencer

import (
	"context"
	"testing"
	"time"

	"github.com/xargin/opentrade/match/internal/engine"
	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/pkg/dec"
)

func newLimitOrder(id uint64, user string, side orderbook.Side, price, qty string) *orderbook.Order {
	p := dec.Zero
	if price != "" {
		p = dec.New(price)
	}
	q := dec.New(qty)
	return &orderbook.Order{
		ID:        id,
		UserID:    user,
		Symbol:    "BTC-USDT",
		Side:      side,
		Type:      orderbook.Limit,
		TIF:       orderbook.GTC,
		Price:     p,
		Qty:       q,
		Remaining: q,
		CreatedAt: int64(id),
	}
}

// runWorker starts the worker and returns a stop function that drains the
// outbox and waits for the goroutine to exit.
func runWorker(t *testing.T, w *SymbolWorker, outbox chan *Output) (collect func() []*Output) {
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	return func() []*Output {
		cancel()
		<-w.Done()
		close(outbox)
		var got []*Output
		for o := range outbox {
			got = append(got, o)
		}
		return got
	}
}

func TestWorkerProcessesInFIFOOrder(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	// Place ask 100@1, then bid 100@1 — expect one trade.
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, "m1", orderbook.Ask, "100", "1")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(2, "t1", orderbook.Bid, "100", "1")})

	// Give the worker a tick to drain, then shut down.
	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	// Expected:
	//   seq 1: OrderAccepted for id=1 (maker rested)
	//   seq 2: Trade for id=2 (taker)
	if len(got) != 2 {
		t.Fatalf("emissions = %d, want 2: %v", len(got), got)
	}
	if got[0].Kind != OutputOrderAccepted || got[0].OrderID != 1 || got[0].MatchSeq != 1 {
		t.Fatalf("got[0] = %+v", got[0])
	}
	if got[1].Kind != OutputTrade || got[1].OrderID != 2 || got[1].MatchSeq != 2 {
		t.Fatalf("got[1] = %+v", got[1])
	}
}

func TestWorkerCancelEmitsCancelled(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, "m1", orderbook.Bid, "100", "2")})
	w.Submit(&Event{Kind: EventOrderCancel, OrderID: 1, UserID: "m1"})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 2 {
		t.Fatalf("emissions = %d, want 2: %v", len(got), got)
	}
	if got[0].Kind != OutputOrderAccepted {
		t.Fatalf("got[0] kind = %d, want Accepted", got[0].Kind)
	}
	if got[1].Kind != OutputOrderCancelled || got[1].OrderID != 1 || got[1].FilledQty.String() != "0" {
		t.Fatalf("got[1] = %+v", got[1])
	}
}

// TestWorkerCancelUnknownOrderStillEmits guards the invariant that every input
// produces an output with a match_seq. Before: handleCancel silently dropped
// cancels for orders not in the book, leaving Counter stuck in PENDING_CANCEL
// forever (see docs/bugs.md Backlog 2026-04-19). After: emit OrderCancelled
// with zero FilledQty so Counter can close the loop via its own state.
func TestWorkerCancelUnknownOrderStillEmits(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	// Cancel for an order that was never placed on this worker's book.
	w.Submit(&Event{Kind: EventOrderCancel, Symbol: "BTC-USDT", OrderID: 999, UserID: "u1"})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 1 {
		t.Fatalf("emissions = %d, want 1: %v", len(got), got)
	}
	if got[0].Kind != OutputOrderCancelled {
		t.Fatalf("kind = %d, want OrderCancelled", got[0].Kind)
	}
	if got[0].MatchSeq != 1 {
		t.Errorf("match_seq = %d, want 1 (invariant: every input bumps seq)", got[0].MatchSeq)
	}
	if got[0].OrderID != 999 || got[0].UserID != "u1" || got[0].Symbol != "BTC-USDT" {
		t.Errorf("identity mismatch: %+v", got[0])
	}
	if got[0].FilledQty.String() != "0" {
		t.Errorf("filled_qty = %s, want 0", got[0].FilledQty)
	}
}

func TestWorkerDedupDuplicateOrderID(t *testing.T) {
	outbox := make(chan *Output, 16)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, "m1", orderbook.Bid, "100", "1")})
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, "m1", orderbook.Bid, "100", "1")})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 2 {
		t.Fatalf("emissions = %d, want 2: %v", len(got), got)
	}
	if got[1].Kind != OutputOrderRejected || got[1].RejectReason != orderbook.RejectDuplicateOrderID {
		t.Fatalf("expected DuplicateOrderID rejection, got %+v", got[1])
	}
}

func TestWorkerRejectsWrongSymbol(t *testing.T) {
	outbox := make(chan *Output, 8)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8}, outbox, nil)

	o := newLimitOrder(1, "u1", orderbook.Bid, "100", "1")
	o.Symbol = "ETH-USDT"
	w.Submit(&Event{Kind: EventOrderPlaced, Order: o})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 1 {
		t.Fatalf("emissions = %d, want 1", len(got))
	}
	if got[0].Kind != OutputOrderRejected || got[0].RejectReason != orderbook.RejectSymbolNotTrading {
		t.Fatalf("got[0] = %+v", got[0])
	}
}

func TestWorkerSTPRejection(t *testing.T) {
	outbox := make(chan *Output, 8)
	w := NewSymbolWorker(Config{Symbol: "BTC-USDT", Inbox: 8, STPMode: engine.STPRejectTaker}, outbox, nil)

	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(1, "u1", orderbook.Ask, "100", "1")})
	// Same user on the other side would self-trade → rejected.
	w.Submit(&Event{Kind: EventOrderPlaced, Order: newLimitOrder(2, "u1", orderbook.Bid, "100", "1")})

	collect := runWorker(t, w, outbox)
	time.Sleep(20 * time.Millisecond)
	got := collect()

	if len(got) != 2 {
		t.Fatalf("emissions = %d, want 2", len(got))
	}
	if got[1].Kind != OutputOrderRejected || got[1].RejectReason != orderbook.RejectSelfTradePrevented {
		t.Fatalf("got[1] = %+v", got[1])
	}
}
