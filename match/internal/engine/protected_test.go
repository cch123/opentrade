package engine

import (
	"testing"

	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/pkg/dec"
)

// -----------------------------------------------------------------------------
// ADR-0083 protected market orders.
//
// The engine receives the order with the effective limit already resolved
// into Price (the SymbolWorker derives it from the book collar / freeze cap);
// these tests assert the engine bounds matching by that limit for orders
// flagged with SlippageBps > 0.
// -----------------------------------------------------------------------------

// newProtectedMarket constructs a base-driven protected market taker with the
// effective limit pre-resolved, the way the worker hands it to the engine.
func newProtectedMarket(id, user uint64, side orderbook.Side, limit, qty string) *orderbook.Order {
	q := dec.New(qty)
	return &orderbook.Order{
		ID:          id,
		UserID:      user,
		Symbol:      "BTC-USDT",
		Side:        side,
		Type:        orderbook.Market,
		TIF:         orderbook.GTC,
		Price:       dec.New(limit),
		Qty:         q,
		Remaining:   q,
		SlippageBps: 50,
	}
}

func TestProtectedMarketBuyStopsAtLimit(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: 2001, side: orderbook.Ask, price: "100", qty: "1"},
		{id: 2, user: 2002, side: orderbook.Ask, price: "102", qty: "1"},
	})
	taker := newProtectedMarket(100, 3001, orderbook.Bid, "101", "2")
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 1 || r.Trades[0].Price.String() != "100" {
		t.Fatalf("trades = %+v, want one fill @100", r.Trades)
	}
	if taker.Remaining.String() != "1" {
		t.Fatalf("remaining = %s, want 1 (level @102 above the collar)", taker.Remaining)
	}
	if b.Has(taker.ID) {
		t.Fatal("protected market taker must not rest on book")
	}
}

func TestProtectedMarketSellStopsAtLimit(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: 2001, side: orderbook.Bid, price: "100", qty: "1"},
		{id: 2, user: 2002, side: orderbook.Bid, price: "98", qty: "1"},
	})
	taker := newProtectedMarket(100, 3001, orderbook.Ask, "99", "2")
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 1 || r.Trades[0].Price.String() != "100" {
		t.Fatalf("trades = %+v, want one fill @100", r.Trades)
	}
	if taker.Remaining.String() != "1" {
		t.Fatalf("remaining = %s, want 1 (level @98 below the floor)", taker.Remaining)
	}
}

func TestProtectedQuoteDrivenBuyRespectsLimit(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: 2001, side: orderbook.Ask, price: "100", qty: "1"},
		{id: 2, user: 2002, side: orderbook.Ask, price: "200", qty: "1"},
	})
	taker := newQuoteBuy(100, 3001, "300")
	taker.SlippageBps = 50
	taker.Price = dec.New("150") // effective limit resolved by the worker
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 1 || r.Trades[0].Price.String() != "100" {
		t.Fatalf("trades = %+v, want one fill @100 (200 above the collar)", r.Trades)
	}
	if taker.RemainingQuote.String() != "200" {
		t.Fatalf("remaining quote = %s, want 200", taker.RemainingQuote)
	}
}

func TestProtectedMarketFOKRespectsCollar(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: 2001, side: orderbook.Ask, price: "100", qty: "1"},
		{id: 2, user: 2002, side: orderbook.Ask, price: "102", qty: "1"},
	})
	// Full fill is only possible by reaching the 102 level above the collar:
	// the FOK pre-check must count collar-acceptable liquidity only.
	taker := newProtectedMarket(100, 3001, orderbook.Bid, "101", "2")
	taker.TIF = orderbook.FOK
	r := Match(b, taker, STPNone)
	if r.Status != TakerRejected || r.RejectReason != orderbook.RejectFOKNotFilled {
		t.Fatalf("status = %d reason = %d, want TakerRejected/fok_not_filled", r.Status, r.RejectReason)
	}
	if len(r.Trades) != 0 {
		t.Fatalf("trades = %d, want 0", len(r.Trades))
	}
}

func TestProtectedMarketZeroLimitFillsNothing(t *testing.T) {
	// A dust freeze cap can resolve the effective limit to 0 — the order must
	// fill nothing and expire (NOT degrade to an unprotected market order).
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: 2001, side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newProtectedMarket(100, 3001, orderbook.Bid, "0", "1")
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 0 {
		t.Fatalf("trades = %d, want 0", len(r.Trades))
	}
}

func TestUnprotectedMarketIgnoresPriceField(t *testing.T) {
	// SlippageBps == 0: a market order keeps consuming regardless of Price
	// (defensive — Price should be zero on the wire for plain market orders).
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: 2001, side: orderbook.Ask, price: "100", qty: "1"},
		{id: 2, user: 2002, side: orderbook.Ask, price: "200", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: 3001, side: orderbook.Bid, typ: orderbook.Market, qty: "2"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerFilled {
		t.Fatalf("status = %d, want TakerFilled", r.Status)
	}
	if len(r.Trades) != 2 {
		t.Fatalf("trades = %d, want 2", len(r.Trades))
	}
}
