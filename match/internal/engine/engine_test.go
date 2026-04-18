package engine

import (
	"testing"

	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/pkg/dec"
)

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

type orderSpec struct {
	id    uint64
	user  string
	side  orderbook.Side
	typ   orderbook.OrderType
	tif   orderbook.TIF
	price string // empty for market
	qty   string
}

func newOrder(s orderSpec) *orderbook.Order {
	p := dec.Zero
	if s.price != "" {
		p = dec.New(s.price)
	}
	q := dec.New(s.qty)
	t := s.typ
	if t == 0 {
		t = orderbook.Limit
	}
	tif := s.tif
	if tif == 0 {
		tif = orderbook.GTC
	}
	return &orderbook.Order{
		ID:        s.id,
		UserID:    s.user,
		Symbol:    "BTC-USDT",
		Side:      s.side,
		Type:      t,
		TIF:       tif,
		Price:     p,
		Qty:       q,
		Remaining: q,
		CreatedAt: int64(s.id),
	}
}

// insertRested pre-seeds the book with GTC limit orders.
func insertRested(t *testing.T, b *orderbook.Book, specs []orderSpec) {
	t.Helper()
	for _, s := range specs {
		if err := b.Insert(newOrder(s)); err != nil {
			t.Fatalf("Insert %d: %v", s.id, err)
		}
	}
}

// -----------------------------------------------------------------------------
// Limit GTC
// -----------------------------------------------------------------------------

func TestLimitNoCrossRestsOnBook(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "200", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, price: "199", qty: "1"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerAcceptedOnBook {
		t.Fatalf("status = %d, want TakerAcceptedOnBook", r.Status)
	}
	if len(r.Trades) != 0 {
		t.Fatalf("trades = %d, want 0", len(r.Trades))
	}
	if !b.Has(taker.ID) {
		t.Fatal("taker should be on book")
	}
}

func TestLimitFullCrossFilled(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, price: "100", qty: "1"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerFilled {
		t.Fatalf("status = %d, want TakerFilled", r.Status)
	}
	if len(r.Trades) != 1 {
		t.Fatalf("trades = %d, want 1", len(r.Trades))
	}
	if r.Trades[0].Price.String() != "100" || r.Trades[0].Qty.String() != "1" {
		t.Fatalf("trade = {%s, %s}", r.Trades[0].Price, r.Trades[0].Qty)
	}
	if b.Len() != 0 {
		t.Fatalf("book should be empty, got %d orders", b.Len())
	}
}

func TestLimitPartialCrossRemainderOnBook(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, price: "100", qty: "3"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerPartialOnBook {
		t.Fatalf("status = %d, want TakerPartialOnBook", r.Status)
	}
	if len(r.Trades) != 1 {
		t.Fatalf("trades = %d, want 1", len(r.Trades))
	}
	if taker.Remaining.String() != "2" {
		t.Fatalf("taker remaining = %s, want 2", taker.Remaining)
	}
	if !b.Has(taker.ID) {
		t.Fatal("remaining taker should be on book")
	}
}

func TestLimitWalksMultipleLevels(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
		{id: 2, user: "m2", side: orderbook.Ask, price: "101", qty: "2"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, price: "101", qty: "3"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerFilled {
		t.Fatalf("status = %d, want TakerFilled", r.Status)
	}
	if len(r.Trades) != 2 {
		t.Fatalf("trades = %d, want 2", len(r.Trades))
	}
	if r.Trades[0].Price.String() != "100" || r.Trades[1].Price.String() != "101" {
		t.Fatalf("trade prices = %s,%s; want 100,101", r.Trades[0].Price, r.Trades[1].Price)
	}
}

// -----------------------------------------------------------------------------
// Market
// -----------------------------------------------------------------------------

func TestMarketBuyConsumesUntilFilled(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
		{id: 2, user: "m2", side: orderbook.Ask, price: "200", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, typ: orderbook.Market, qty: "2"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerFilled {
		t.Fatalf("status = %d, want TakerFilled", r.Status)
	}
	if len(r.Trades) != 2 {
		t.Fatalf("trades = %d, want 2", len(r.Trades))
	}
}

func TestMarketExpiresWhenBookExhausted(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, typ: orderbook.Market, qty: "5"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 1 {
		t.Fatalf("trades = %d, want 1", len(r.Trades))
	}
	if b.Has(taker.ID) {
		t.Fatal("market taker must not rest on book")
	}
}

// -----------------------------------------------------------------------------
// MARKET BUY with quoteOrderQty (ADR-0035)
// -----------------------------------------------------------------------------

// newQuoteBuy constructs a quote-driven market buy taker.
func newQuoteBuy(id uint64, user, quoteQty string) *orderbook.Order {
	q := dec.New(quoteQty)
	return &orderbook.Order{
		ID:             id,
		UserID:         user,
		Symbol:         "BTC-USDT",
		Side:           orderbook.Bid,
		Type:           orderbook.Market,
		TIF:            orderbook.GTC,
		QuoteQty:       q,
		RemainingQuote: q,
	}
}

func TestMarketBuyByQuoteBudget_WalksLevels(t *testing.T) {
	// Ask book: 0.3 @ 50000 (15000), 0.2 @ 50100 (10020). Budget 25000
	// crosses both levels; last trade is bounded by the remaining quote
	// which doesn't divide evenly into 50100, so a sub-atom of quote
	// (≈1e-14 USDT) gets stuck on the taker and the status comes back as
	// TakerExpired. That's the same shape BN reports as "basically filled":
	// user view is "got 0.4996 BTC, spent ~25000 USDT", Counter refunds the
	// residual freeze. The important invariant is no trade overspends.
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "50000", qty: "0.3"},
		{id: 2, user: "m2", side: orderbook.Ask, price: "50100", qty: "0.2"},
	})
	taker := newQuoteBuy(100, "t1", "25000")
	r := Match(b, taker, STPNone)
	if r.Status != TakerFilled && r.Status != TakerExpired {
		t.Fatalf("status = %d, want Filled or Expired", r.Status)
	}
	if len(r.Trades) != 2 {
		t.Fatalf("trades = %d, want 2", len(r.Trades))
	}
	if r.Trades[0].Qty.String() != "0.3" || r.Trades[0].Price.String() != "50000" {
		t.Errorf("trade[0]: %+v", r.Trades[0])
	}
	total := dec.Zero
	for _, tr := range r.Trades {
		total = total.Add(tr.Price.Mul(tr.Qty))
	}
	if total.GreaterThan(dec.New("25000")) {
		t.Errorf("total spend %s > budget 25000", total)
	}
	// Residual budget should be at most rounding dust (below 1e-8 USDT).
	if taker.RemainingQuote.GreaterThan(dec.New("0.00000001")) {
		t.Errorf("residual quote = %s, want near zero", taker.RemainingQuote)
	}
}

func TestMarketBuyByQuoteBudget_ExpiresWhenBookExhausted(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "50000", qty: "0.1"},
	})
	taker := newQuoteBuy(100, "t1", "25000") // budget far exceeds depth
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 1 {
		t.Fatalf("trades = %d, want 1", len(r.Trades))
	}
	// Spent 5000 USDT, 20000 left.
	if taker.RemainingQuote.String() != "20000" {
		t.Errorf("RemainingQuote = %s, want 20000", taker.RemainingQuote)
	}
	if b.Has(taker.ID) {
		t.Fatal("market quote taker must not rest on book")
	}
}

func TestMarketBuyByQuoteBudget_NoCrossExpires(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT") // empty ask side
	taker := newQuoteBuy(100, "t1", "100")
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 0 {
		t.Fatalf("trades = %d, want 0", len(r.Trades))
	}
}

// -----------------------------------------------------------------------------
// IOC
// -----------------------------------------------------------------------------

func TestIOCPartialRemainderExpires(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, tif: orderbook.IOC, price: "100", qty: "3"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 1 {
		t.Fatalf("trades = %d, want 1", len(r.Trades))
	}
	if b.Has(taker.ID) {
		t.Fatal("IOC remainder must not rest on book")
	}
}

func TestIOCNoCrossExpires(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "200", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, tif: orderbook.IOC, price: "100", qty: "1"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerExpired {
		t.Fatalf("status = %d, want TakerExpired", r.Status)
	}
	if len(r.Trades) != 0 {
		t.Fatalf("trades = %d, want 0", len(r.Trades))
	}
}

// -----------------------------------------------------------------------------
// FOK
// -----------------------------------------------------------------------------

func TestFOKRejectedIfUnfillable(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, tif: orderbook.FOK, price: "100", qty: "2"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerRejected || r.RejectReason != orderbook.RejectFOKNotFilled {
		t.Fatalf("status=%d reason=%s, want Rejected/FOKNotFilled", r.Status, r.RejectReason)
	}
	if len(r.Trades) != 0 {
		t.Fatalf("trades = %d, want 0", len(r.Trades))
	}
	// Book untouched.
	if b.LevelQty(orderbook.Ask, dec.New("100")).String() != "1" {
		t.Fatalf("book should be untouched after FOK reject")
	}
}

func TestFOKFilledIfFullyFillable(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
		{id: 2, user: "m2", side: orderbook.Ask, price: "101", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, tif: orderbook.FOK, price: "101", qty: "2"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerFilled {
		t.Fatalf("status = %d, want TakerFilled", r.Status)
	}
	if len(r.Trades) != 2 {
		t.Fatalf("trades = %d, want 2", len(r.Trades))
	}
}

// -----------------------------------------------------------------------------
// Post-Only
// -----------------------------------------------------------------------------

func TestPostOnlyRejectedIfCrosses(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, tif: orderbook.PostOnly, price: "100", qty: "1"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerRejected || r.RejectReason != orderbook.RejectPostOnlyWouldTake {
		t.Fatalf("status=%d reason=%s, want Rejected/PostOnlyWouldTake", r.Status, r.RejectReason)
	}
}

func TestPostOnlyRestsWhenNoCross(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "m1", side: orderbook.Ask, price: "101", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "t1", side: orderbook.Bid, tif: orderbook.PostOnly, price: "100", qty: "1"})
	r := Match(b, taker, STPNone)
	if r.Status != TakerAcceptedOnBook {
		t.Fatalf("status = %d, want TakerAcceptedOnBook", r.Status)
	}
	if !b.Has(taker.ID) {
		t.Fatal("post-only should be on book")
	}
}

// -----------------------------------------------------------------------------
// STP
// -----------------------------------------------------------------------------

func TestSTPRejectTakerDetectsSelfCross(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "u1", side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "u1", side: orderbook.Bid, price: "100", qty: "1"})
	r := Match(b, taker, STPRejectTaker)
	if r.Status != TakerRejected || r.RejectReason != orderbook.RejectSelfTradePrevented {
		t.Fatalf("status=%d reason=%s, want Rejected/SelfTrade", r.Status, r.RejectReason)
	}
}

func TestSTPRejectTakerAllowsDifferentUsers(t *testing.T) {
	b := orderbook.NewBook("BTC-USDT")
	insertRested(t, b, []orderSpec{
		{id: 1, user: "u1", side: orderbook.Ask, price: "100", qty: "1"},
	})
	taker := newOrder(orderSpec{id: 100, user: "u2", side: orderbook.Bid, price: "100", qty: "1"})
	r := Match(b, taker, STPRejectTaker)
	if r.Status != TakerFilled {
		t.Fatalf("status = %d, want TakerFilled", r.Status)
	}
}
