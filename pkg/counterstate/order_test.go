package counterstate

import (
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func TestComputeFreeze_Limit(t *testing.T) {
	cases := []struct {
		name   string
		symbol string
		side   Side
		price  string
		qty    string
		asset  string
		amount string
		err    bool
	}{
		{"buy", "BTC-USDT", SideBid, "100", "2", "USDT", "200", false},
		{"sell", "BTC-USDT", SideAsk, "100", "2", "BTC", "2", false},
		{"bad symbol", "BTCUSDT", SideBid, "100", "1", "", "", true},
		{"zero qty", "BTC-USDT", SideBid, "100", "0", "", "", true},
		{"negative price", "BTC-USDT", SideBid, "-1", "1", "", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			asset, amount, err := ComputeFreeze(c.symbol, c.side, OrderTypeLimit, dec.New(c.price), dec.New(c.qty), dec.Zero, dec.Zero, 0)
			if c.err {
				if err == nil {
					t.Fatalf("expected error, got (%s,%s)", asset, amount)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if asset != c.asset || amount.String() != c.amount {
				t.Fatalf("got (%s,%s), want (%s,%s)", asset, amount, c.asset, c.amount)
			}
		})
	}
}

func TestComputeFreeze_MarketSell(t *testing.T) {
	// Market sell freezes base qty (same as limit sell; no price needed).
	asset, amount, err := ComputeFreeze("BTC-USDT", SideAsk, OrderTypeMarket, dec.Zero, dec.New("0.5"), dec.Zero, dec.Zero, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if asset != "BTC" || amount.String() != "0.5" {
		t.Fatalf("got (%s,%s), want (BTC,0.5)", asset, amount)
	}
}

func TestComputeFreeze_MarketBuyByQuote(t *testing.T) {
	// BN-style quoteOrderQty: freeze quote_qty in quote currency.
	asset, amount, err := ComputeFreeze("BTC-USDT", SideBid, OrderTypeMarket, dec.Zero, dec.Zero, dec.New("100"), dec.Zero, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if asset != "USDT" || amount.String() != "100" {
		t.Fatalf("got (%s,%s), want (USDT,100)", asset, amount)
	}
}

func TestComputeFreeze_MarketBuyWithoutQuoteRejected(t *testing.T) {
	// Market buy without quote_qty is explicitly refused (would require
	// Counter to estimate freeze from last price — ADR-0035 §备选方案 Z).
	_, _, err := ComputeFreeze("BTC-USDT", SideBid, OrderTypeMarket, dec.Zero, dec.New("1"), dec.Zero, dec.Zero, 0)
	if err == nil {
		t.Fatal("expected error for market buy without quote_qty")
	}
}

func TestOrderStatusExternalMapping(t *testing.T) {
	o := &Order{Status: OrderStatusPendingNew}
	if o.ExternalStatus() != ExternalStatusNew {
		t.Fatalf("PENDING_NEW → %d, want NEW", o.ExternalStatus())
	}
	o.Status = OrderStatusPendingCancel
	o.PreCancelStatus = OrderStatusPartiallyFilled
	if o.ExternalStatus() != ExternalStatusPartiallyFilled {
		t.Fatalf("PENDING_CANCEL from PART → %d, want PARTIAL", o.ExternalStatus())
	}
	o.PreCancelStatus = OrderStatusNew
	if o.ExternalStatus() != ExternalStatusNew {
		t.Fatalf("PENDING_CANCEL from NEW → %d, want NEW", o.ExternalStatus())
	}
}

func TestOrderStoreInsertDedup(t *testing.T) {
	s := newOrderStore()
	o := &Order{ID: 1, UserID: 1001, ClientOrderID: "c1", Status: OrderStatusPendingNew}
	if err := s.Insert(o); err != nil {
		t.Fatal(err)
	}
	// Second order, same user + COID → collision with active.
	o2 := &Order{ID: 2, UserID: 1001, ClientOrderID: "c1", Status: OrderStatusPendingNew}
	if err := s.Insert(o2); err != ErrClientOrderIDActive {
		t.Fatalf("err = %v, want ErrClientOrderIDActive", err)
	}
	// Different user OK.
	o3 := &Order{ID: 3, UserID: 1002, ClientOrderID: "c1", Status: OrderStatusPendingNew}
	if err := s.Insert(o3); err != nil {
		t.Fatal(err)
	}
	// Same ID → Duplicate.
	if err := s.Insert(&Order{ID: 1, UserID: 1001}); err != ErrDuplicateOrder {
		t.Fatalf("err = %v, want ErrDuplicateOrder", err)
	}
}

func TestOrderStoreReleaseOnTerminal(t *testing.T) {
	s := newOrderStore()
	_ = s.Insert(&Order{ID: 1, UserID: 1001, ClientOrderID: "c1", Status: OrderStatusNew})
	if _, err := s.UpdateStatus(1, OrderStatusFilled, 1); err != nil {
		t.Fatal(err)
	}
	// COID must no longer collide with new orders.
	if err := s.Insert(&Order{ID: 2, UserID: 1001, ClientOrderID: "c1", Status: OrderStatusPendingNew}); err != nil {
		t.Fatalf("COID not released: %v", err)
	}
}

func TestComputeSettlementLimitBuyMatchesAtMakerPrice(t *testing.T) {
	state := NewShardState(0)
	// Seed maker (sell) and taker (buy) orders in the store.
	_ = state.Orders().Insert(&Order{
		ID: 1, UserID: 9001, Symbol: "BTC-USDT", Side: SideAsk,
		Type: OrderTypeLimit, TIF: TIFGTC,
		Price: dec.New("100"), Qty: dec.New("2"), Status: OrderStatusNew,
		FrozenAsset: "BTC", FrozenAmount: dec.New("2"),
	})
	_ = state.Orders().Insert(&Order{
		ID: 2, UserID: 9002, Symbol: "BTC-USDT", Side: SideBid,
		Type: OrderTypeLimit, TIF: TIFGTC,
		Price: dec.New("105"), Qty: dec.New("1"), Status: OrderStatusPendingNew,
		FrozenAsset: "USDT", FrozenAmount: dec.New("105"),
	})

	maker, taker, err := ComputeSettlement(state, TradeInput{
		TradeID: "t1", Symbol: "BTC-USDT",
		Price: dec.New("100"), Qty: dec.New("1"),
		MakerUserID: 9001, MakerOrderID: 1,
		TakerUserID: 9002, TakerOrderID: 2, TakerSide: SideBid,
		MakerFilledQtyAfter: dec.New("1"),
		TakerFilledQtyAfter: dec.New("1"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Maker (sell 2 BTC @100): fill 1 → frozen BTC -1, receives 100 USDT.
	if maker.FrozenBaseDelta.String() != "-1" || maker.QuoteDelta.String() != "100" {
		t.Fatalf("maker = %+v", maker)
	}
	if maker.StatusAfter != OrderStatusPartiallyFilled {
		t.Fatalf("maker status = %d, want PARTIAL", maker.StatusAfter)
	}

	// Taker (buy 1 BTC @105, matches @100):
	//   reservation for this slice = 105*1 = 105 → frozen_quote -= 105
	//   price improvement refund = (105-100)*1 = 5 → available_quote += 5
	//   base delta = +1 BTC
	if taker.FrozenQuoteDelta.String() != "-105" {
		t.Fatalf("taker frozen_quote = %s, want -105", taker.FrozenQuoteDelta)
	}
	if taker.QuoteDelta.String() != "5" {
		t.Fatalf("taker quote = %s, want 5", taker.QuoteDelta)
	}
	if taker.BaseDelta.String() != "1" {
		t.Fatalf("taker base = %s, want 1", taker.BaseDelta)
	}
	if taker.StatusAfter != OrderStatusFilled {
		t.Fatalf("taker status = %d, want FILLED", taker.StatusAfter)
	}
}

func TestApplyPartySettlement(t *testing.T) {
	state := NewShardState(0)
	// Preload user with frozen USDT + initial order.
	acc := state.Account(1001)
	acc.PutForRestore("USDT", Balance{Available: dec.New("0"), Frozen: dec.New("105")})
	_ = state.Orders().Insert(&Order{
		ID: 1, UserID: 1001, Symbol: "BTC-USDT", Side: SideBid,
		Qty: dec.New("1"), Status: OrderStatusPendingNew,
		FrozenAsset: "USDT", FrozenAmount: dec.New("105"),
	})

	settlement := PartySettlement{
		UserID: 1001, OrderID: 1,
		BaseDelta: dec.New("1"), FrozenQuoteDelta: dec.New("-105"), QuoteDelta: dec.New("5"),
		FilledQtyAfter: dec.New("1"),
		StatusAfter:    OrderStatusFilled,
	}
	if err := state.ApplyPartySettlement("BTC-USDT", settlement); err != nil {
		t.Fatal(err)
	}

	if got := state.Balance(1001, "BTC"); got.Available.String() != "1" {
		t.Fatalf("BTC = %+v", got)
	}
	if got := state.Balance(1001, "USDT"); got.Available.String() != "5" || got.Frozen.String() != "0" {
		t.Fatalf("USDT = %+v", got)
	}
	o := state.Orders().Get(1)
	if o.Status != OrderStatusFilled || o.FilledQty.String() != "1" {
		t.Fatalf("order = %+v", o)
	}
}

// -----------------------------------------------------------------------------
// ADR-0083 protected market buy by base qty
// -----------------------------------------------------------------------------

func TestComputeFreeze_ProtectedMarketBuyByBase(t *testing.T) {
	asset, amount, err := ComputeFreeze("BTC-USDT", SideBid, OrderTypeMarket, dec.Zero, dec.New("0.5"), dec.Zero, dec.New("23000"), 50)
	if err != nil {
		t.Fatal(err)
	}
	if asset != "USDT" || amount.String() != "23000" {
		t.Fatalf("freeze = (%s, %s), want (USDT, 23000)", asset, amount)
	}
}

func TestComputeFreeze_ProtectedShapeRejections(t *testing.T) {
	cases := []struct {
		name        string
		side        Side
		typ         OrderType
		price       string
		qty         string
		quoteQty    string
		quoteCap    string
		slippageBps uint32
		wantErr     error
	}{
		{"protected buy by qty without cap", SideBid, OrderTypeMarket, "", "1", "", "", 50, ErrProtectedBuyNeedsCap},
		{"slippage on limit", SideBid, OrderTypeLimit, "100", "1", "", "", 50, ErrInvalidSlippage},
		{"slippage above 10000", SideBid, OrderTypeMarket, "", "1", "", "100", 10001, ErrInvalidSlippage},
		{"cap with quote budget", SideBid, OrderTypeMarket, "", "", "100", "100", 50, ErrProtectedQuoteHasCap},
		{"cap on market sell", SideAsk, OrderTypeMarket, "", "1", "", "100", 50, ErrQuoteCapNotAllowed},
		{"cap on unprotected buy", SideBid, OrderTypeMarket, "", "1", "", "100", 0, ErrQuoteCapNotAllowed},
		{"cap on limit", SideBid, OrderTypeLimit, "100", "1", "", "100", 0, ErrQuoteCapNotAllowed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			price, qty, quoteQty, quoteCap := dec.Zero, dec.Zero, dec.Zero, dec.Zero
			if c.price != "" {
				price = dec.New(c.price)
			}
			if c.qty != "" {
				qty = dec.New(c.qty)
			}
			if c.quoteQty != "" {
				quoteQty = dec.New(c.quoteQty)
			}
			if c.quoteCap != "" {
				quoteCap = dec.New(c.quoteCap)
			}
			_, _, err := ComputeFreeze("BTC-USDT", c.side, c.typ, price, qty, quoteQty, quoteCap, c.slippageBps)
			if err != c.wantErr {
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			}
		})
	}
}

func TestComputeSettlementProtectedMarketBuyByBase(t *testing.T) {
	state := NewShardState(0)
	_ = state.Orders().Insert(&Order{
		ID: 1, UserID: 9001, Symbol: "BTC-USDT", Side: SideAsk,
		Type: OrderTypeLimit, TIF: TIFGTC,
		Price: dec.New("100"), Qty: dec.New("2"), Status: OrderStatusNew,
		FrozenAsset: "BTC", FrozenAmount: dec.New("2"),
	})
	// Protected market buy 1 BTC, quote_cap 150 frozen (ADR-0083). The fill
	// at 100 must consume exactly match_price × qty = 100 from frozen — no
	// price-improvement refund (the order has no user price).
	_ = state.Orders().Insert(&Order{
		ID: 2, UserID: 9002, Symbol: "BTC-USDT", Side: SideBid,
		Type: OrderTypeMarket, TIF: TIFGTC, SlippageBps: 50,
		Qty: dec.New("1"), Status: OrderStatusPendingNew,
		FrozenAsset: "USDT", FrozenAmount: dec.New("150"),
	})

	_, taker, err := ComputeSettlement(state, TradeInput{
		TradeID: "t1", Symbol: "BTC-USDT",
		Price: dec.New("100"), Qty: dec.New("1"),
		MakerUserID: 9001, MakerOrderID: 1,
		TakerUserID: 9002, TakerOrderID: 2, TakerSide: SideBid,
		MakerFilledQtyAfter: dec.New("1"),
		TakerFilledQtyAfter: dec.New("1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if taker.FrozenQuoteDelta.String() != "-100" {
		t.Fatalf("taker frozen_quote = %s, want -100 (match spend, not cap)", taker.FrozenQuoteDelta)
	}
	if !dec.IsZero(taker.QuoteDelta) {
		t.Fatalf("taker quote refund = %s, want 0 (no user price to improve on)", taker.QuoteDelta)
	}
	if taker.BaseDelta.String() != "1" {
		t.Fatalf("taker base = %s, want 1", taker.BaseDelta)
	}
	// Qty-driven: full fill reaches FILLED through the normal base path.
	if taker.StatusAfter != OrderStatusFilled {
		t.Fatalf("taker status = %d, want FILLED", taker.StatusAfter)
	}
}

func TestProtectedMarketBuyResidualRefund(t *testing.T) {
	state := NewShardState(0)
	acc := state.Account(1001)
	acc.PutForRestore("USDT", Balance{Available: dec.Zero, Frozen: dec.New("150")})
	o := &Order{
		ID: 1, UserID: 1001, Symbol: "BTC-USDT", Side: SideBid,
		Type: OrderTypeMarket, TIF: TIFGTC, SlippageBps: 50,
		Qty: dec.New("1"), Status: OrderStatusPendingNew,
		FrozenAsset: "USDT", FrozenAmount: dec.New("150"),
	}
	_ = state.Orders().Insert(o)

	// Partial fill 0.6 @ 100 consumes 60 from frozen.
	if err := state.ApplyPartySettlement("BTC-USDT", PartySettlement{
		UserID: 1001, OrderID: 1,
		BaseDelta: dec.New("0.6"), FrozenQuoteDelta: dec.New("-60"),
		FilledQtyAfter: dec.New("0.6"),
		StatusAfter:    OrderStatusPartiallyFilled,
	}); err != nil {
		t.Fatal(err)
	}
	if o.FrozenSpent.String() != "60" {
		t.Fatalf("frozen_spent = %s, want 60", o.FrozenSpent)
	}

	// Expire releases residual = 150 − 60 = 90 back to available.
	if err := state.UnfreezeOnTerminal(o, o.FrozenSpent); err != nil {
		t.Fatal(err)
	}
	b := state.Balance(1001, "USDT")
	if b.Available.String() != "90" || b.Frozen.String() != "0" {
		t.Fatalf("USDT = %+v, want available 90 / frozen 0", b)
	}
}
