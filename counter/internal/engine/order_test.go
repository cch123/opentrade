package engine

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
			asset, amount, err := ComputeFreeze(c.symbol, c.side, OrderTypeLimit, dec.New(c.price), dec.New(c.qty), dec.Zero)
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
	asset, amount, err := ComputeFreeze("BTC-USDT", SideAsk, OrderTypeMarket, dec.Zero, dec.New("0.5"), dec.Zero)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if asset != "BTC" || amount.String() != "0.5" {
		t.Fatalf("got (%s,%s), want (BTC,0.5)", asset, amount)
	}
}

func TestComputeFreeze_MarketBuyByQuote(t *testing.T) {
	// BN-style quoteOrderQty: freeze quote_qty in quote currency.
	asset, amount, err := ComputeFreeze("BTC-USDT", SideBid, OrderTypeMarket, dec.Zero, dec.Zero, dec.New("100"))
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
	_, _, err := ComputeFreeze("BTC-USDT", SideBid, OrderTypeMarket, dec.Zero, dec.New("1"), dec.Zero)
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
	o := &Order{ID: 1, UserID: "u1", ClientOrderID: "c1", Status: OrderStatusPendingNew}
	if err := s.Insert(o); err != nil {
		t.Fatal(err)
	}
	// Second order, same user + COID → collision with active.
	o2 := &Order{ID: 2, UserID: "u1", ClientOrderID: "c1", Status: OrderStatusPendingNew}
	if err := s.Insert(o2); err != ErrClientOrderIDActive {
		t.Fatalf("err = %v, want ErrClientOrderIDActive", err)
	}
	// Different user OK.
	o3 := &Order{ID: 3, UserID: "u2", ClientOrderID: "c1", Status: OrderStatusPendingNew}
	if err := s.Insert(o3); err != nil {
		t.Fatal(err)
	}
	// Same ID → Duplicate.
	if err := s.Insert(&Order{ID: 1, UserID: "u1"}); err != ErrDuplicateOrder {
		t.Fatalf("err = %v, want ErrDuplicateOrder", err)
	}
}

func TestOrderStoreReleaseOnTerminal(t *testing.T) {
	s := newOrderStore()
	_ = s.Insert(&Order{ID: 1, UserID: "u1", ClientOrderID: "c1", Status: OrderStatusNew})
	if _, err := s.UpdateStatus(1, OrderStatusFilled, 1); err != nil {
		t.Fatal(err)
	}
	// COID must no longer collide with new orders.
	if err := s.Insert(&Order{ID: 2, UserID: "u1", ClientOrderID: "c1", Status: OrderStatusPendingNew}); err != nil {
		t.Fatalf("COID not released: %v", err)
	}
}

func TestComputeSettlementLimitBuyMatchesAtMakerPrice(t *testing.T) {
	state := NewShardState(0)
	// Seed maker (sell) and taker (buy) orders in the store.
	_ = state.Orders().Insert(&Order{
		ID: 1, UserID: "mkr", Symbol: "BTC-USDT", Side: SideAsk,
		Type: OrderTypeLimit, TIF: TIFGTC,
		Price: dec.New("100"), Qty: dec.New("2"), Status: OrderStatusNew,
		FrozenAsset: "BTC", FrozenAmount: dec.New("2"),
	})
	_ = state.Orders().Insert(&Order{
		ID: 2, UserID: "tkr", Symbol: "BTC-USDT", Side: SideBid,
		Type: OrderTypeLimit, TIF: TIFGTC,
		Price: dec.New("105"), Qty: dec.New("1"), Status: OrderStatusPendingNew,
		FrozenAsset: "USDT", FrozenAmount: dec.New("105"),
	})

	maker, taker, err := ComputeSettlement(state, TradeInput{
		TradeID: "t1", Symbol: "BTC-USDT",
		Price: dec.New("100"), Qty: dec.New("1"),
		MakerUserID: "mkr", MakerOrderID: 1,
		TakerUserID: "tkr", TakerOrderID: 2, TakerSide: SideBid,
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
	acc := state.Account("u1")
	acc.PutForRestore("USDT", Balance{Available: dec.New("0"), Frozen: dec.New("105")})
	_ = state.Orders().Insert(&Order{
		ID: 1, UserID: "u1", Symbol: "BTC-USDT", Side: SideBid,
		Qty: dec.New("1"), Status: OrderStatusPendingNew,
		FrozenAsset: "USDT", FrozenAmount: dec.New("105"),
	})

	settlement := PartySettlement{
		UserID: "u1", OrderID: 1,
		BaseDelta: dec.New("1"), FrozenQuoteDelta: dec.New("-105"), QuoteDelta: dec.New("5"),
		FilledQtyAfter: dec.New("1"),
		StatusAfter:    OrderStatusFilled,
	}
	if err := state.ApplyPartySettlement("BTC-USDT", settlement); err != nil {
		t.Fatal(err)
	}

	if got := state.Balance("u1", "BTC"); got.Available.String() != "1" {
		t.Fatalf("BTC = %+v", got)
	}
	if got := state.Balance("u1", "USDT"); got.Available.String() != "5" || got.Frozen.String() != "0" {
		t.Fatalf("USDT = %+v", got)
	}
	o := state.Orders().Get(1)
	if o.Status != OrderStatusFilled || o.FilledQty.String() != "1" {
		t.Fatalf("order = %+v", o)
	}
}
