package service

import (
	"context"
	"testing"

	"github.com/xargin/opentrade/pkg/counterstate"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// precisionFixtureCfg mirrors the defaults used across ADR-0053 docs and
// M2 precision-cli output for BTC-USDT.
func precisionFixtureCfg() etcdcfg.SymbolConfig {
	return etcdcfg.SymbolConfig{
		Shard:            "match-0",
		Trading:          true,
		BaseAsset:        "BTC",
		QuoteAsset:       "USDT",
		PrecisionVersion: 1,
		Tiers: []etcdcfg.PrecisionTier{
			{
				PriceFrom:                     dec.New("0"),
				PriceTo:                       dec.New("0"),
				TickSize:                      dec.New("0.01"),
				StepSize:                      dec.New("0.00001"),
				QuoteStepSize:                 dec.New("0.01"),
				MinQty:                        dec.New("0.0001"),
				MaxQty:                        dec.New("1000"),
				MinQuoteQty:                   dec.New("1"),
				MinQuoteAmount:                dec.New("5"),
				MarketMinQty:                  dec.New("0.0001"),
				MarketMaxQty:                  dec.New("500"),
				EnforceMinQuoteAmountOnMarket: true,
				AvgPriceMins:                  5,
			},
		},
	}
}

func TestValidatePrecision_CompatModeNoTiers(t *testing.T) {
	cfg := etcdcfg.SymbolConfig{Shard: "match-0", Trading: true} // no Tiers
	req := PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: counterstate.SideBid, OrderType: counterstate.OrderTypeLimit,
		Price: dec.New("50000.123"), // would fail tick in strict mode
		Qty:   dec.New("0.00000001"), // would fail step + min_qty
	}
	reason, pass := validatePrecision(cfg, req)
	if !pass || reason != etcdcfg.RejectNone {
		t.Errorf("compat mode should pass, got reason=%s pass=%v", reason, pass)
	}
}

func TestValidatePrecision_LimitValid(t *testing.T) {
	cfg := precisionFixtureCfg()
	req := PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: counterstate.SideBid, OrderType: counterstate.OrderTypeLimit,
		Price: dec.New("50000.00"), Qty: dec.New("0.1"),
	}
	reason, pass := validatePrecision(cfg, req)
	if !pass {
		t.Errorf("valid limit rejected: reason=%s", reason)
	}
}

func TestValidatePrecision_LimitRejects(t *testing.T) {
	cfg := precisionFixtureCfg()
	cases := []struct {
		name       string
		price, qty string
		want       etcdcfg.RejectReason
	}{
		{"bad-tick", "50000.001", "0.1", etcdcfg.RejectInvalidPriceTick},
		{"bad-step", "50000.00", "0.0000001", etcdcfg.RejectInvalidLotSize},
		{"below-min-qty", "50000.00", "0.00005", etcdcfg.RejectMinQty},
		{"min-quote-amount", "40000.00", "0.0001", etcdcfg.RejectMinQuoteAmount}, // 4 < 5
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := PlaceOrderRequest{
				UserID: "u1", Symbol: "BTC-USDT",
				Side: counterstate.SideBid, OrderType: counterstate.OrderTypeLimit,
				Price: dec.New(c.price), Qty: dec.New(c.qty),
			}
			reason, pass := validatePrecision(cfg, req)
			if pass {
				t.Errorf("expected reject, got pass")
			}
			if reason != c.want {
				t.Errorf("reason=%s want=%s", reason, c.want)
			}
		})
	}
}

func TestValidatePrecision_MarketBuyByQuote(t *testing.T) {
	cfg := precisionFixtureCfg()
	cases := []struct {
		name     string
		quoteQty string
		wantPass bool
		want     etcdcfg.RejectReason
	}{
		{"valid", "10.00", true, etcdcfg.RejectNone},
		{"bad-quote-step", "10.001", false, etcdcfg.RejectInvalidQuoteStep},
		{"below-min-quote-qty", "0.50", false, etcdcfg.RejectMinQuoteQty},
		{"below-min-quote-amount", "2.00", false, etcdcfg.RejectMinQuoteAmount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := PlaceOrderRequest{
				UserID: "u1", Symbol: "BTC-USDT",
				Side: counterstate.SideBid, OrderType: counterstate.OrderTypeMarket,
				QuoteQty: dec.New(c.quoteQty),
			}
			reason, pass := validatePrecision(cfg, req)
			if pass != c.wantPass {
				t.Errorf("pass=%v want=%v (reason=%s)", pass, c.wantPass, reason)
			}
			if !c.wantPass && reason != c.want {
				t.Errorf("reason=%s want=%s", reason, c.want)
			}
		})
	}
}

// ADR-0053 M3: without a ReferencePrice (BFF cache miss / cold start),
// MARKET-by-base precision validation is skipped to avoid rejecting orders
// we can't fairly evaluate.
func TestValidatePrecision_MarketByBaseSkipsWithoutReferencePrice(t *testing.T) {
	cfg := precisionFixtureCfg()
	req := PlaceOrderRequest{
		UserID: "u1", Symbol: "BTC-USDT",
		Side: counterstate.SideAsk, OrderType: counterstate.OrderTypeMarket,
		Qty: dec.New("0.00000001"), // deliberately violates MinQty
		// ReferencePrice not set
	}
	reason, pass := validatePrecision(cfg, req)
	if !pass || reason != etcdcfg.RejectNone {
		t.Errorf("no ReferencePrice should skip; got reason=%s pass=%v", reason, pass)
	}
}

// ADR-0053 M3.b: with a ReferencePrice supplied (BFF filled from depth
// cache), MARKET-by-base gets the full lot / min-qty / min-amount
// treatment.
func TestValidatePrecision_MarketByBaseEnforcedWithReferencePrice(t *testing.T) {
	cfg := precisionFixtureCfg()
	cases := []struct {
		name string
		qty  string
		ref  string
		want etcdcfg.RejectReason
		pass bool
	}{
		{"valid", "0.001", "50000", etcdcfg.RejectNone, true},
		{"bad-step", "0.0000001", "50000", etcdcfg.RejectInvalidLotSize, false},
		{"below-market-min-qty", "0.00005", "50000", etcdcfg.RejectMinQty, false},
		{"below-min-quote-amount", "0.0001", "40000", etcdcfg.RejectMinQuoteAmount, false}, // 4 < 5
		{"valid-at-boundary", "0.0001", "50000", etcdcfg.RejectNone, true},                 // 5 == 5
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := PlaceOrderRequest{
				UserID: "u1", Symbol: "BTC-USDT",
				Side:           counterstate.SideAsk,
				OrderType:      counterstate.OrderTypeMarket,
				Qty:            dec.New(c.qty),
				ReferencePrice: dec.New(c.ref),
			}
			reason, pass := validatePrecision(cfg, req)
			if pass != c.pass {
				t.Errorf("pass=%v want=%v (reason=%s)", pass, c.pass, reason)
			}
			if !c.pass && reason != c.want {
				t.Errorf("reason=%s want=%s", reason, c.want)
			}
		})
	}
}

func TestMapOrderKind(t *testing.T) {
	cases := []struct {
		otype    counterstate.OrderType
		side     counterstate.Side
		quoteQty dec.Decimal
		want     etcdcfg.OrderKind
	}{
		{counterstate.OrderTypeLimit, counterstate.SideBid, dec.Zero, etcdcfg.OrderKindLimit},
		{counterstate.OrderTypeLimit, counterstate.SideAsk, dec.Zero, etcdcfg.OrderKindLimit},
		{counterstate.OrderTypeMarket, counterstate.SideBid, dec.New("10"), etcdcfg.OrderKindMarketBuyByQuote},
		{counterstate.OrderTypeMarket, counterstate.SideBid, dec.Zero, etcdcfg.OrderKindMarketByBase}, // buy with qty (no quoteQty)
		{counterstate.OrderTypeMarket, counterstate.SideAsk, dec.Zero, etcdcfg.OrderKindMarketByBase}, // sell
	}
	for _, c := range cases {
		got := mapOrderKind(c.otype, c.side, c.quoteQty)
		if got != c.want {
			t.Errorf("mapOrderKind(%v, %v, %s) = %d, want %d",
				c.otype, c.side, c.quoteQty.String(), got, c.want)
		}
	}
}

// End-to-end: PlaceOrder with a SymbolLookup that rejects -> Accepted=false.
func TestPlaceOrder_PrecisionRejectsBeforeSequencer(t *testing.T) {
	svc, _, _, _ := newOrderFixture(t)
	svc.SetSymbolLookup(func(symbol string) (etcdcfg.SymbolConfig, bool) {
		if symbol != "BTC-USDT" {
			return etcdcfg.SymbolConfig{}, false
		}
		return precisionFixtureCfg(), true
	})
	// Qty too small → RejectMinQty (step-valid but below MinQty).
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", ClientOrderID: "c-prec-1", Symbol: "BTC-USDT",
		Side: counterstate.SideBid, OrderType: counterstate.OrderTypeLimit,
		TIF:   counterstate.TIFGTC,
		Price: dec.New("50000.00"),
		Qty:   dec.New("0.00005"),
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Accepted {
		t.Fatal("expected rejection due to MinQty")
	}
	if res.RejectReason != string(etcdcfg.RejectMinQty) {
		t.Errorf("RejectReason=%q want=%q", res.RejectReason, etcdcfg.RejectMinQty)
	}
}

// Compat: nil lookup keeps legacy behaviour — bad-precision orders still
// go through freeze + sequencer. We probe by passing an order that would
// fail strict validation but construct freeze correctly. Balance assertions
// piggyback on the existing happy-path test fixture.
func TestPlaceOrder_NilLookupPreservesLegacyPath(t *testing.T) {
	svc, _, _, _ := newOrderFixture(t)
	// Do NOT call SetSymbolLookup → stays nil.
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", ClientOrderID: "c-nil-1", Symbol: "BTC-USDT",
		Side: counterstate.SideBid, OrderType: counterstate.OrderTypeLimit,
		TIF:   counterstate.TIFGTC,
		Price: dec.New("50000.00"),
		Qty:   dec.New("0.00005"), // would fail MinQty in strict mode
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !res.Accepted {
		t.Errorf("nil lookup should keep legacy pass-through; reason=%s", res.RejectReason)
	}
}

// Lookup returns (_, false) for unknown symbols — covered as "compat
// fallback" so admin adds a new symbol without immediately flipping every
// in-flight order stream to reject.
func TestPlaceOrder_LookupMissSkipsPrecision(t *testing.T) {
	svc, _, _, _ := newOrderFixture(t)
	svc.SetSymbolLookup(func(symbol string) (etcdcfg.SymbolConfig, bool) {
		return etcdcfg.SymbolConfig{}, false // always miss
	})
	res, err := svc.PlaceOrder(context.Background(), PlaceOrderRequest{
		UserID: "u1", ClientOrderID: "c-miss-1", Symbol: "BTC-USDT",
		Side: counterstate.SideBid, OrderType: counterstate.OrderTypeLimit,
		TIF:   counterstate.TIFGTC,
		Price: dec.New("50000.00"),
		Qty:   dec.New("0.00005"), // would fail MinQty in strict mode
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !res.Accepted {
		t.Errorf("lookup miss should skip precision; reason=%s", res.RejectReason)
	}
}
