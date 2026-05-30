package main

import (
	"context"
	"testing"
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	indexprice "github.com/xargin/opentrade/perp-pricing/internal/index"
	"github.com/xargin/opentrade/perp-pricing/internal/journal"
	"github.com/xargin/opentrade/pkg/dec"
	"go.uber.org/zap"
)

type publishedFunding struct {
	symbol  string
	roundID int64
	rate    dec.Decimal
}

type fakeMarkPublisher struct {
	marks    []string
	fundings []publishedFunding
}

func (f *fakeMarkPublisher) PublishMarkTick(_ context.Context, symbol string, _, _, _ dec.Decimal, _ int64, _, _ bool) error {
	f.marks = append(f.marks, symbol)
	return nil
}

func (f *fakeMarkPublisher) PublishFundingTick(_ context.Context, symbol string, roundID int64, rate, _ dec.Decimal, _ int64) error {
	f.fundings = append(f.fundings, publishedFunding{symbol: symbol, roundID: roundID, rate: rate})
	return nil
}

func testConfig() Config {
	return Config{
		SpotSymbol:         "BTC-USDT",
		PerpSymbol:         "BTC-USDT-PERP",
		Alpha:              "1",
		BasisCap:           "0",
		FundingRateCap:     "0",
		InterestRateDaily:  "0.0003",
		PremiumBand:        "0.0005",
		IndexQuorum:        2,
		IndexDeviationBand: "0.05",
	}
}

func fullAt(price string) *eventpb.OrderBookFull {
	return &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{{Price: price, Qty: "1"}},
		Asks: []*eventpb.OrderBookLevel{{Price: price, Qty: "1"}},
	}
}

func TestBuildSymbolRuntimes_PerSymbolFundingIntervals(t *testing.T) {
	cfg := testConfig()
	cfg.PerpSymbols = "BTC-USDT-PERP,ETH-USDT-PERP"
	cfg.SpotSymbols = "BTC-USDT-PERP=BTC-USDT,ETH-USDT-PERP=ETH-USDT"
	cfg.FundingIntervals = "ETH-USDT-PERP=4h"

	rts, err := buildSymbolRuntimes(cfg, 8*time.Hour, 5*time.Second, time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(rts) != 2 {
		t.Fatalf("runtimes = %d, want 2", len(rts))
	}
	if rts[0].PerpSymbol != "BTC-USDT-PERP" || rts[0].FundingInterval != 8*time.Hour {
		t.Fatalf("BTC runtime = %+v, want 8h", rts[0])
	}
	if rts[1].PerpSymbol != "ETH-USDT-PERP" || rts[1].SpotSymbol != "ETH-USDT" || rts[1].FundingInterval != 4*time.Hour {
		t.Fatalf("ETH runtime = %+v, want ETH spot and 4h", rts[1])
	}
}

func TestRunSymbolTick_IndependentFundingBoundaries(t *testing.T) {
	cfg := testConfig()
	cfg.PerpSymbols = "BTC-USDT-PERP,ETH-USDT-PERP"
	cfg.SpotSymbols = "BTC-USDT-PERP=BTC-USDT,ETH-USDT-PERP=ETH-USDT"
	cfg.FundingIntervals = "ETH-USDT-PERP=4h"
	start := time.Date(2026, 5, 30, 0, 30, 0, 0, time.UTC)
	rts, err := buildSymbolRuntimes(cfg, 8*time.Hour, 5*time.Second, start)
	if err != nil {
		t.Fatal(err)
	}

	book := journal.NewBook()
	now := time.Date(2026, 5, 30, 4, 0, 0, 0, time.UTC)
	book.ApplyFullAt("BTC-USDT", fullAt("100"), now.UnixMilli())
	book.ApplyFullAt("ETH-USDT", fullAt("200"), now.UnixMilli())
	indexBook := indexBookWithFreshSelfSources(t, rts, book)
	pub := &fakeMarkPublisher{}

	for _, rt := range rts {
		runSymbolTick(context.Background(), now, dec.New("20000"), book, indexBook, rt, pub, zap.NewNop())
	}
	if len(pub.fundings) != 1 {
		t.Fatalf("funding ticks = %+v, want exactly ETH 4h boundary", pub.fundings)
	}
	if got := pub.fundings[0].symbol; got != "ETH-USDT-PERP" {
		t.Fatalf("funding symbol = %s, want ETH-USDT-PERP", got)
	}
}

func TestBuildSymbolRuntimes_RejectsUnknownFundingOverride(t *testing.T) {
	cfg := testConfig()
	cfg.PerpSymbols = "BTC-USDT-PERP"
	cfg.FundingIntervals = "ETH-USDT-PERP=4h"
	if _, err := buildSymbolRuntimes(cfg, 8*time.Hour, 5*time.Second, time.Unix(0, 0).UTC()); err == nil {
		t.Fatal("expected unknown symbol in funding-intervals to fail")
	}
}

func indexBookWithFreshSelfSources(t *testing.T, rts []*symbolRuntime, book *journal.Book) *indexprice.SourceBook {
	t.Helper()
	idxBook := indexprice.NewSourceBook()
	for _, rt := range rts {
		mid, ts, ok := book.MidAt(rt.SpotSymbol)
		if !ok {
			t.Fatalf("missing spot book for %s", rt.SpotSymbol)
		}
		idxBook.Upsert(rt.SelfSourceName, mid, ts)
	}
	return idxBook
}
