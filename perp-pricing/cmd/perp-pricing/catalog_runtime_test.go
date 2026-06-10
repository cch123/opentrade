package main

// catalog_runtime_test.go covers the ADR-0075 catalog-driven runtime
// reconciliation: building runtimes from active configs, stamping
// config_version onto ticks, rebuilding on a version change, and dropping
// symbols with no effective config.

import (
	"context"
	"testing"
	"time"

	"github.com/xargin/opentrade/perp-pricing/internal/journal"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpcfg"
	"go.uber.org/zap"
)

type versionedMark struct {
	symbol  string
	version uint64
}

type versionedPublisher struct {
	marks    []versionedMark
	fundings []versionedMark
}

func (f *versionedPublisher) PublishMarkTick(_ context.Context, symbol string, _, _, _ dec.Decimal, _ int64, _, _ bool, v uint64) error {
	f.marks = append(f.marks, versionedMark{symbol, v})
	return nil
}

func (f *versionedPublisher) PublishFundingTick(_ context.Context, symbol string, _ int64, _, _ dec.Decimal, _ int64, v uint64) error {
	f.fundings = append(f.fundings, versionedMark{symbol, v})
	return nil
}

func catalogWithSymbol(t *testing.T) (*perpcfg.MemoryStore, *perpcfg.Cache) {
	t.Helper()
	store := perpcfg.NewMemoryStore()
	spec := perpcfg.PerpSymbol{
		Symbol: "BTC-USDT-PERP", ContractType: perpcfg.ContractLinearPerp,
		BaseAsset: "BTC", QuoteAsset: "USDT", SettleAsset: "USDT",
		ContractSize: dec.FromInt(1), PriceScale: 2, QtyScale: 3,
	}
	cfg := perpcfg.PerpSymbolConfig{
		Symbol: "BTC-USDT-PERP", Status: perpcfg.StatusTrading,
		Precision:   perpcfg.Precision{TickSize: dec.New("0.5"), QtyStep: dec.New("0.001")},
		OrderLimits: perpcfg.OrderLimits{},
		RiskTiers: []perpcfg.RiskTier{{RiskID: 1, MaxNotional: dec.FromInt(0),
			MaintMarginRatio: dec.New("0.005"), MaxLeverage: dec.New("100"), LiqFeeRate: dec.New("0.001")}},
		Funding: perpcfg.FundingParams{IntervalSeconds: 28800, InterestRate: dec.New("0.0003"),
			Cap: dec.New("0.0075"), Floor: dec.New("-0.0075"), Clamp: dec.New("0.0005")},
		Pricing: perpcfg.PricingParams{MarkEmaAlpha: dec.New("1"),
			ImpactNotional: dec.New("20000"), IndexDeviationBand: dec.New("0.05")},
		Fees:      perpcfg.FeeParams{TakerFeeRate: dec.New("0.00055")},
		RiskApply: perpcfg.RiskApplyStaged,
	}
	if err := store.CreateSymbol(context.Background(), spec, cfg); err != nil {
		t.Fatal(err)
	}
	cache := perpcfg.NewCache(perpcfg.CacheConfig{Store: store})
	if err := cache.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store, cache
}

func TestCatalogRuntimeSyncAndVersionStamping(t *testing.T) {
	store, cache := catalogWithSymbol(t)
	mgr := newRuntimeManager(nil, cache, dec.New("20000"), zap.NewNop())
	now := time.Date(2026, 6, 10, 0, 30, 0, 0, time.UTC)

	// sync() builds the runtime from the catalog (hot listing path).
	mgr.sync(now)
	rts := mgr.ordered()
	if len(rts) != 1 {
		t.Fatalf("runtimes = %d, want 1", len(rts))
	}
	rt := rts[0]
	if rt.ConfigVersion != 1 || rt.FundingInterval != 8*time.Hour || rt.SpotSymbol != "BTC-USDT" {
		t.Fatalf("runtime = %+v", rt)
	}

	// A mark tick carries the runtime's config version.
	book := journal.NewBook()
	book.ApplyFullAt("BTC-USDT", fullAt("100"), now.UnixMilli())
	book.ApplyFullAt("BTC-USDT-PERP", fullAt("101"), now.UnixMilli())
	idxBook := indexBookWithFreshSelfSources(t, rts, book)
	pub := &versionedPublisher{}
	runSymbolTick(context.Background(), now, book, idxBook, rt, pub, zap.NewNop())
	if len(pub.marks) != 1 || pub.marks[0].version != 1 {
		t.Fatalf("mark stamps = %+v, want v1", pub.marks)
	}

	// Publish v2 (4h funding interval): sync rebuilds the runtime, the next
	// tick stamps v2.
	versions, _ := store.ListVersions(context.Background(), "BTC-USDT-PERP")
	next := *versions[len(versions)-1]
	next.Funding.IntervalSeconds = 14400
	if _, err := store.PublishConfig(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if err := cache.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	mgr.sync(now)
	rt2 := mgr.ordered()[0]
	if rt2.ConfigVersion != 2 || rt2.FundingInterval != 4*time.Hour {
		t.Fatalf("rebuilt runtime = v%d interval %v", rt2.ConfigVersion, rt2.FundingInterval)
	}
	if rt2 == rt {
		t.Fatal("rebuild must produce a fresh runtime")
	}
	idxBook2 := indexBookWithFreshSelfSources(t, mgr.ordered(), book)
	runSymbolTick(context.Background(), now, book, idxBook2, rt2, pub, zap.NewNop())
	if last := pub.marks[len(pub.marks)-1]; last.version != 2 {
		t.Fatalf("post-rebuild mark stamped v%d", last.version)
	}

	// Funding boundary fires with the version stamp too: re-anchor the
	// boundary into the past, then cross it.
	rt2.LastBoundary = now.UTC().Truncate(rt2.FundingInterval).Add(-rt2.FundingInterval)
	later := now.UTC().Truncate(rt2.FundingInterval).Add(time.Minute)
	book.ApplyFullAt("BTC-USDT", fullAt("100"), later.UnixMilli())
	idxBook3 := indexBookWithFreshSelfSources(t, mgr.ordered(), book)
	runSymbolTick(context.Background(), later, book, idxBook3, rt2, pub, zap.NewNop())
	if len(pub.fundings) != 1 || pub.fundings[0].version != 2 {
		t.Fatalf("funding stamps = %+v, want one v2", pub.fundings)
	}
}

func TestCatalogRuntimeDroppedWhenNotEffective(t *testing.T) {
	_, cache := catalogWithSymbol(t)
	mgr := newRuntimeManager(nil, cache, dec.New("20000"), zap.NewNop())
	now := time.Now()
	mgr.sync(now)
	if len(mgr.ordered()) != 1 {
		t.Fatal("runtime missing after sync")
	}
	// Simulate the symbol disappearing from the active view (e.g. a cache
	// rebuilt from a store whose only versions are future-dated).
	empty := perpcfg.NewCache(perpcfg.CacheConfig{Store: perpcfg.NewMemoryStore()})
	if err := empty.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	mgr.catalog = empty
	mgr.sync(now)
	if len(mgr.ordered()) != 0 {
		t.Fatal("runtime must drop when no config is effective")
	}
}
