package perpcfg

import (
	"context"
	"testing"
	"time"

	"github.com/xargin/opentrade/pkg/dec"
)

// fakeClock is a manually advanced time source.
type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time          { return f.now }
func (f *fakeClock) Advance(d time.Duration) { f.now = f.now.Add(d) }

func newTestCache(t *testing.T, store Store, clk *fakeClock) *Cache {
	t.Helper()
	c := NewCache(CacheConfig{Store: store, MaxStaleness: 10 * time.Second, Clock: clk.Now})
	if err := c.Load(context.Background()); err != nil {
		t.Fatalf("load: %v", err)
	}
	return c
}

func mustCreate(t *testing.T, store *MemoryStore, symbol string) {
	t.Helper()
	spec := validSpec()
	spec.Symbol = symbol
	cfg := validConfig()
	cfg.Symbol = symbol
	if err := store.CreateSymbol(context.Background(), spec, cfg); err != nil {
		t.Fatalf("create %s: %v", symbol, err)
	}
}

func TestCacheFailClosedOnUnknownSymbol(t *testing.T) {
	store := NewMemoryStore()
	clk := &fakeClock{now: time.UnixMilli(1_000_000)}
	c := newTestCache(t, store, clk)
	if _, ok := c.Active("BTC-USDT-PERP"); ok {
		t.Fatal("empty cache must not resolve a view")
	}
	if c.HasSymbol("BTC-USDT-PERP") {
		t.Fatal("empty cache must not claim the symbol")
	}
}

func TestCacheActivationAtEffectiveTime(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	clk := &fakeClock{now: time.UnixMilli(1_000_000)}
	mustCreate(t, store, "BTC-USDT-PERP")
	c := newTestCache(t, store, clk)

	v, ok := c.Active("BTC-USDT-PERP")
	if !ok || v.Cfg.ConfigVersion != 1 {
		t.Fatalf("want v1 active, got %+v ok=%v", v.Cfg, ok)
	}

	// Publish v2 effective 5s in the future: tick size changes 0.5 → 0.1.
	next := *v.Cfg
	next.Precision.TickSize = dec.New("0.1")
	next.EffectiveFromMs = clk.Now().Add(5 * time.Second).UnixMilli()
	if _, err := store.PublishConfig(ctx, next); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := c.SyncOnce(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Before the boundary the OLD version must serve — the whole point of the
	// scheduled switch is that admission flips atomically at effective time,
	// not at publish time.
	v, _ = c.Active("BTC-USDT-PERP")
	if v.Cfg.ConfigVersion != 1 {
		t.Fatalf("pre-boundary active = v%d, want v1", v.Cfg.ConfigVersion)
	}
	clk.Advance(5 * time.Second)
	v, _ = c.Active("BTC-USDT-PERP")
	if v.Cfg.ConfigVersion != 2 || !dec.Equal(v.Cfg.Precision.TickSize, dec.New("0.1")) {
		t.Fatalf("post-boundary active = v%d tick=%s, want v2 tick=0.1",
			v.Cfg.ConfigVersion, v.Cfg.Precision.TickSize)
	}

	// The view is one immutable row: precision and risk tiers always come
	// from the SAME version (per-symbol atomic replacement).
	if v.Cfg.ConfigVersion != 2 {
		t.Fatal("torn view")
	}
}

func TestCacheSymbolOnlyFutureVersions(t *testing.T) {
	store := NewMemoryStore()
	clk := &fakeClock{now: time.UnixMilli(1_000_000)}
	spec := validSpec()
	cfg := validConfig()
	cfg.Status = StatusPreopen
	cfg.EffectiveFromMs = clk.Now().Add(time.Hour).UnixMilli() // listed, not yet effective
	if err := store.CreateSymbol(context.Background(), spec, cfg); err != nil {
		t.Fatalf("create: %v", err)
	}
	c := newTestCache(t, store, clk)
	if !c.HasSymbol("BTC-USDT-PERP") {
		t.Fatal("symbol should be known")
	}
	if _, ok := c.Active("BTC-USDT-PERP"); ok {
		t.Fatal("no version effective yet — Active must fail closed")
	}
	clk.Advance(time.Hour)
	if _, ok := c.Active("BTC-USDT-PERP"); !ok {
		t.Fatal("version should activate after effective time")
	}
}

func TestCacheStaleness(t *testing.T) {
	store := NewMemoryStore()
	clk := &fakeClock{now: time.UnixMilli(1_000_000)}
	mustCreate(t, store, "BTC-USDT-PERP")
	c := newTestCache(t, store, clk)
	if c.Stale() {
		t.Fatal("fresh cache must not be stale")
	}
	// Successful polls (even with no change) keep it fresh.
	clk.Advance(8 * time.Second)
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	clk.Advance(8 * time.Second)
	if c.Stale() {
		t.Fatal("8s since last sync with 10s budget must not be stale")
	}
	clk.Advance(3 * time.Second)
	if !c.Stale() {
		t.Fatal("11s since last sync must be stale")
	}
	// A cache that never loaded is stale by definition.
	c2 := NewCache(CacheConfig{Store: store, Clock: clk.Now})
	if !c2.Stale() {
		t.Fatal("unloaded cache must be stale")
	}
}

func TestCachePollDetectsPublish(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	clk := &fakeClock{now: time.UnixMilli(1_000_000)}
	mustCreate(t, store, "BTC-USDT-PERP")
	c := newTestCache(t, store, clk)

	cfg := validConfig()
	cfg.Status = StatusCancelOnly
	ver, err := store.PublishConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ver != 2 {
		t.Fatalf("assigned version = %d, want 2", ver)
	}
	// Anchor changed → SyncOnce reloads.
	if err := c.SyncOnce(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	v, _ := c.Active("BTC-USDT-PERP")
	if v.Cfg.ConfigVersion != 2 || v.Cfg.Status != StatusCancelOnly {
		t.Fatalf("active = v%d %s, want v2 CANCEL_ONLY", v.Cfg.ConfigVersion, v.Cfg.Status)
	}
	// Pinned lookup still reaches v1 (staged-risk path).
	old, ok := c.At("BTC-USDT-PERP", 1)
	if !ok || old.Status != StatusTrading {
		t.Fatalf("At(1) = %+v ok=%v", old, ok)
	}
}

func TestEffectiveRiskVersion(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	clk := &fakeClock{now: time.UnixMilli(1_000_000)}
	mustCreate(t, store, "BTC-USDT-PERP")
	c := newTestCache(t, store, clk)
	now := clk.Now().UnixMilli()

	// v2: staged tightening (lower leverage).
	v2 := validConfig()
	v2.RiskTiers[0].MaxLeverage = dec.New("75")
	v2.RiskApply = RiskApplyStaged
	if _, err := store.PublishConfig(ctx, v2); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	_ = c.SyncOnce(ctx)

	// A position pinned at v1 keeps v1 under staged tightening.
	if got, ok := c.EffectiveRiskVersion("BTC-USDT-PERP", 1, now); !ok || got != 1 {
		t.Fatalf("staged: effective = %d ok=%v, want 1", got, ok)
	}
	// pinned 0 (fresh open) → active.
	if got, _ := c.EffectiveRiskVersion("BTC-USDT-PERP", 0, now); got != 2 {
		t.Fatalf("fresh open effective = %d, want 2", got)
	}

	// v3: IMMEDIATE loosening — overrides every pin at or below it.
	v3 := validConfig()
	v3.RiskTiers[0].MaxLeverage = dec.New("125")
	v3.RiskApply = RiskApplyImmediate
	if _, err := store.PublishConfig(ctx, v3); err != nil {
		t.Fatalf("publish v3: %v", err)
	}
	_ = c.SyncOnce(ctx)
	if got, _ := c.EffectiveRiskVersion("BTC-USDT-PERP", 1, now); got != 3 {
		t.Fatalf("immediate override: effective = %d, want 3", got)
	}

	// v4: staged again — pins above v3 hold at their pin.
	v4 := validConfig()
	v4.RiskTiers[0].MaxLeverage = dec.New("100")
	v4.RiskApply = RiskApplyStaged
	if _, err := store.PublishConfig(ctx, v4); err != nil {
		t.Fatalf("publish v4: %v", err)
	}
	_ = c.SyncOnce(ctx)
	if got, _ := c.EffectiveRiskVersion("BTC-USDT-PERP", 3, now); got != 3 {
		t.Fatalf("staged after immediate: effective = %d, want 3", got)
	}

	// A future-dated IMMEDIATE version must NOT override before its boundary.
	v5 := validConfig()
	v5.RiskApply = RiskApplyImmediate
	v5.EffectiveFromMs = now + 60_000
	if _, err := store.PublishConfig(ctx, v5); err != nil {
		t.Fatalf("publish v5: %v", err)
	}
	_ = c.SyncOnce(ctx)
	if got, _ := c.EffectiveRiskVersion("BTC-USDT-PERP", 3, now); got != 3 {
		t.Fatalf("future immediate leaked: effective = %d, want 3", got)
	}
	if got, _ := c.EffectiveRiskVersion("BTC-USDT-PERP", 3, now+60_000); got != 5 {
		t.Fatalf("future immediate at boundary: effective = %d, want 5", got)
	}
}

func TestMemoryStoreRollbackPublishesCopy(t *testing.T) {
	// Rollback is "publish a new version copying the target" (ADR-0075 §5) —
	// exercised here at the store level; the admin endpoint composes this.
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreate(t, store, "BTC-USDT-PERP")
	v2 := validConfig()
	v2.Precision.TickSize = dec.New("0.1")
	if _, err := store.PublishConfig(ctx, v2); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	// Roll back to v1: copy its content, stamp source_version.
	versions, err := store.ListVersions(ctx, "BTC-USDT-PERP")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	rollback := *versions[0]
	rollback.SourceVersion = rollback.ConfigVersion
	rollback.Reason = "rollback to v1"
	ver, err := store.PublishConfig(ctx, rollback)
	if err != nil {
		t.Fatalf("rollback publish: %v", err)
	}
	if ver != 3 {
		t.Fatalf("rollback version = %d, want 3 (never overwrite history)", ver)
	}
	versions, _ = store.ListVersions(ctx, "BTC-USDT-PERP")
	if len(versions) != 3 {
		t.Fatalf("history length = %d, want 3", len(versions))
	}
	got := versions[2]
	if !dec.Equal(got.Precision.TickSize, dec.New("0.5")) || got.SourceVersion != 1 {
		t.Fatalf("rollback row = tick %s source %d, want tick 0.5 source 1",
			got.Precision.TickSize, got.SourceVersion)
	}
}

func TestMemoryStoreGuardrails(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	mustCreate(t, store, "BTC-USDT-PERP")
	if err := store.CreateSymbol(ctx, validSpec(), validConfig()); err != ErrSymbolExists {
		t.Fatalf("duplicate create err = %v", err)
	}
	missing := validConfig()
	missing.Symbol = "ETH-USDT-PERP"
	if _, err := store.PublishConfig(ctx, missing); err != ErrSymbolNotFound {
		t.Fatalf("publish unknown err = %v", err)
	}
	// IMMEDIATE tightening without a policy is refused by the store too.
	tight := validConfig()
	tight.RiskTiers[0].MaxLeverage = dec.New("50")
	tight.RiskApply = RiskApplyImmediate
	if _, err := store.PublishConfig(ctx, tight); err == nil {
		t.Fatal("IMMEDIATE tightening without policy must fail at the store")
	}
}
