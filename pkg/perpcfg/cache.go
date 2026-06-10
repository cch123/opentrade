package perpcfg

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

// View is one symbol's resolved read: the stable spec plus the config version
// active at query time. Cfg points into the cache's immutable rows — callers
// must treat it as read-only.
type View struct {
	Spec PerpSymbol
	Cfg  *PerpSymbolConfig
}

// CacheConfig tunes the catalog cache.
type CacheConfig struct {
	Store        Store
	PollInterval time.Duration // anchor poll cadence; default 1s
	// MaxStaleness bounds how old the last successful store sync may be
	// before Stale() trips and admission must fail closed (ADR-0075:
	// "Match 必须拒绝…本地缓存过期超过阈值的 symbol"). 0 = default 30s;
	// negative disables the check.
	MaxStaleness time.Duration
	Logger       *zap.Logger
	Clock        func() time.Time // nil → time.Now
}

// Cache is the service-local read model of the catalog (ADR-0075 §4). It
// polls the store's single-row anchor; on change it reloads the full catalog
// and swaps each symbol's entry atomically. Activation at effective_from_ms
// needs no writer: Active resolves the effective version per query from the
// already-loaded history, so the switch is per-symbol atomic by construction.
type Cache struct {
	store        Store
	pollInterval time.Duration
	maxStaleness time.Duration
	logger       *zap.Logger
	clock        func() time.Time

	mu         sync.RWMutex
	symbols    map[string]*CatalogSymbol
	anchor     uint64
	loaded     bool
	lastSyncMs int64
}

// NewCache wires a cache; call Load before serving, then Run for the poll
// loop.
func NewCache(cfg CacheConfig) *Cache {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.MaxStaleness == 0 {
		cfg.MaxStaleness = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &Cache{
		store:        cfg.Store,
		pollInterval: cfg.PollInterval,
		maxStaleness: cfg.MaxStaleness,
		logger:       cfg.Logger,
		clock:        cfg.Clock,
		symbols:      map[string]*CatalogSymbol{},
	}
}

// Load performs the initial blocking catalog read. Services MUST fail startup
// when this errors: starting with an empty cache would fail-closed every
// admission, and worse, hide a misconfigured store behind reject noise.
func (c *Cache) Load(ctx context.Context) error {
	cat, err := c.store.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("perpcfg: initial catalog load: %w", err)
	}
	c.install(cat)
	return nil
}

// Run polls the anchor until ctx is done. Poll errors are logged and retried
// next tick; the cache keeps serving the last good catalog, with Stale()
// tripping once MaxStaleness elapses.
func (c *Cache) Run(ctx context.Context) {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.SyncOnce(ctx); err != nil && ctx.Err() == nil {
				c.logger.Warn("perp catalog poll failed", zap.Error(err))
			}
		}
	}
}

// SyncOnce runs one poll step: read the anchor, reload on change. Exposed so
// tests (and admin tooling) can drive the cache deterministically.
func (c *Cache) SyncOnce(ctx context.Context) error {
	anchor, err := c.store.AnchorVersion(ctx)
	if err != nil {
		return err
	}
	c.mu.RLock()
	unchanged := c.loaded && anchor == c.anchor
	c.mu.RUnlock()
	if unchanged {
		c.touch()
		return nil
	}
	cat, err := c.store.LoadAll(ctx)
	if err != nil {
		return err
	}
	c.install(cat)
	return nil
}

func (c *Cache) install(cat Catalog) {
	now := c.clock().UnixMilli()
	c.mu.Lock()
	c.symbols = cat.Symbols
	c.anchor = cat.Anchor
	c.loaded = true
	c.lastSyncMs = now
	c.mu.Unlock()
}

func (c *Cache) touch() {
	now := c.clock().UnixMilli()
	c.mu.Lock()
	c.lastSyncMs = now
	c.mu.Unlock()
}

// Stale reports whether the last successful store sync is older than
// MaxStaleness (or the cache never loaded). Admission paths combine this with
// the per-symbol lookup: stale cache → fail closed.
func (c *Cache) Stale() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.loaded {
		return true
	}
	if c.maxStaleness < 0 {
		return false
	}
	return c.clock().UnixMilli()-c.lastSyncMs > c.maxStaleness.Milliseconds()
}

// Active resolves symbol's view at the current clock. ok=false when the
// symbol is absent or no version is effective yet — callers reject
// (unknown_symbol_config, fail-closed).
func (c *Cache) Active(symbol string) (View, bool) {
	return c.ActiveAt(symbol, c.clock().UnixMilli())
}

// ActiveAt is Active at an explicit timestamp (tests / replay tooling).
func (c *Cache) ActiveAt(symbol string, nowMs int64) (View, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cs := c.symbols[symbol]
	if cs == nil {
		return View{}, false
	}
	cfg, ok := cs.ActiveAt(nowMs)
	if !ok {
		return View{}, false
	}
	return View{Spec: cs.Spec, Cfg: cfg}, true
}

// At returns the exact published version — staged-risk pinned lookups and
// historical replay.
func (c *Cache) At(symbol string, version uint64) (*PerpSymbolConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cs := c.symbols[symbol]
	if cs == nil {
		return nil, false
	}
	return cs.At(version)
}

// EffectiveRiskVersion resolves which version's risk tiers govern a position
// pinned at `pinned` (ADR-0075 §3 staged semantics): the highest
// RiskApply=IMMEDIATE version that is effective now overrides the pin;
// otherwise the pin holds. pinned 0 (pre-catalog positions / fresh opens)
// resolves to the active version. ok=false when the symbol has no active
// config.
func (c *Cache) EffectiveRiskVersion(symbol string, pinned uint64, nowMs int64) (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cs := c.symbols[symbol]
	if cs == nil {
		return 0, false
	}
	active, ok := cs.ActiveAt(nowMs)
	if !ok {
		return 0, false
	}
	if pinned == 0 || pinned > active.ConfigVersion {
		return active.ConfigVersion, true
	}
	out := pinned
	for _, v := range cs.Versions {
		if v.ConfigVersion > active.ConfigVersion || v.EffectiveFromMs > nowMs {
			break
		}
		if v.RiskApply == RiskApplyImmediate && v.ConfigVersion > out {
			out = v.ConfigVersion
		}
	}
	return out, true
}

// HasSymbol reports whether the symbol exists in the cache (regardless of
// whether any version is effective yet).
func (c *Cache) HasSymbol(symbol string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.symbols[symbol] != nil
}

// Symbols lists the cached symbols, sorted.
func (c *Cache) Symbols() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.symbols))
	for s := range c.symbols {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// LastSyncMs reports the last successful store sync (observability).
func (c *Cache) LastSyncMs() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastSyncMs
}
