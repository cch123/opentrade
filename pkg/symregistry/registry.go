// Package symregistry is a shared per-symbol config cache backed by an
// etcdcfg.Source. Consumers query it for SymbolConfig lookups: counter
// enforces ADR-0053 precision filters during PlaceOrder, trigger enforces
// the ADR-0054 per-(user, symbol) active-trigger cap. Updates arrive via a
// background Watch goroutine.
//
// This is intentionally minimal — a sync.RWMutex-guarded map + seed +
// watch loop. Match has its own (richer) registry for shard ownership; the
// SymbolConfig consumers only need "symbol → SymbolConfig" lookup, so we
// keep a small dedicated type here in pkg/ for reuse.
package symregistry

import (
	"context"
	"sync"

	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// Registry caches SymbolConfigs observed from etcd.
type Registry struct {
	mu   sync.RWMutex
	data map[string]etcdcfg.SymbolConfig
}

// New returns an empty registry. Call Run with an etcdcfg.Source to start
// populating it.
func New() *Registry {
	return &Registry{data: map[string]etcdcfg.SymbolConfig{}}
}

// Get returns (cfg, true) when symbol is known, (zero, false) otherwise.
// Unknown symbol = precision validation skipped (compat fallback) —
// intentional so admin-created symbols don't instantly break orders that
// raced the config write.
func (r *Registry) Get(symbol string) (etcdcfg.SymbolConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cfg, ok := r.data[symbol]
	return cfg, ok
}

// Put is the write path, exposed for tests and for the watch loop. The
// watch loop calls Put on every EventPut.
func (r *Registry) Put(symbol string, cfg etcdcfg.SymbolConfig) {
	r.mu.Lock()
	r.data[symbol] = cfg
	r.mu.Unlock()
}

// Delete drops a symbol. Idempotent.
func (r *Registry) Delete(symbol string) {
	r.mu.Lock()
	delete(r.data, symbol)
	r.mu.Unlock()
}

// Len returns the current number of known symbols (for observability).
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.data)
}

// Run seeds the registry from source.List and streams changes from
// source.Watch until ctx is cancelled or the watch channel closes. The
// function returns the first fatal error, or nil if ctx was cancelled
// cleanly.
func (r *Registry) Run(ctx context.Context, source etcdcfg.Source) error {
	cfgs, rev, err := source.List(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	for s, c := range cfgs {
		r.data[s] = c
	}
	r.mu.Unlock()

	ch, err := source.Watch(ctx, rev+1)
	if err != nil {
		return err
	}
	for ev := range ch {
		switch ev.Type {
		case etcdcfg.EventPut:
			r.Put(ev.Symbol, ev.Config)
		case etcdcfg.EventDelete:
			r.Delete(ev.Symbol)
		}
	}
	return ctx.Err()
}
