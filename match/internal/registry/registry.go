// Package registry manages the set of symbols served by a match instance.
//
// On AddSymbol it restores the symbol from its last snapshot (via RestoreFn),
// starts a SymbolWorker goroutine, and registers it with the journal
// Dispatcher so order-event fan-out picks it up. On RemoveSymbol it
// unregisters from the Dispatcher, closes the worker, waits for the
// in-flight events to drain, and writes a final snapshot.
//
// Public methods are goroutine-safe so an etcd watcher and the startup path
// can drive it concurrently (ADR-0030).
package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/match/internal/journal"
	"github.com/xargin/opentrade/match/internal/sequencer"
)

// WorkerFactory builds a fresh SymbolWorker for the given symbol. The
// registry passes the workerFactory the shared outbox so all workers emit
// into the same producer pump.
type WorkerFactory func(symbol string) *sequencer.SymbolWorker

// RestoreFn optionally restores a worker's state from on-disk snapshot
// before Run starts. Must be safe to call with no snapshot present
// (caller returns nil).
type RestoreFn func(*sequencer.SymbolWorker) error

// SnapshotFn writes a final snapshot for the worker after RemoveSymbol
// closes it. Errors are logged but not returned to the caller — the worker
// is already gone; losing the final snapshot just means the next load
// replays from an older offset.
type SnapshotFn func(*sequencer.SymbolWorker)

// Config bundles the hooks.
type Config struct {
	Factory  WorkerFactory
	Restore  RestoreFn
	Snapshot SnapshotFn

	// Dispatcher is where workers register themselves so the order-event
	// consumer routes to them.
	Dispatcher *journal.Dispatcher

	Logger *zap.Logger
}

// Registry manages the lifecycle of per-symbol workers.
type Registry struct {
	cfg    Config
	parent context.Context

	mu     sync.Mutex
	active map[string]*handle
	closed bool
}

type handle struct {
	worker *sequencer.SymbolWorker
	cancel context.CancelFunc
	done   chan struct{}
}

// New constructs a Registry. parentCtx is used as the parent for per-worker
// contexts so Close propagates cancellation.
func New(parentCtx context.Context, cfg Config) (*Registry, error) {
	if cfg.Factory == nil {
		return nil, errors.New("registry: Factory required")
	}
	if cfg.Dispatcher == nil {
		return nil, errors.New("registry: Dispatcher required")
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &Registry{cfg: cfg, parent: parentCtx, active: make(map[string]*handle)}, nil
}

// AddSymbol starts a worker for symbol. No-op if the symbol is already
// active or the registry has been closed.
func (r *Registry) AddSymbol(symbol string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("registry: closed")
	}
	if _, ok := r.active[symbol]; ok {
		return nil
	}
	w := r.cfg.Factory(symbol)
	if w == nil {
		return fmt.Errorf("registry: factory returned nil for %s", symbol)
	}
	if r.cfg.Restore != nil {
		if err := r.cfg.Restore(w); err != nil {
			return fmt.Errorf("registry: restore %s: %w", symbol, err)
		}
	}
	r.cfg.Dispatcher.Register(w)

	wctx, cancel := context.WithCancel(r.parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(wctx)
	}()
	r.active[symbol] = &handle{worker: w, cancel: cancel, done: done}
	r.cfg.Logger.Info("symbol added",
		zap.String("symbol", symbol),
		zap.Uint64("match_seq_id", w.MatchSeq()))
	return nil
}

// RemoveSymbol tears down the worker for symbol. No-op if absent.
func (r *Registry) RemoveSymbol(symbol string) error {
	r.mu.Lock()
	h, ok := r.active[symbol]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	delete(r.active, symbol)
	r.mu.Unlock()

	r.cfg.Dispatcher.Unregister(symbol)
	// Close inbox so Run drains and exits naturally. Also cancel the ctx
	// so Run stops even if the inbox is being spammed.
	h.worker.Close()
	h.cancel()
	<-h.done

	if r.cfg.Snapshot != nil {
		r.cfg.Snapshot(h.worker)
	}
	r.cfg.Logger.Info("symbol removed", zap.String("symbol", symbol))
	return nil
}

// HasSymbol reports whether symbol is currently active.
func (r *Registry) HasSymbol(symbol string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.active[symbol]
	return ok
}

// Symbols returns the sorted list of active symbols (best-effort snapshot).
func (r *Registry) Symbols() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.active))
	for s := range r.active {
		out = append(out, s)
	}
	return out
}

// Workers exposes the live worker handles for operations that need to reach
// the underlying sequencer state (e.g. periodic snapshots). Callers must
// not retain pointers past RemoveSymbol.
func (r *Registry) Workers() []*sequencer.SymbolWorker {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*sequencer.SymbolWorker, 0, len(r.active))
	for _, h := range r.active {
		out = append(out, h.worker)
	}
	return out
}

// Close tears down every active worker and marks the registry as closed.
func (r *Registry) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	syms := make([]string, 0, len(r.active))
	for s := range r.active {
		syms = append(syms, s)
	}
	r.mu.Unlock()

	for _, s := range syms {
		_ = r.RemoveSymbol(s)
	}
}
