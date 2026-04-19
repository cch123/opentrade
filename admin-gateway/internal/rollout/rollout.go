// Package rollout executes ADR-0053 precision rollouts in the admin-gateway.
//
// A rollout is the transition from a symbol's current Tiers to its
// ScheduledChange.NewTiers at the moment ScheduledChange.EffectiveAt is
// reached. This package scans all symbol configs on a tick, picks out those
// whose scheduled change is due, and atomically Puts the new config back to
// etcd:
//
//	pre:  Tiers=old, PrecisionVersion=V,   ScheduledChange={new, V+1, t}
//	post: Tiers=new, PrecisionVersion=V+1, ScheduledChange=nil
//
// Design rationale (over doing this inside match / counter):
//   - admin-gateway already owns the write path to /cex/match/symbols/*.
//   - Rollout is a low-frequency ops event — seconds-scale latency (next
//     tick) is fine. No need to plumb a timer through the match worker
//     critical path.
//   - Single writer keeps CAS simple; match / counter can stay read-only
//     for these keys.
//
// M4.a scope: swap the config. M4.b (deferred) will add orderbook-walk +
// cancel of non-conformant resting orders when a symbol transitions from
// compatibility mode to strict precision for the first time; for tier
// splits (the expected hot path per ADR-0053 §3) no cancellation is
// needed because old orders retain their admission tier.
package rollout

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// EtcdSource is the subset of etcdcfg the rollout loop needs. The
// admin-gateway server already pins a wider interface; here we keep the
// surface minimal so tests can inject MemorySource cleanly.
type EtcdSource interface {
	List(ctx context.Context) (map[string]etcdcfg.SymbolConfig, int64, error)
	Put(ctx context.Context, symbol string, cfg etcdcfg.SymbolConfig) (int64, error)
}

// Clock is abstracted so tests can advance time deterministically.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Config bundles the loop's dependencies.
type Config struct {
	Etcd         EtcdSource
	Audit        adminaudit.Logger
	Logger       *zap.Logger
	Clock        Clock
	ScanInterval time.Duration // default 5s; must be > 0
}

// Runner is a stateless driver — Scan is idempotent, so restarts or
// multiple admin-gateway instances (not supported today, but future-safe)
// converge on the same state.
type Runner struct {
	cfg Config
}

// New wires a Runner. Audit = NopLogger is fine but not zero-value nil.
func New(cfg Config) (*Runner, error) {
	if cfg.Etcd == nil {
		return nil, fmt.Errorf("rollout: Etcd is required")
	}
	if cfg.Audit == nil {
		cfg.Audit = adminaudit.NopLogger{}
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = 5 * time.Second
	}
	return &Runner{cfg: cfg}, nil
}

// Run ticks Scan on ScanInterval until ctx is cancelled. First tick fires
// immediately so a freshly-started admin-gateway picks up any past-due
// rollouts (e.g. after a restart window).
func (r *Runner) Run(ctx context.Context) error {
	// Eager first scan (observability of startup + catch-up after restart).
	if err := r.Scan(ctx); err != nil {
		r.cfg.Logger.Warn("rollout first scan", zap.Error(err))
	}
	t := time.NewTicker(r.cfg.ScanInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := r.Scan(ctx); err != nil {
				r.cfg.Logger.Warn("rollout scan", zap.Error(err))
			}
		}
	}
}

// Scan lists all symbols, executes due rollouts, and returns the first
// unrecoverable error (if any). Individual-symbol errors are logged but
// don't abort the sweep — other symbols still get processed.
//
// Exposed separately from Run so ops tooling (e.g. a CLI "force rollout
// now") can drive a one-shot pass.
func (r *Runner) Scan(ctx context.Context) error {
	cfgs, _, err := r.cfg.Etcd.List(ctx)
	if err != nil {
		return fmt.Errorf("list symbols: %w", err)
	}
	now := r.cfg.Clock.Now()
	for symbol, cfg := range cfgs {
		if cfg.ScheduledChange == nil {
			continue
		}
		if cfg.ScheduledChange.EffectiveAt.After(now) {
			continue // not yet due
		}
		if err := r.execute(ctx, symbol, cfg, now); err != nil {
			r.cfg.Logger.Error("rollout execute failed",
				zap.String("symbol", symbol), zap.Error(err))
			// Continue — next scan will retry.
		}
	}
	return nil
}

// execute performs the atomic swap for a single symbol. It's unconditional
// about the outcome — when ValidateTierEvolution passed at schedule time
// and the ScheduledChange is still attached to the cfg, we trust it and
// Put. If etcd state diverged between List and Put (rare), the next Scan
// will reconcile.
func (r *Runner) execute(ctx context.Context, symbol string, cfg etcdcfg.SymbolConfig, now time.Time) error {
	change := cfg.ScheduledChange
	oldVer := cfg.PrecisionVersion

	cfg.Tiers = change.NewTiers
	cfg.PrecisionVersion = change.NewPrecisionVersion
	cfg.ScheduledChange = nil

	rev, putErr := r.cfg.Etcd.Put(ctx, symbol, cfg)

	params := map[string]any{
		"from_precision_version": oldVer,
		"to_precision_version":   change.NewPrecisionVersion,
		"effective_at":           change.EffectiveAt.Format(time.RFC3339),
		"executed_at":            now.Format(time.RFC3339),
		"new_tier_count":         len(change.NewTiers),
	}
	if change.Reason != "" {
		params["reason"] = change.Reason
	}
	status := adminaudit.StatusOK
	if putErr != nil {
		status = adminaudit.StatusFailed
	}
	_ = r.cfg.Audit.Log(adminaudit.Entry{
		AdminID: "admin-gateway/rollout",
		Op:      "admin.precision.rolled_out",
		Target:  symbol,
		Params:  params,
		Status:  status,
		Error:   errString(putErr),
	})
	if putErr != nil {
		return putErr
	}
	r.cfg.Logger.Info("precision rollout executed",
		zap.String("symbol", symbol),
		zap.Uint64("from_version", oldVer),
		zap.Uint64("to_version", change.NewPrecisionVersion),
		zap.Int64("etcd_revision", rev))
	return nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
