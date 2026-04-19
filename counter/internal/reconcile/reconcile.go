// Package reconcile audits Counter's in-memory balances against the
// `accounts` projection maintained by trade-dump (ADR-0008 §对账). It runs
// on a ticker; mismatches are logged at WARN with structured fields so
// external monitoring can alert.
//
// The audit is read-only: it never mutates Counter state or MySQL. Drift is
// a signal for operators to investigate (snapshot corruption, lost journal
// write, projection lag), not something we auto-heal.
//
// Flow (per run):
//
//  1. Snapshot in-memory users via ShardState.Users()
//  2. Batch-query MySQL `accounts` for those user_ids (chunks of 200)
//  3. For each (user_id, asset):
//     - memory non-zero, DB missing → WARN only_in_memory
//     - memory & DB both present but Available/Frozen differ → WARN value_diff
//  4. Emit one INFO summary per run (users_checked / mismatches)
//
// Non-goals (MVP):
//   - Detecting rows only in MySQL (projection lag / out-of-shard rows)
//   - Auto-repair or alerting — the operator reads logs
//   - Comparing against SUM(account_logs); the `accounts` projection is
//     already maintained with counter_seq_id guards by trade-dump (ADR-0028),
//     so any drift there would light up trade-dump tests before reaching us
package reconcile

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// Config tunes the reconciler.
type Config struct {
	ShardID  int
	Interval time.Duration
	// BatchSize caps users per IN() clause to keep the query within
	// reasonable MySQL planner / packet limits.
	BatchSize int
}

// Reconciler audits Counter memory against MySQL on a tick.
type Reconciler struct {
	cfg    Config
	state  *engine.ShardState
	db     *sql.DB
	logger *zap.Logger
}

// New constructs a Reconciler. Caller owns db lifecycle.
func New(cfg Config, state *engine.ShardState, db *sql.DB, logger *zap.Logger) *Reconciler {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	return &Reconciler{cfg: cfg, state: state, db: db, logger: logger}
}

// Run ticks until ctx is cancelled. Run does not block; each tick's errors
// are logged and swallowed so one MySQL blip doesn't stop future audits.
// Returns ctx.Err() when the context is done.
func (r *Reconciler) Run(ctx context.Context) error {
	if r.cfg.Interval <= 0 {
		r.logger.Info("reconcile disabled (interval <= 0)")
		<-ctx.Done()
		return ctx.Err()
	}
	r.logger.Info("reconcile loop started",
		zap.Int("shard_id", r.cfg.ShardID),
		zap.Duration("interval", r.cfg.Interval))
	tk := time.NewTicker(r.cfg.Interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tk.C:
			rep, err := r.RunOnce(ctx)
			if err != nil {
				r.logger.Error("reconcile run failed", zap.Error(err))
				continue
			}
			r.logger.Info("reconcile complete",
				zap.Int("shard_id", r.cfg.ShardID),
				zap.Int("users", rep.UsersChecked),
				zap.Int("assets", rep.AssetsChecked),
				zap.Int("mismatches", len(rep.Mismatches)))
		}
	}
}

// Report summarizes one audit pass.
type Report struct {
	UsersChecked  int
	AssetsChecked int
	Mismatches    []Mismatch
}

// MismatchKind is a short tag describing why we flagged a row.
type MismatchKind string

const (
	// MismatchOnlyInMemory: Counter has a non-zero balance but MySQL has no
	// matching row. Likely projection lag (fresh transfer just happened).
	MismatchOnlyInMemory MismatchKind = "only_in_memory"
	// MismatchValueDiff: both sides have the row but available/frozen
	// differs. Could be projection lag, but persistent diffs flag a bug.
	MismatchValueDiff MismatchKind = "value_diff"
)

// Mismatch is one drift observation. Fields stringify decimals so callers
// can log without adding shopspring to their context.
type Mismatch struct {
	UserID       string
	Asset        string
	Kind         MismatchKind
	MemAvailable string
	MemFrozen    string
	DBAvailable  string
	DBFrozen     string
}

// RunOnce performs a single audit pass. Exposed for tests + manual runs.
func (r *Reconciler) RunOnce(ctx context.Context) (Report, error) {
	users := r.state.Users()
	if len(users) == 0 {
		return Report{}, nil
	}
	// Build memory view first. This may miss transfers that land mid-run,
	// but the ticker cadence (hour scale) makes that negligible.
	mem := make(map[string]map[string]engine.Balance, len(users))
	for _, u := range users {
		cp := r.state.Account(u).Copy()
		if len(cp) == 0 {
			continue
		}
		mem[u] = cp
	}

	dbView, err := r.loadAccounts(ctx, users)
	if err != nil {
		return Report{}, fmt.Errorf("load accounts: %w", err)
	}

	rep := Report{UsersChecked: len(mem)}
	for userID, assets := range mem {
		for asset, b := range assets {
			rep.AssetsChecked++
			dbAssets, ok := dbView[userID]
			if !ok {
				rep.Mismatches = append(rep.Mismatches, Mismatch{
					UserID: userID, Asset: asset,
					Kind:         MismatchOnlyInMemory,
					MemAvailable: b.Available.String(),
					MemFrozen:    b.Frozen.String(),
				})
				continue
			}
			dbBal, ok := dbAssets[asset]
			if !ok {
				rep.Mismatches = append(rep.Mismatches, Mismatch{
					UserID: userID, Asset: asset,
					Kind:         MismatchOnlyInMemory,
					MemAvailable: b.Available.String(),
					MemFrozen:    b.Frozen.String(),
				})
				continue
			}
			if !dec.Equal(b.Available, dbBal.Available) || !dec.Equal(b.Frozen, dbBal.Frozen) {
				rep.Mismatches = append(rep.Mismatches, Mismatch{
					UserID: userID, Asset: asset,
					Kind:         MismatchValueDiff,
					MemAvailable: b.Available.String(),
					MemFrozen:    b.Frozen.String(),
					DBAvailable:  dbBal.Available.String(),
					DBFrozen:     dbBal.Frozen.String(),
				})
			}
		}
	}

	for _, m := range rep.Mismatches {
		r.logger.Warn("reconcile mismatch",
			zap.String("kind", string(m.Kind)),
			zap.String("user_id", m.UserID),
			zap.String("asset", m.Asset),
			zap.String("mem_available", m.MemAvailable),
			zap.String("mem_frozen", m.MemFrozen),
			zap.String("db_available", m.DBAvailable),
			zap.String("db_frozen", m.DBFrozen))
	}
	return rep, nil
}

// loadAccounts queries MySQL in batches and returns a
// user_id → asset → Balance map.
func (r *Reconciler) loadAccounts(ctx context.Context, users []string) (map[string]map[string]engine.Balance, error) {
	out := make(map[string]map[string]engine.Balance, len(users))
	for start := 0; start < len(users); start += r.cfg.BatchSize {
		end := start + r.cfg.BatchSize
		if end > len(users) {
			end = len(users)
		}
		batch := users[start:end]
		if err := r.loadBatch(ctx, batch, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r *Reconciler) loadBatch(ctx context.Context, batch []string, out map[string]map[string]engine.Balance) error {
	if len(batch) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(batch))
	placeholders = placeholders[:len(placeholders)-1]
	query := "SELECT user_id, asset, available, frozen FROM accounts WHERE user_id IN (" + placeholders + ")"
	args := make([]any, len(batch))
	for i, u := range batch {
		args[i] = u
	}
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var userID, asset, avail, frozen string
		if err := rows.Scan(&userID, &asset, &avail, &frozen); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		availDec, err := dec.Parse(avail)
		if err != nil {
			return fmt.Errorf("parse available for %s/%s: %w", userID, asset, err)
		}
		frozenDec, err := dec.Parse(frozen)
		if err != nil {
			return fmt.Errorf("parse frozen for %s/%s: %w", userID, asset, err)
		}
		if out[userID] == nil {
			out[userID] = make(map[string]engine.Balance)
		}
		out[userID][asset] = engine.Balance{Available: availDec, Frozen: frozenDec}
	}
	return rows.Err()
}
