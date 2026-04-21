package saga

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/asset/internal/metrics"
	"github.com/xargin/opentrade/pkg/transferledger"
)

// Reconciler periodically aggregates transfer_ledger state counts into
// the Prometheus gauge (ADR-0057 M6). The gauge is the "what is the
// saga population doing right now" snapshot; counters on Driver cover
// flow-rate questions. The reconciler is the only writer to
// saga_state_count.
//
// Secondary duty: watchdog for COMPENSATE_STUCK. If the count ticks up,
// a WARN log is emitted so an on-call can investigate — the metric
// itself is the alertable signal, but a log trail on the asset-service
// side makes post-incident diagnostics easier.
type Reconciler struct {
	ledger   *transferledger.Ledger
	metrics  *metrics.Saga
	logger   *zap.Logger
	interval time.Duration

	// lastStuck tracks the previous COMPENSATE_STUCK count so we only
	// log on upticks (a long-stuck saga shouldn't spam the log).
	lastStuck int64
}

// ReconcilerConfig tunes the reconciler. Zero Interval defaults to
// DefaultReconcileInterval.
type ReconcilerConfig struct {
	Interval time.Duration
}

// DefaultReconcileInterval is conservative — the gauge is a population
// snapshot, not a hot-path metric. Operators can tighten it via flag if
// they need sub-30s granularity.
const DefaultReconcileInterval = 30 * time.Second

// NewReconciler wires a Reconciler. metrics must be non-nil — there's
// no point running a reconciler without a gauge to update.
func NewReconciler(cfg ReconcilerConfig, ledger *transferledger.Ledger, m *metrics.Saga, logger *zap.Logger) *Reconciler {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultReconcileInterval
	}
	return &Reconciler{
		ledger:   ledger,
		metrics:  m,
		logger:   logger,
		interval: cfg.Interval,
	}
}

// Run ticks until ctx is done. Each tick runs ReconcileOnce with a
// bounded timeout so a slow DB can't pile up queries. The first
// reconcile runs immediately so the gauge is populated before the first
// Prometheus scrape (typical scrape interval is 15s, longer than the
// first tick would otherwise wait).
func (r *Reconciler) Run(ctx context.Context) error {
	if err := r.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
		r.logger.Warn("saga: reconcile failed", zap.Error(err))
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Warn("saga: reconcile failed", zap.Error(err))
			}
		}
	}
}

// ReconcileOnce performs a single sweep: query current state counts
// and refresh the gauge. Exported for tests and for operators who
// want to trigger a reconcile via an admin endpoint.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	counts, err := r.ledger.CountByState(queryCtx)
	if err != nil {
		return err
	}
	for state, n := range counts {
		r.metrics.StateCount.WithLabelValues(string(state)).Set(float64(n))
	}
	// Log uptick on STUCK — both for post-incident trails and to give
	// pre-alert operators a head start.
	stuck := counts[transferledger.StateCompensateStuck]
	if stuck > r.lastStuck {
		r.logger.Warn("saga: COMPENSATE_STUCK population grew; investigate operator queue",
			zap.Int64("stuck_total", stuck),
			zap.Int64("delta", stuck-r.lastStuck))
	}
	r.lastStuck = stuck
	return nil
}
