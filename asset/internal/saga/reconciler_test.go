package saga

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/xargin/opentrade/asset/internal/metrics"
	"github.com/xargin/opentrade/pkg/transferledger"
)

func newReconcilerFixture(t *testing.T) (*Reconciler, *metrics.Saga, sqlmock.Sqlmock, *sql.DB, *observer.ObservedLogs) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ledger := transferledger.NewLedgerWithDB(db)

	m := metrics.NewSaga(prometheus.NewRegistry())
	core, obs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)

	r := NewReconciler(ReconcilerConfig{Interval: time.Minute}, ledger, m, logger)
	return r, m, mock, db, obs
}

// TestReconcileOnce_WritesGauges: a single reconcile sweep queries
// CountByState and pushes the counts into the Prometheus gauge.
func TestReconcileOnce_WritesGauges(t *testing.T) {
	r, m, mock, _, _ := newReconcilerFixture(t)

	const q = `SELECT state, COUNT(*) FROM transfer_ledger GROUP BY state`
	mock.ExpectQuery(q).WillReturnRows(
		sqlmock.NewRows([]string{"state", "count"}).
			AddRow(string(transferledger.StateInit), 2).
			AddRow(string(transferledger.StateCompleted), 7).
			AddRow(string(transferledger.StateCompensateStuck), 1),
	)

	if err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	checks := map[transferledger.State]float64{
		transferledger.StateInit:             2,
		transferledger.StateCompleted:        7,
		transferledger.StateCompensateStuck:  1,
		// Unseen states must be reset to 0 so a row flipping out of a
		// state doesn't leave a stale gauge value behind.
		transferledger.StateDebited:          0,
		transferledger.StateFailed:           0,
		transferledger.StateCompensating:     0,
		transferledger.StateCompensated:      0,
	}
	for state, want := range checks {
		got := testutil.ToFloat64(m.StateCount.WithLabelValues(string(state)))
		if got != want {
			t.Errorf("gauge[%s] = %v, want %v", state, got, want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestReconcileOnce_LogsOnStuckUptick: the reconciler emits a WARN when
// COMPENSATE_STUCK grows between ticks — no log on steady state.
func TestReconcileOnce_LogsOnStuckUptick(t *testing.T) {
	r, _, mock, _, obs := newReconcilerFixture(t)

	const q = `SELECT state, COUNT(*) FROM transfer_ledger GROUP BY state`

	// Tick 1: 0 stuck → no warn.
	mock.ExpectQuery(q).WillReturnRows(sqlmock.NewRows([]string{"state", "count"}))
	if err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("tick1: %v", err)
	}
	if obs.Len() != 0 {
		t.Errorf("no warn expected at 0 stuck, got %d logs", obs.Len())
	}

	// Tick 2: 1 stuck → warn.
	mock.ExpectQuery(q).WillReturnRows(
		sqlmock.NewRows([]string{"state", "count"}).
			AddRow(string(transferledger.StateCompensateStuck), 1),
	)
	if err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("tick2: %v", err)
	}
	if obs.Len() != 1 {
		t.Fatalf("expected 1 warn log, got %d", obs.Len())
	}
	log := obs.All()[0]
	if log.Level != zapcore.WarnLevel {
		t.Errorf("level = %v", log.Level)
	}

	// Tick 3: still 1 stuck → no new warn (no delta).
	mock.ExpectQuery(q).WillReturnRows(
		sqlmock.NewRows([]string{"state", "count"}).
			AddRow(string(transferledger.StateCompensateStuck), 1),
	)
	if err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("tick3: %v", err)
	}
	if obs.Len() != 1 {
		t.Errorf("steady-state should not re-warn, got %d logs", obs.Len())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// TestReconcileOnce_PropagatesDBError: a DB error surfaces so the
// caller (Run loop) can log it — gauges stay at their last-known
// values.
func TestReconcileOnce_PropagatesDBError(t *testing.T) {
	r, _, mock, _, _ := newReconcilerFixture(t)

	mock.ExpectQuery(`SELECT state, COUNT(*) FROM transfer_ledger GROUP BY state`).
		WillReturnError(errors.New("boom"))

	if err := r.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}

// TestRun_ExitsOnContextCancel: the Run loop returns cleanly when ctx
// is cancelled and does not error on a cancelled initial reconcile.
func TestRun_ExitsOnContextCancel(t *testing.T) {
	r, _, mock, _, _ := newReconcilerFixture(t)

	// First reconcile returns quickly.
	mock.ExpectQuery(`SELECT state, COUNT(*) FROM transfer_ledger GROUP BY state`).
		WillReturnRows(sqlmock.NewRows([]string{"state", "count"}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Give the initial reconcile a moment to land, then cancel.
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run exited with err: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not exit within deadline")
	}
}
