package saga

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/asset/internal/holder"
	assetmetrics "github.com/xargin/opentrade/asset/internal/metrics"
	"github.com/xargin/opentrade/pkg/transferledger"
)

// newMetricsDriver builds a driver wired to a fresh Prometheus registry
// so counter assertions don't leak across tests.
func newMetricsDriver(t *testing.T) (*fixture, *assetmetrics.Saga) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ledger := transferledger.NewLedgerWithDB(db)
	ledger.SetClock(func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	reg := holder.NewRegistry()
	from := &fakeHolder{}
	to := &fakeHolder{}
	reg.Register("funding", from)
	reg.Register("spot", to)

	sagaMetrics := assetmetrics.NewSaga(prometheus.NewRegistry())

	d := New(Config{
		RPCTimeout:        100 * time.Millisecond,
		ForwardRetries:    2,
		ForwardBackoff:    1 * time.Millisecond,
		CompensateRetries: 3,
		CompensateBackoff: 1 * time.Millisecond,
	}, ledger, reg, zap.NewNop(), sagaMetrics)
	d.SetSleep(func(context.Context, time.Duration) {})
	d.SetClock(func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	return &fixture{
		driver: d, ledger: ledger, registry: reg,
		mock: mock, from: from, to: to, db: db,
	}, sagaMetrics
}

// TestDriver_MetricsHappyPath: INIT→DEBITED→COMPLETED bumps two
// transition counters, zero compensate/stuck.
func TestDriver_MetricsHappyPath(t *testing.T) {
	f, m := newMetricsDriver(t)
	f.from.outResps = []holder.Result{{Status: holder.StatusConfirmed}}
	f.to.inResps = []holder.Result{{Status: holder.StatusConfirmed}}
	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateDebited, "", 1)
	f.expectUpdate("saga-1", transferledger.StateDebited, transferledger.StateCompleted, "", 1)

	if _, err := f.driver.Run(context.Background(), sampleEntry()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := testutil.ToFloat64(m.TransitionsTotal.WithLabelValues("INIT", "DEBITED")); got != 1 {
		t.Errorf("transitions[INIT→DEBITED] = %v", got)
	}
	if got := testutil.ToFloat64(m.TransitionsTotal.WithLabelValues("DEBITED", "COMPLETED")); got != 1 {
		t.Errorf("transitions[DEBITED→COMPLETED] = %v", got)
	}
	if got := testutil.ToFloat64(m.CompensateAttempts); got != 0 {
		t.Errorf("compensate_attempts = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.StuckTotal); got != 0 {
		t.Errorf("stuck = %v, want 0", got)
	}
}

// TestDriver_MetricsStuckPath: compensate exhausts retries and the
// saga lands in COMPENSATE_STUCK, bumping stuck_total and
// compensate_attempts.
func TestDriver_MetricsStuckPath(t *testing.T) {
	f, m := newMetricsDriver(t)
	entry := sampleEntry()
	entry.State = transferledger.StateCompensating
	entry.RejectReason = "trigger"

	f.from.compErrs = []error{
		errors.New("timeout"), errors.New("timeout"), errors.New("timeout"),
	}
	f.from.compResps = []holder.Result{{}, {}, {}}

	f.mock.ExpectExec(`UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`).
		WithArgs(string(transferledger.StateCompensateStuck),
			sqlmock.AnyArg(), int64(1_700_000_000_000),
			"saga-1", string(transferledger.StateCompensating)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if _, err := f.driver.Run(context.Background(), entry); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := testutil.ToFloat64(m.StuckTotal); got != 1 {
		t.Errorf("stuck_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.CompensateAttempts); got != 3 {
		t.Errorf("compensate_attempts = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.TransitionsTotal.WithLabelValues("COMPENSATING", "COMPENSATE_STUCK")); got != 1 {
		t.Errorf("transition[COMPENSATING→COMPENSATE_STUCK] = %v", got)
	}
}
