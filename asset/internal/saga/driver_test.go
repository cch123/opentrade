package saga

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/asset/internal/holder"
	"github.com/xargin/opentrade/pkg/transferledger"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeHolder lets each test script the per-method response precisely.
type fakeHolder struct {
	mu sync.Mutex

	outResps  []holder.Result
	outErrs   []error
	outCalls  int32

	inResps  []holder.Result
	inErrs   []error
	inCalls  int32

	compResps []holder.Result
	compErrs  []error
	compCalls int32
}

func (f *fakeHolder) TransferOut(_ context.Context, _ holder.Request) (holder.Result, error) {
	i := atomic.AddInt32(&f.outCalls, 1) - 1
	return pickResult(f.outResps, f.outErrs, int(i))
}
func (f *fakeHolder) TransferIn(_ context.Context, _ holder.Request) (holder.Result, error) {
	i := atomic.AddInt32(&f.inCalls, 1) - 1
	return pickResult(f.inResps, f.inErrs, int(i))
}
func (f *fakeHolder) CompensateTransferOut(_ context.Context, _ holder.Request) (holder.Result, error) {
	i := atomic.AddInt32(&f.compCalls, 1) - 1
	return pickResult(f.compResps, f.compErrs, int(i))
}

func pickResult(rs []holder.Result, es []error, i int) (holder.Result, error) {
	if i >= len(rs) && i >= len(es) {
		// Default: Confirmed with no error. Useful when a test only
		// cares about a prefix of calls.
		return holder.Result{Status: holder.StatusConfirmed}, nil
	}
	var r holder.Result
	if i < len(rs) {
		r = rs[i]
	}
	var err error
	if i < len(es) {
		err = es[i]
	}
	return r, err
}

type capturePub struct {
	mu     sync.Mutex
	seq    uint64
	events []*eventpb.AssetJournalEvent
}

func (p *capturePub) Publish(_ context.Context, _ string, evt *eventpb.AssetJournalEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evt)
	return nil
}
func (p *capturePub) NextSeq() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seq++
	return p.seq
}
func (p *capturePub) Close(context.Context) error { return nil }

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

type fixture struct {
	driver   *Driver
	ledger   *transferledger.Ledger
	registry *holder.Registry
	pub      *capturePub
	mock     sqlmock.Sqlmock
	from     *fakeHolder
	to       *fakeHolder
	db       *sql.DB
}

func newFixture(t *testing.T) *fixture {
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

	pub := &capturePub{}

	d := New(Config{
		ProducerID:        "asset-test",
		RPCTimeout:        100 * time.Millisecond,
		ForwardRetries:    2,
		ForwardBackoff:    1 * time.Millisecond,
		CompensateRetries: 3,
		CompensateBackoff: 1 * time.Millisecond,
	}, ledger, reg, pub, zap.NewNop(), nil)
	d.SetSleep(func(context.Context, time.Duration) {}) // skip backoff waits
	d.SetClock(func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	return &fixture{
		driver: d, ledger: ledger, registry: reg,
		pub: pub, mock: mock, from: from, to: to, db: db,
	}
}

// expectUpdate queues one UpdateState + optional follow-up disambiguation
// read. rowsAffected controls whether the update "wins" (1) or loses to
// a concurrent driver (0).
func (f *fixture) expectUpdate(transferID string, from, to transferledger.State, reason string, rowsAffected int64) {
	const q = `UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`
	f.mock.ExpectExec(q).
		WithArgs(string(to), reason, int64(1_700_000_000_000), transferID, string(from)).
		WillReturnResult(sqlmock.NewResult(0, rowsAffected))
}

// sampleEntry returns an INIT-state ledger entry for driver tests.
// Tests skip the ledger.Create step (already covered by
// transferledger's own tests) and hand this directly to Run.
func sampleEntry() transferledger.Entry {
	return transferledger.Entry{
		TransferID:  "saga-1",
		UserID:      "u1",
		FromBiz:     "funding",
		ToBiz:       "spot",
		Asset:       "USDT",
		Amount:      "100",
		State:       transferledger.StateInit,
		CreatedAtMs: 1_700_000_000_000,
		UpdatedAtMs: 1_700_000_000_000,
	}
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

func TestRun_HappyPath(t *testing.T) {
	f := newFixture(t)
	f.from.outResps = []holder.Result{{Status: holder.StatusConfirmed}}
	f.to.inResps = []holder.Result{{Status: holder.StatusConfirmed}}

	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateDebited, "", 1)
	f.expectUpdate("saga-1", transferledger.StateDebited, transferledger.StateCompleted, "", 1)

	final, err := f.driver.Run(context.Background(), sampleEntry())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateCompleted {
		t.Errorf("final state = %q", final.State)
	}
	if atomic.LoadInt32(&f.from.outCalls) != 1 {
		t.Errorf("from.TransferOut calls = %d", f.from.outCalls)
	}
	if atomic.LoadInt32(&f.to.inCalls) != 1 {
		t.Errorf("to.TransferIn calls = %d", f.to.inCalls)
	}
	if len(f.pub.events) != 2 {
		t.Errorf("journal events = %d, want 2 (DEBITED, COMPLETED)", len(f.pub.events))
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// FAILED: from rejects
// ---------------------------------------------------------------------------

func TestRun_FromRejects_Failed(t *testing.T) {
	f := newFixture(t)
	f.from.outResps = []holder.Result{{
		Status: holder.StatusRejected,
		Reason: holder.ReasonInsufficientBalance,
	}}

	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateFailed, "insufficient_balance", 1)

	final, err := f.driver.Run(context.Background(), sampleEntry())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateFailed {
		t.Errorf("state = %q", final.State)
	}
	if final.RejectReason != "insufficient_balance" {
		t.Errorf("reason = %q", final.RejectReason)
	}
	if atomic.LoadInt32(&f.to.inCalls) != 0 {
		t.Errorf("to should not be called when from rejects")
	}
}

// ---------------------------------------------------------------------------
// COMPENSATED: to rejects after from debited
// ---------------------------------------------------------------------------

func TestRun_ToRejects_Compensates(t *testing.T) {
	f := newFixture(t)
	f.from.outResps = []holder.Result{{Status: holder.StatusConfirmed}}
	f.to.inResps = []holder.Result{{
		Status: holder.StatusRejected,
		Reason: holder.ReasonAssetFrozen,
	}}
	f.from.compResps = []holder.Result{{Status: holder.StatusConfirmed}}

	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateDebited, "", 1)
	f.expectUpdate("saga-1", transferledger.StateDebited, transferledger.StateCompensating, "asset_frozen", 1)
	f.expectUpdate("saga-1", transferledger.StateCompensating, transferledger.StateCompensated, "", 1)

	final, err := f.driver.Run(context.Background(), sampleEntry())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateCompensated {
		t.Errorf("state = %q", final.State)
	}
	if atomic.LoadInt32(&f.from.compCalls) != 1 {
		t.Errorf("compensate calls = %d", f.from.compCalls)
	}
}

// ---------------------------------------------------------------------------
// Transport retries on forward leg succeed after one failure
// ---------------------------------------------------------------------------

func TestRun_ForwardLegRetriesSucceed(t *testing.T) {
	f := newFixture(t)
	// First TransferOut returns a transport error, second succeeds.
	f.from.outErrs = []error{errors.New("conn refused"), nil}
	f.from.outResps = []holder.Result{{}, {Status: holder.StatusConfirmed}}
	f.to.inResps = []holder.Result{{Status: holder.StatusConfirmed}}

	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateDebited, "", 1)
	f.expectUpdate("saga-1", transferledger.StateDebited, transferledger.StateCompleted, "", 1)

	final, err := f.driver.Run(context.Background(), sampleEntry())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateCompleted {
		t.Errorf("state = %q", final.State)
	}
	if atomic.LoadInt32(&f.from.outCalls) != 2 {
		t.Errorf("from calls = %d, want 2 (1 transport fail + 1 success)", f.from.outCalls)
	}
}

// ---------------------------------------------------------------------------
// Transport retries exhausted on forward TransferOut → FAILED
// ---------------------------------------------------------------------------

func TestRun_ForwardLegExhausted_Failed(t *testing.T) {
	f := newFixture(t)
	// Every attempt fails with transport error.
	f.from.outErrs = []error{errors.New("conn refused"), errors.New("conn refused")}
	f.from.outResps = []holder.Result{{}, {}}

	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateFailed, "from_transport_error", 1)

	final, err := f.driver.Run(context.Background(), sampleEntry())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateFailed {
		t.Errorf("state = %q", final.State)
	}
	if final.RejectReason != "from_transport_error" {
		t.Errorf("reason = %q", final.RejectReason)
	}
}

// ---------------------------------------------------------------------------
// TransferIn transport exhausted → COMPENSATING → COMPENSATED
// ---------------------------------------------------------------------------

func TestRun_ToTransportExhausted_Compensates(t *testing.T) {
	f := newFixture(t)
	f.from.outResps = []holder.Result{{Status: holder.StatusConfirmed}}
	f.to.inErrs = []error{errors.New("conn refused"), errors.New("conn refused")}
	f.from.compResps = []holder.Result{{Status: holder.StatusConfirmed}}

	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateDebited, "", 1)
	f.expectUpdate("saga-1", transferledger.StateDebited, transferledger.StateCompensating, "to_transport_error", 1)
	f.expectUpdate("saga-1", transferledger.StateCompensating, transferledger.StateCompensated, "", 1)

	final, err := f.driver.Run(context.Background(), sampleEntry())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateCompensated {
		t.Errorf("state = %q", final.State)
	}
}

// ---------------------------------------------------------------------------
// COMPENSATE_STUCK: compensation exhausts retries
// ---------------------------------------------------------------------------

func TestRun_CompensateExhausted_Stuck(t *testing.T) {
	f := newFixture(t)
	// Skip the forward legs and start directly at COMPENSATING — that
	// lets us focus on compensate retry logic.
	entry := sampleEntry()
	entry.State = transferledger.StateCompensating
	entry.RejectReason = "test_trigger"

	// Every compensate attempt times out.
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

	final, err := f.driver.Run(context.Background(), entry)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateCompensateStuck {
		t.Errorf("state = %q, want COMPENSATE_STUCK", final.State)
	}
	if atomic.LoadInt32(&f.from.compCalls) != 3 {
		t.Errorf("compensate calls = %d, want 3 (retries exhausted)", f.from.compCalls)
	}
}

// ---------------------------------------------------------------------------
// Compensation rejected by from holder → STUCK (semantic failure)
// ---------------------------------------------------------------------------

func TestRun_CompensateRejected_Stuck(t *testing.T) {
	f := newFixture(t)
	entry := sampleEntry()
	entry.State = transferledger.StateCompensating
	entry.RejectReason = "test"

	f.from.compResps = []holder.Result{{
		Status: holder.StatusRejected,
		Reason: holder.ReasonInternal,
	}}

	f.mock.ExpectExec(`UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`).
		WithArgs(string(transferledger.StateCompensateStuck),
			"compensate_rejected:internal", int64(1_700_000_000_000),
			"saga-1", string(transferledger.StateCompensating)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	final, err := f.driver.Run(context.Background(), entry)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateCompensateStuck {
		t.Errorf("state = %q", final.State)
	}
	if atomic.LoadInt32(&f.from.compCalls) != 1 {
		t.Errorf("compensate calls = %d, want 1 (rejected, no retry)", f.from.compCalls)
	}
}

// ---------------------------------------------------------------------------
// Registry missing a biz_line at DEBITED→COMPLETED transition triggers
// compensation (to biz disappeared mid-flight). COMPENSATE_STUCK if
// from biz disappears during compensate.
// ---------------------------------------------------------------------------

func TestRun_ToBizUnknown_StartsCompensate(t *testing.T) {
	f := newFixture(t)
	// Replace the registry with one that only knows "funding"
	// (simulating a broken config or an unknown to_biz).
	f.registry = holder.NewRegistry()
	f.registry.Register("funding", f.from)
	// Rebuild driver with the neutered registry.
	f.driver = New(Config{
		ProducerID: "asset-test", RPCTimeout: 100 * time.Millisecond,
		ForwardRetries: 1, ForwardBackoff: 1 * time.Millisecond,
		CompensateRetries: 2, CompensateBackoff: 1 * time.Millisecond,
	}, f.ledger, f.registry, f.pub, zap.NewNop(), nil)
	f.driver.SetSleep(func(context.Context, time.Duration) {})
	f.driver.SetClock(func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	// Start at DEBITED so doCredit runs with an unknown to biz → COMPENSATING.
	entry := sampleEntry()
	entry.State = transferledger.StateDebited
	f.from.compResps = []holder.Result{{Status: holder.StatusConfirmed}}

	f.expectUpdate("saga-1", transferledger.StateDebited, transferledger.StateCompensating, "unknown_to_biz", 1)
	f.expectUpdate("saga-1", transferledger.StateCompensating, transferledger.StateCompensated, "", 1)

	final, err := f.driver.Run(context.Background(), entry)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateCompensated {
		t.Errorf("state = %q", final.State)
	}
}

// ---------------------------------------------------------------------------
// State mismatch: ledger drifted under us (Recover race) → reconcile
// by reading back and continuing.
// ---------------------------------------------------------------------------

func TestRun_StateMismatch_Reconciles(t *testing.T) {
	f := newFixture(t)
	f.from.outResps = []holder.Result{{Status: holder.StatusConfirmed}}
	f.to.inResps = []holder.Result{{Status: holder.StatusConfirmed}}

	// First UpdateState returns 0 rows affected (state drifted). The
	// ledger's UpdateState then does a disambiguation Get (SELECT #1)
	// to decide between ErrStateMismatch / ErrNotFound. Our driver
	// then does its OWN Get (SELECT #2) to read the authoritative
	// current state and continue from there.
	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateDebited, "", 0)
	debitedRow := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}).AddRow("saga-1", "u1", "funding", "spot", "USDT", "100",
			string(transferledger.StateDebited), "", int64(1_700_000_000_000), int64(1_700_000_000_000))
	}
	const selectQ = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`
	f.mock.ExpectQuery(selectQ).WithArgs("saga-1").WillReturnRows(debitedRow())
	f.mock.ExpectQuery(selectQ).WithArgs("saga-1").WillReturnRows(debitedRow())
	f.expectUpdate("saga-1", transferledger.StateDebited, transferledger.StateCompleted, "", 1)

	final, err := f.driver.Run(context.Background(), sampleEntry())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if final.State != transferledger.StateCompleted {
		t.Errorf("state = %q", final.State)
	}
	// Importantly, from.TransferOut should have been called exactly
	// once; reconciliation skips the ledger update but leaves the
	// earlier side effect in place.
	if atomic.LoadInt32(&f.from.outCalls) != 1 {
		t.Errorf("from calls = %d, want 1", f.from.outCalls)
	}
}
