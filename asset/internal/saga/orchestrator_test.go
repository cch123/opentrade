package saga

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/asset/internal/holder"
	"github.com/xargin/opentrade/pkg/transferledger"
)

// orchFixture wraps a sqlmock-backed ledger + fake holders + driver +
// orchestrator so each test can script the DB/holder responses
// precisely.
type orchFixture struct {
	orch     *Orchestrator
	ledger   *transferledger.Ledger
	mock     sqlmock.Sqlmock
	from     *fakeHolder
	to       *fakeHolder
	pub      *capturePub
	db       *sql.DB
}

func newOrchFixture(t *testing.T) *orchFixture {
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
	driver := New(Config{
		ProducerID:        "asset-test",
		RPCTimeout:        50 * time.Millisecond,
		ForwardRetries:    1,
		ForwardBackoff:    1 * time.Millisecond,
		CompensateRetries: 2,
		CompensateBackoff: 1 * time.Millisecond,
	}, ledger, reg, pub, zap.NewNop())
	driver.SetSleep(func(context.Context, time.Duration) {})
	driver.SetClock(func() time.Time { return time.UnixMilli(1_700_000_000_000) })

	orch := NewOrchestrator(OrchestratorConfig{}, ledger, driver, zap.NewNop())

	return &orchFixture{
		orch: orch, ledger: ledger, mock: mock,
		from: from, to: to, pub: pub, db: db,
	}
}

// expectCreate queues the INSERT performed by ledger.Create for a fresh
// saga. Tests that need Create to hit ErrAlreadyExists instead use
// expectCreateDuplicate.
func (f *orchFixture) expectCreate(userID, transferID, fromBiz, toBiz, asset, amount string) {
	const q = `INSERT INTO transfer_ledger
		(transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	f.mock.ExpectExec(q).
		WithArgs(transferID, userID, fromBiz, toBiz, asset, amount,
			string(transferledger.StateInit), "",
			int64(1_700_000_000_000), int64(1_700_000_000_000)).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func (f *orchFixture) expectUpdate(transferID string, from, to transferledger.State, reason string, rowsAffected int64) {
	const q = `UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`
	f.mock.ExpectExec(q).
		WithArgs(string(to), reason, int64(1_700_000_000_000), transferID, string(from)).
		WillReturnResult(sqlmock.NewResult(0, rowsAffected))
}

// ---------------------------------------------------------------------------
// Transfer happy path
// ---------------------------------------------------------------------------

func TestOrchestrator_Transfer_HappyPath(t *testing.T) {
	f := newOrchFixture(t)
	f.from.outResps = []holder.Result{{Status: holder.StatusConfirmed}}
	f.to.inResps = []holder.Result{{Status: holder.StatusConfirmed}}

	f.expectCreate("u1", "saga-1", "funding", "spot", "USDT", "100")
	f.expectUpdate("saga-1", transferledger.StateInit, transferledger.StateDebited, "", 1)
	f.expectUpdate("saga-1", transferledger.StateDebited, transferledger.StateCompleted, "", 1)

	out, err := f.orch.Transfer(context.Background(), TransferInput{
		UserID: "u1", TransferID: "saga-1",
		FromBiz: "funding", ToBiz: "spot",
		Asset: "USDT", Amount: "100",
	})
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if out.State != transferledger.StateCompleted {
		t.Errorf("state = %q", out.State)
	}
	if !out.Terminal {
		t.Error("terminal should be true")
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Validation rejected BEFORE touching ledger
// ---------------------------------------------------------------------------

func TestOrchestrator_Transfer_InvalidArgs(t *testing.T) {
	f := newOrchFixture(t)

	cases := []TransferInput{
		{TransferID: "t", FromBiz: "funding", ToBiz: "spot", Asset: "USDT", Amount: "100"},   // missing user
		{UserID: "u1", FromBiz: "funding", ToBiz: "spot", Asset: "USDT", Amount: "100"},      // missing tx
		{UserID: "u1", TransferID: "t", ToBiz: "spot", Asset: "USDT", Amount: "100"},         // missing from
		{UserID: "u1", TransferID: "t", FromBiz: "funding", Asset: "USDT", Amount: "100"},    // missing to
		{UserID: "u1", TransferID: "t", FromBiz: "funding", ToBiz: "funding", Asset: "X", Amount: "1"}, // same
		{UserID: "u1", TransferID: "t", FromBiz: "funding", ToBiz: "spot", Amount: "100"},    // missing asset
		{UserID: "u1", TransferID: "t", FromBiz: "funding", ToBiz: "spot", Asset: "USDT"},    // missing amount
	}
	for i, c := range cases {
		_, err := f.orch.Transfer(context.Background(), c)
		if err == nil {
			t.Errorf("case %d: expected error", i)
			continue
		}
		if !errors.Is(err, ErrInvalidRequest) && !errors.Is(err, ErrSameBiz) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations (no DB calls): %v", err)
	}
}

// ---------------------------------------------------------------------------
// Idempotent replay: same transfer_id on a terminal row returns
// immediately without re-driving.
// ---------------------------------------------------------------------------

func TestOrchestrator_Transfer_IdempotentReplay_Terminal(t *testing.T) {
	f := newOrchFixture(t)

	// Create returns duplicate — read-back shows COMPLETED.
	const insertQ = `INSERT INTO transfer_ledger
		(transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	f.mock.ExpectExec(insertQ).
		WithArgs("saga-1", "u1", "funding", "spot", "USDT", "100",
			string(transferledger.StateInit), "",
			int64(1_700_000_000_000), int64(1_700_000_000_000)).
		WillReturnError(errors.New("Error 1062: Duplicate entry 'saga-1' for key 'PRIMARY'"))
	f.mock.ExpectQuery(`SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`).
		WithArgs("saga-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}).AddRow("saga-1", "u1", "funding", "spot", "USDT", "100",
			string(transferledger.StateCompleted), "", int64(1_700_000_000_000), int64(1_700_000_000_000)))

	out, err := f.orch.Transfer(context.Background(), TransferInput{
		UserID: "u1", TransferID: "saga-1",
		FromBiz: "funding", ToBiz: "spot",
		Asset: "USDT", Amount: "100",
	})
	if err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if out.State != transferledger.StateCompleted {
		t.Errorf("state = %q", out.State)
	}
	// No holder calls — idempotent replay short-circuits.
	if atomic.LoadInt32(&f.from.outCalls) != 0 || atomic.LoadInt32(&f.to.inCalls) != 0 {
		t.Errorf("no holder calls expected; got from=%d to=%d", f.from.outCalls, f.to.inCalls)
	}
}

// ---------------------------------------------------------------------------
// Idempotent replay with different shape → ErrInvalidRequest
// ---------------------------------------------------------------------------

func TestOrchestrator_Transfer_ShapeConflict(t *testing.T) {
	f := newOrchFixture(t)
	const insertQ = `INSERT INTO transfer_ledger
		(transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	f.mock.ExpectExec(insertQ).
		WithArgs("saga-1", "u1", "funding", "spot", "USDT", "100",
			string(transferledger.StateInit), "",
			int64(1_700_000_000_000), int64(1_700_000_000_000)).
		WillReturnError(errors.New("Error 1062: Duplicate entry"))
	f.mock.ExpectQuery(`SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`).
		WithArgs("saga-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}).AddRow("saga-1", "u1", "funding", "spot", "BTC", "100", // different asset!
			string(transferledger.StateCompleted), "", int64(1_700_000_000_000), int64(1_700_000_000_000)))

	_, err := f.orch.Transfer(context.Background(), TransferInput{
		UserID: "u1", TransferID: "saga-1",
		FromBiz: "funding", ToBiz: "spot",
		Asset: "USDT", Amount: "100",
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v, want ErrInvalidRequest", err)
	}
}

// ---------------------------------------------------------------------------
// Recover picks up a pending DEBITED saga and drives it to COMPLETED
// ---------------------------------------------------------------------------

func TestOrchestrator_Recover(t *testing.T) {
	f := newOrchFixture(t)
	f.to.inResps = []holder.Result{{Status: holder.StatusConfirmed}}

	// ListPending returns one DEBITED row. Recover should drive it to
	// COMPLETED via one UpdateState.
	const listQ = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger
		WHERE state IN ('INIT','DEBITED','COMPENSATING')
		ORDER BY updated_at_ms ASC
		LIMIT ?`
	f.mock.ExpectQuery(listQ).WithArgs(500).WillReturnRows(
		sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}).AddRow("saga-recovered", "u1", "funding", "spot", "USDT", "50",
			string(transferledger.StateDebited), "", int64(1_699_999_999_000), int64(1_699_999_999_000)),
	)
	f.expectUpdate("saga-recovered", transferledger.StateDebited, transferledger.StateCompleted, "", 1)

	if err := f.orch.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if atomic.LoadInt32(&f.to.inCalls) != 1 {
		t.Errorf("to.TransferIn calls = %d", f.to.inCalls)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Recover with empty pending list is a no-op (no ListPending expected
// to have side effects beyond the query itself).
// ---------------------------------------------------------------------------

func TestOrchestrator_Recover_Empty(t *testing.T) {
	f := newOrchFixture(t)
	const listQ = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger
		WHERE state IN ('INIT','DEBITED','COMPENSATING')
		ORDER BY updated_at_ms ASC
		LIMIT ?`
	f.mock.ExpectQuery(listQ).WithArgs(500).WillReturnRows(sqlmock.NewRows([]string{
		"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
		"state", "reject_reason", "created_at_ms", "updated_at_ms",
	}))

	if err := f.orch.Recover(context.Background()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if err := f.mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Query returns the ledger row verbatim (trimmed TransferOutput shape)
// ---------------------------------------------------------------------------

func TestOrchestrator_Query(t *testing.T) {
	f := newOrchFixture(t)
	f.mock.ExpectQuery(`SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`).
		WithArgs("saga-q").
		WillReturnRows(sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}).AddRow("saga-q", "u1", "funding", "spot", "USDT", "5",
			string(transferledger.StateFailed), "insufficient_balance", int64(1), int64(2)))

	out, err := f.orch.Query(context.Background(), "saga-q")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if out.State != transferledger.StateFailed || out.Reason != "insufficient_balance" || !out.Terminal {
		t.Errorf("out = %+v", out)
	}
}
