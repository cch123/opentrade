package transferledger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const frozenMs = int64(1_700_000_000_000)

func newMock(t *testing.T) (*Ledger, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	l := NewLedgerWithDB(db)
	l.SetClock(func() time.Time { return time.UnixMilli(frozenMs) })
	return l, mock
}

func sampleEntry() Entry {
	return Entry{
		TransferID: "tx-1",
		UserID:     "u-1",
		FromBiz:    "funding",
		ToBiz:      "spot",
		Asset:      "USDT",
		Amount:     "100",
	}
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

func TestCreate_Success(t *testing.T) {
	l, mock := newMock(t)

	const q = `INSERT INTO transfer_ledger
		(transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	mock.ExpectExec(q).
		WithArgs("tx-1", "u-1", "funding", "spot", "USDT", "100", "INIT", "", frozenMs, frozenMs).
		WillReturnResult(sqlmock.NewResult(0, 1))

	got, err := l.Create(context.Background(), sampleEntry())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got.State != StateInit {
		t.Errorf("state = %q, want INIT", got.State)
	}
	if got.CreatedAtMs != frozenMs || got.UpdatedAtMs != frozenMs {
		t.Errorf("timestamps not stamped: created=%d updated=%d", got.CreatedAtMs, got.UpdatedAtMs)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestCreate_DuplicateReturnsExisting(t *testing.T) {
	l, mock := newMock(t)

	const insertQ = `INSERT INTO transfer_ledger
		(transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	mock.ExpectExec(insertQ).
		WithArgs("tx-1", "u-1", "funding", "spot", "USDT", "100", "INIT", "", frozenMs, frozenMs).
		WillReturnError(errors.New("Error 1062: Duplicate entry 'tx-1' for key 'PRIMARY'"))

	const selectQ = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`
	existingRow := sqlmock.NewRows([]string{
		"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
		"state", "reject_reason", "created_at_ms", "updated_at_ms",
	}).AddRow("tx-1", "u-1", "funding", "spot", "USDT", "100",
		string(StateCompleted), "", frozenMs-1000, frozenMs-500)
	mock.ExpectQuery(selectQ).WithArgs("tx-1").WillReturnRows(existingRow)

	got, err := l.Create(context.Background(), sampleEntry())
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("err = %v, want ErrAlreadyExists", err)
	}
	if got.State != StateCompleted {
		t.Errorf("returned existing state = %q, want COMPLETED", got.State)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestCreate_NonDuplicateErrorPropagates(t *testing.T) {
	l, mock := newMock(t)

	const insertQ = `INSERT INTO transfer_ledger
		(transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	mock.ExpectExec(insertQ).
		WithArgs("tx-1", "u-1", "funding", "spot", "USDT", "100", "INIT", "", frozenMs, frozenMs).
		WillReturnError(errors.New("connection lost"))

	_, err := l.Create(context.Background(), sampleEntry())
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrAlreadyExists) {
		t.Errorf("err should NOT be ErrAlreadyExists: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

func TestGet_Success(t *testing.T) {
	l, mock := newMock(t)

	const q = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`
	rows := sqlmock.NewRows([]string{
		"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
		"state", "reject_reason", "created_at_ms", "updated_at_ms",
	}).AddRow("tx-1", "u-1", "funding", "spot", "USDT", "100",
		string(StateDebited), "", frozenMs, frozenMs)
	mock.ExpectQuery(q).WithArgs("tx-1").WillReturnRows(rows)

	got, err := l.Get(context.Background(), "tx-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != StateDebited {
		t.Errorf("state = %q, want DEBITED", got.State)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestGet_NotFound(t *testing.T) {
	l, mock := newMock(t)

	const q = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`
	mock.ExpectQuery(q).
		WithArgs("missing").
		WillReturnRows(sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}))

	_, err := l.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// UpdateState
// ---------------------------------------------------------------------------

func TestUpdateState_Success_ClearsReason(t *testing.T) {
	l, mock := newMock(t)

	const q = `UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`
	mock.ExpectExec(q).
		WithArgs("DEBITED", "", frozenMs, "tx-1", "INIT").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Pass a non-empty reject reason — must be cleared because DEBITED
	// doesn't carry one.
	if err := l.UpdateState(context.Background(), "tx-1", StateInit, StateDebited, "ignored"); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestUpdateState_Success_CarriesReason(t *testing.T) {
	l, mock := newMock(t)

	const q = `UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`
	mock.ExpectExec(q).
		WithArgs("FAILED", "insufficient_balance", frozenMs, "tx-1", "INIT").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := l.UpdateState(context.Background(), "tx-1", StateInit, StateFailed, "insufficient_balance"); err != nil {
		t.Fatalf("UpdateState: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestUpdateState_StateMismatch(t *testing.T) {
	l, mock := newMock(t)

	const updateQ = `UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`
	// 0 rows affected — either missing or state drifted.
	mock.ExpectExec(updateQ).
		WithArgs("COMPLETED", "", frozenMs, "tx-1", "DEBITED").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Disambiguation read: row exists but at a different state.
	const selectQ = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`
	mock.ExpectQuery(selectQ).WithArgs("tx-1").WillReturnRows(
		sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}).AddRow("tx-1", "u-1", "funding", "spot", "USDT", "100",
			string(StateCompleted), "", frozenMs, frozenMs),
	)

	err := l.UpdateState(context.Background(), "tx-1", StateDebited, StateCompleted, "")
	if !errors.Is(err, ErrStateMismatch) {
		t.Errorf("err = %v, want ErrStateMismatch", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestUpdateState_NotFound(t *testing.T) {
	l, mock := newMock(t)

	const updateQ = `UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`
	mock.ExpectExec(updateQ).
		WithArgs("COMPLETED", "", frozenMs, "missing", "DEBITED").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Disambiguation read returns no rows.
	const selectQ = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`
	mock.ExpectQuery(selectQ).WithArgs("missing").WillReturnRows(
		sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}),
	)

	err := l.UpdateState(context.Background(), "missing", StateDebited, StateCompleted, "")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ListPending
// ---------------------------------------------------------------------------

func TestListPending(t *testing.T) {
	l, mock := newMock(t)

	const q = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger
		WHERE state IN ('INIT','DEBITED','COMPENSATING')
		ORDER BY updated_at_ms ASC
		LIMIT ?`
	rows := sqlmock.NewRows([]string{
		"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
		"state", "reject_reason", "created_at_ms", "updated_at_ms",
	}).
		AddRow("tx-a", "u-1", "funding", "spot", "USDT", "50",
			string(StateInit), "", frozenMs-1000, frozenMs-1000).
		AddRow("tx-b", "u-2", "spot", "funding", "BTC", "0.1",
			string(StateDebited), "", frozenMs-500, frozenMs-200)

	mock.ExpectQuery(q).WithArgs(50).WillReturnRows(rows)

	out, err := l.ListPending(context.Background(), 50)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	if out[0].TransferID != "tx-a" || out[0].State != StateInit {
		t.Errorf("row 0 = %+v", out[0])
	}
	if out[1].TransferID != "tx-b" || out[1].State != StateDebited {
		t.Errorf("row 1 = %+v", out[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestListPending_DefaultLimit(t *testing.T) {
	l, mock := newMock(t)

	const q = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger
		WHERE state IN ('INIT','DEBITED','COMPENSATING')
		ORDER BY updated_at_ms ASC
		LIMIT ?`
	mock.ExpectQuery(q).WithArgs(200).WillReturnRows(
		sqlmock.NewRows([]string{
			"transfer_id", "user_id", "from_biz", "to_biz", "asset", "amount",
			"state", "reject_reason", "created_at_ms", "updated_at_ms",
		}),
	)

	_, err := l.ListPending(context.Background(), 0)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// State helpers
// ---------------------------------------------------------------------------

func TestCountByState(t *testing.T) {
	l, mock := newMock(t)

	mock.ExpectQuery(`SELECT state, COUNT(*) FROM transfer_ledger GROUP BY state`).
		WillReturnRows(sqlmock.NewRows([]string{"state", "count"}).
			AddRow(string(StateCompleted), 42).
			AddRow(string(StateCompensateStuck), 1))

	got, err := l.CountByState(context.Background())
	if err != nil {
		t.Fatalf("CountByState: %v", err)
	}
	if got[StateCompleted] != 42 {
		t.Errorf("COMPLETED = %d, want 42", got[StateCompleted])
	}
	if got[StateCompensateStuck] != 1 {
		t.Errorf("COMPENSATE_STUCK = %d, want 1", got[StateCompensateStuck])
	}
	// Unseen states are present with 0 so gauges can reset labels
	// rather than leave stale values.
	if got[StateInit] != 0 {
		t.Errorf("INIT = %d, want 0", got[StateInit])
	}
	if len(got) != len(AllStates) {
		t.Errorf("map size = %d, want %d", len(got), len(AllStates))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestState_IsTerminal(t *testing.T) {
	cases := map[State]bool{
		StateInit:            false,
		StateDebited:         false,
		StateCompensating:    false,
		StateCompleted:       true,
		StateFailed:          true,
		StateCompensated:     true,
		StateCompensateStuck: true,
	}
	for s, want := range cases {
		if got := s.IsTerminal(); got != want {
			t.Errorf("%q.IsTerminal() = %v, want %v", s, got, want)
		}
	}
}
