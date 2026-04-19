package writer

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func newJournalMock(t *testing.T) (*MySQL, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewMySQLWithDB(db, 10), mock
}

func TestApplyJournalBatch_EmptyIsNoop(t *testing.T) {
	w, mock := newJournalMock(t)
	if err := w.ApplyJournalBatch(context.Background(), JournalBatch{}); err != nil {
		t.Fatalf("empty: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected sql: %v", err)
	}
}

func TestApplyJournalBatch_FullFlowInSingleTx(t *testing.T) {
	w, mock := newJournalMock(t)

	batch := JournalBatch{
		Orders: []OrderRow{
			{OrderID: 42, UserID: "u1", Symbol: "BTC-USDT", Side: 1, OrderType: 1, TIF: 1,
				Price: "100", Qty: "1", FrozenAmt: "100", Status: 1,
				CreatedAtMs: 1_700_000_000_000, UpdatedAtMs: 1_700_000_000_000, Kind: OrderRowInsert},
			{OrderID: 42, UserID: "u1", Status: 4, FilledQty: "1", UpdatedAtMs: 1_700_000_000_001, Kind: OrderRowUpdate},
		},
		Accounts: []AccountRow{
			{UserID: "u1", Asset: "USDT", Available: "400", Frozen: "100", CounterSeqID: 5},
		},
		AccountLogs: []AccountLogRow{
			{ShardID: 0, CounterSeqID: 5, Asset: "USDT", UserID: "u1",
				DeltaAvail: "-100", DeltaFrozen: "100",
				AvailAfter: "400", FrozenAfter: "100",
				BizType: "freeze_place_order", BizRefID: "42", TsUnixMs: 1_700_000_000_000},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO orders`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE orders SET status`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO accounts`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO account_logs`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := w.ApplyJournalBatch(context.Background(), batch); err != nil {
		t.Fatalf("ApplyJournalBatch: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestApplyJournalBatch_OnlyOrderUpdate(t *testing.T) {
	w, mock := newJournalMock(t)
	batch := JournalBatch{
		Orders: []OrderRow{
			{OrderID: 7, UserID: "u1", Status: 6, UpdatedAtMs: 1, Kind: OrderRowUpdate},
		},
	}
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE orders SET status`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := w.ApplyJournalBatch(context.Background(), batch); err != nil {
		t.Fatalf("%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestApplyJournalBatch_RollbackOnError(t *testing.T) {
	w, mock := newJournalMock(t)
	batch := JournalBatch{
		Orders: []OrderRow{
			{OrderID: 42, UserID: "u1", Symbol: "BTC-USDT", Side: 1, OrderType: 1, TIF: 1,
				Price: "100", Qty: "1", FrozenAmt: "100", Status: 1,
				CreatedAtMs: 1, UpdatedAtMs: 1, Kind: OrderRowInsert},
		},
	}
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO orders`).WillReturnError(sqlErr("dead link"))
	mock.ExpectRollback()

	if err := w.ApplyJournalBatch(context.Background(), batch); err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

// small helper for error construction without importing errors just to wrap a string
type sqlErrStr string

func (s sqlErrStr) Error() string { return string(s) }

func sqlErr(msg string) error { return sqlErrStr(msg) }
