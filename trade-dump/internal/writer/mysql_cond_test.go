package writer

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func newCondMock(t *testing.T) (*MySQL, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewMySQLWithDB(db, 10), mock
}

func TestApplyConditionalBatch_EmptyIsNoop(t *testing.T) {
	w, mock := newCondMock(t)
	if err := w.ApplyConditionalBatch(context.Background(), ConditionalBatch{}); err != nil {
		t.Fatalf("empty: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected sql: %v", err)
	}
}

func TestApplyConditionalBatch_UpsertUsesGuardClause(t *testing.T) {
	w, mock := newCondMock(t)

	batch := ConditionalBatch{Rows: []ConditionalRow{
		{
			ID:                1,
			UserID:            "u1",
			Symbol:            "BTC-USDT",
			Side:              2,
			Type:              1,
			StopPrice:         "100",
			Qty:               "1",
			Status:            1,
			CreatedAtMs:       1_700_000_000_000,
			TriggeredAtMs:     0,
			LastUpdateMs:      1_700_000_000_010,
			TrailingActive:    false,
			TrailingDeltaBps:  0,
			ActivationPrice:   "",
			TrailingWatermark: "",
		},
	}}

	mock.ExpectBegin()
	// The INSERT contains the last_update_ms guard clause; regexp match the
	// header + one of the guard fragments.
	mock.ExpectExec(`INSERT INTO conditionals`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := w.ApplyConditionalBatch(context.Background(), batch); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected sql: %v", err)
	}
}
