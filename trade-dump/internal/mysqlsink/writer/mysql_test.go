package writer

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func newMockWriter(t *testing.T, chunkSize int) (*MySQL, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewMySQLWithDB(db, chunkSize), mock
}

func sampleRow(seqID uint64) TradeRow {
	return TradeRow{
		TradeID:      "BTC-USDT:" + itoa(seqID),
		Symbol:       "BTC-USDT",
		Price:        "42000",
		Qty:          "0.5",
		MakerUserID:  "maker",
		MakerOrderID: 10 + seqID,
		TakerUserID:  "taker",
		TakerOrderID: 20 + seqID,
		TakerSide:    1,
		TS:           int64(1_700_000_000_000 + seqID),
		MatchSeqID:   seqID,
	}
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestInsertTrades_EmptyIsNoop(t *testing.T) {
	w, mock := newMockWriter(t, 0)
	if err := w.InsertTrades(context.Background(), nil); err != nil {
		t.Fatalf("nil: %v", err)
	}
	if err := w.InsertTrades(context.Background(), []TradeRow{}); err != nil {
		t.Fatalf("empty: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected sql calls: %v", err)
	}
}

func TestInsertTrades_SingleChunk(t *testing.T) {
	w, mock := newMockWriter(t, 10)
	rows := []TradeRow{sampleRow(1), sampleRow(2)}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO trades (trade_id, symbol, price, qty, maker_user_id, maker_order_id, taker_user_id, taker_order_id, taker_side, ts, match_seq_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE trade_id = trade_id").
		WithArgs(
			"BTC-USDT:1", "BTC-USDT", "42000", "0.5", "maker", uint64(11), "taker", uint64(21), int8(1), int64(1_700_000_000_001), uint64(1),
			"BTC-USDT:2", "BTC-USDT", "42000", "0.5", "maker", uint64(12), "taker", uint64(22), int8(1), int64(1_700_000_000_002), uint64(2),
		).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	if err := w.InsertTrades(context.Background(), rows); err != nil {
		t.Fatalf("InsertTrades: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestInsertTrades_ChunksOversizedBatch(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	w := NewMySQLWithDB(db, 2)
	rows := []TradeRow{sampleRow(1), sampleRow(2), sampleRow(3)}

	twoRowValues := regexp.QuoteMeta("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	oneRowValuesTail := regexp.QuoteMeta("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") + " ON DUPLICATE"

	mock.ExpectBegin()
	mock.ExpectExec(twoRowValues).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(oneRowValuesTail).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := w.InsertTrades(context.Background(), rows); err != nil {
		t.Fatalf("InsertTrades: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestInsertTrades_RollbackOnExecError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	w := NewMySQLWithDB(db, 10)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO trades").WillReturnError(errors.New("disk full"))
	mock.ExpectRollback()

	err = w.InsertTrades(context.Background(), []TradeRow{sampleRow(1)})
	if err == nil {
		t.Fatalf("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestInsertTrades_RollbackOnCommitError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	w := NewMySQLWithDB(db, 10)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO trades").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("lost connection"))
	// No ExpectRollback: database/sql marks the Tx done after a failed
	// Commit, so the deferred Rollback returns sql.ErrTxDone without
	// hitting the driver.

	err = w.InsertTrades(context.Background(), []TradeRow{sampleRow(1)})
	if err == nil {
		t.Fatalf("expected error on commit")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}
