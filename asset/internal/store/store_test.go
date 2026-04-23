package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

const (
	insertFundingUserQ = `INSERT IGNORE INTO funding_users (user_id, funding_version, updated_at_ms)
		VALUES (?, ?, ?)`
	selectFundingUserQ    = `SELECT funding_version FROM funding_users WHERE user_id = ? FOR UPDATE`
	insertFundingAccountQ = `INSERT IGNORE INTO funding_accounts (user_id, asset, available, frozen, balance_version, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?)`
	selectFundingAccountQ = `SELECT available, frozen, balance_version FROM funding_accounts
		WHERE user_id = ? AND asset = ? FOR UPDATE`
	updateFundingAccountQ = `UPDATE funding_accounts
		SET available = ?, frozen = ?, balance_version = ?, updated_at_ms = ?
		WHERE user_id = ? AND asset = ?`
	updateFundingUserQ = `UPDATE funding_users
		SET funding_version = ?, updated_at_ms = ?
		WHERE user_id = ?`
	selectSingleBalanceQ = `SELECT available, frozen, balance_version
			FROM funding_accounts WHERE user_id = ? AND asset = ? LIMIT 1`
)

func newMockStore(t *testing.T) (*Store, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	st := NewWithDB(db)
	st.SetClock(func() time.Time { return time.UnixMilli(1234) })
	t.Cleanup(func() { _ = db.Close() })
	return st, mock, db
}

func noMutationRows() *sqlmock.Rows {
	return sqlmock.NewRows(mutationCols())
}

func mutationCols() []string {
	return []string{
		"transfer_id", "op_type", "user_id", "asset", "amount", "peer_biz",
		"memo", "status", "reject_reason", "available_after", "frozen_after",
		"funding_version", "balance_version", "created_at_ms",
	}
}

func expectLockRows(mock sqlmock.Sqlmock, userVersion uint64, available string, frozen string, balanceVersion uint64) {
	mock.ExpectExec(insertFundingUserQ).
		WithArgs("u1", uint64(0), int64(1234)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(selectFundingUserQ).
		WithArgs("u1").
		WillReturnRows(sqlmock.NewRows([]string{"funding_version"}).AddRow(userVersion))
	mock.ExpectExec(insertFundingAccountQ).
		WithArgs("u1", "USDT", dec.Zero.String(), dec.Zero.String(), uint64(0), int64(1234)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(selectFundingAccountQ).
		WithArgs("u1", "USDT").
		WillReturnRows(sqlmock.NewRows([]string{"available", "frozen", "balance_version"}).AddRow(available, frozen, balanceVersion))
}

func TestTransferInConfirmed(t *testing.T) {
	st, mock, _ := newMockStore(t)

	mock.ExpectBegin()
	mock.ExpectQuery(selectMutationQ).
		WithArgs("tx-1", string(OpTransferIn)).
		WillReturnRows(noMutationRows())
	expectLockRows(mock, 0, "0", "0", 0)
	mock.ExpectExec(updateFundingAccountQ).
		WithArgs("100", "0", uint64(1), int64(1234), "u1", "USDT").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(updateFundingUserQ).
		WithArgs(uint64(1), int64(1234), "u1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(insertMutationQ).
		WithArgs("tx-1", string(OpTransferIn), "u1", "USDT", "100",
			"spot", "seed", mutationConfirmed, "", "100", "0", uint64(1), uint64(1), int64(1234)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	res, err := st.TransferIn(context.Background(), Request{
		UserID: "u1", TransferID: "tx-1", Asset: "USDT",
		Amount: dec.MustParse("100"), PeerBiz: "spot", Memo: "seed",
	})
	if err != nil {
		t.Fatalf("TransferIn: %v", err)
	}
	if res.Status != StatusConfirmed {
		t.Fatalf("status = %v", res.Status)
	}
	if res.BalanceAfter.Available.String() != "100" || res.FundingVersion != 1 {
		t.Fatalf("result = %+v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestTransferInDuplicateReturnsStoredResult(t *testing.T) {
	st, mock, _ := newMockStore(t)

	mock.ExpectBegin()
	mock.ExpectQuery(selectMutationQ).
		WithArgs("tx-1", string(OpTransferIn)).
		WillReturnRows(sqlmock.NewRows(mutationCols()).AddRow(
			"tx-1", string(OpTransferIn), "u1", "USDT", "100.000000000000000000",
			"spot", "seed", mutationConfirmed, "",
			"100.000000000000000000", "0.000000000000000000",
			uint64(1), uint64(1), int64(1234),
		))
	mock.ExpectCommit()

	res, err := st.TransferIn(context.Background(), Request{
		UserID: "u1", TransferID: "tx-1", Asset: "USDT",
		Amount: dec.MustParse("100"), PeerBiz: "spot", Memo: "seed",
	})
	if err != nil {
		t.Fatalf("TransferIn duplicate: %v", err)
	}
	if res.Status != StatusDuplicated {
		t.Fatalf("status = %v, want duplicated", res.Status)
	}
	if res.BalanceAfter.Available.String() != "100" {
		t.Fatalf("available = %s", res.BalanceAfter.Available)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestTransferOutInsufficientPersistsRejectedMutation(t *testing.T) {
	st, mock, _ := newMockStore(t)

	mock.ExpectBegin()
	mock.ExpectQuery(selectMutationQ).
		WithArgs("tx-out", string(OpTransferOut)).
		WillReturnRows(noMutationRows())
	expectLockRows(mock, 4, "10", "0", 2)
	mock.ExpectExec(insertMutationQ).
		WithArgs("tx-out", string(OpTransferOut), "u1", "USDT", "100",
			"spot", "", mutationRejected, engine.ErrInsufficientAvailable.Error(),
			"10", "0", uint64(4), uint64(2), int64(1234)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	res, err := st.TransferOut(context.Background(), Request{
		UserID: "u1", TransferID: "tx-out", Asset: "USDT",
		Amount: dec.MustParse("100"), PeerBiz: "spot",
	})
	if err != nil {
		t.Fatalf("TransferOut: %v", err)
	}
	if res.Status != StatusRejected || !errors.Is(res.RejectReason, engine.ErrInsufficientAvailable) {
		t.Fatalf("result = %+v", res)
	}
	if res.BalanceAfter.Available.String() != "10" || res.FundingVersion != 4 || res.BalanceAfter.Version != 2 {
		t.Fatalf("balance/version = %+v fv=%d", res.BalanceAfter, res.FundingVersion)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestIdempotencyConflict(t *testing.T) {
	st, mock, _ := newMockStore(t)

	mock.ExpectBegin()
	mock.ExpectQuery(selectMutationQ).
		WithArgs("tx-1", string(OpTransferIn)).
		WillReturnRows(sqlmock.NewRows(mutationCols()).AddRow(
			"tx-1", string(OpTransferIn), "u1", "USDT", "50",
			"spot", "seed", mutationConfirmed, "", "50", "0",
			uint64(1), uint64(1), int64(1234),
		))
	mock.ExpectRollback()

	_, err := st.TransferIn(context.Background(), Request{
		UserID: "u1", TransferID: "tx-1", Asset: "USDT",
		Amount: dec.MustParse("100"), PeerBiz: "spot", Memo: "seed",
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want idempotency conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestQuerySingleMissingReturnsZeroBalance(t *testing.T) {
	st, mock, _ := newMockStore(t)

	mock.ExpectQuery(selectSingleBalanceQ).
		WithArgs("u1", "USDT").
		WillReturnError(sql.ErrNoRows)

	out, err := st.QueryFundingBalance(context.Background(), "u1", "USDT")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(out) != 1 || out[0].Asset != "USDT" || !out[0].Balance.Available.IsZero() {
		t.Fatalf("out = %+v", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
