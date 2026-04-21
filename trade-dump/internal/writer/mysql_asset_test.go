package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestApplyAssetBatch_EmptyIsNoop(t *testing.T) {
	w, mock := newJournalMock(t)
	if err := w.ApplyAssetBatch(context.Background(), AssetBatch{}); err != nil {
		t.Fatalf("empty: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected sql: %v", err)
	}
}

func TestApplyAssetBatch_FullFlowInSingleTx(t *testing.T) {
	w, mock := newJournalMock(t)

	batch := AssetBatch{
		Accounts: []FundingAccountRow{
			{UserID: "u1", Asset: "USDT", Available: "75", Frozen: "0",
				AssetSeqID: 7, FundingVersion: 3, BalanceVersion: 4},
		},
		Logs: []FundingAccountLogRow{
			{AssetSeqID: 7, Asset: "USDT", UserID: "u1",
				DeltaAvail: "-25", DeltaFrozen: "0",
				AvailAfter: "75", FrozenAfter: "0",
				BizType: "transfer_out", BizRefID: "saga-1", PeerBiz: "spot",
				TsUnixMs: 1_700_000_000_000},
		},
		Transfers: []TransferRow{
			{TransferID: "saga-1", UserID: "u1", FromBiz: "funding", ToBiz: "spot",
				Asset: "USDT", Amount: "25", State: "DEBITED",
				AssetSeqID: 7, CreatedAtMs: 1_700_000_000_000, UpdatedAtMs: 1_700_000_000_000},
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO funding_accounts`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO funding_account_logs`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO transfers`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := w.ApplyAssetBatch(context.Background(), batch); err != nil {
		t.Fatalf("ApplyAssetBatch: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestApplyAssetBatch_RollbackOnAccountsFailure(t *testing.T) {
	w, mock := newJournalMock(t)

	batch := AssetBatch{
		Accounts: []FundingAccountRow{
			{UserID: "u1", Asset: "USDT", AssetSeqID: 1},
		},
	}
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO funding_accounts`).WillReturnError(errors.New("dup key"))
	mock.ExpectRollback()

	if err := w.ApplyAssetBatch(context.Background(), batch); err == nil {
		t.Fatal("expected error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestApplyAssetBatch_LogsOnly(t *testing.T) {
	// Accounts and transfers may be empty on SagaStateChange-only
	// batches (the rare case where we only see a state transition but
	// no balance movement for funding wallet — e.g. transfer where
	// neither side is funding). Logs-only batches should work too.
	w, mock := newJournalMock(t)

	batch := AssetBatch{
		Logs: []FundingAccountLogRow{{
			AssetSeqID: 1, Asset: "USDT", UserID: "u1",
			DeltaAvail: "1", AvailAfter: "1",
			BizType: "transfer_in", BizRefID: "saga-x",
		}},
	}
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO funding_account_logs`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := w.ApplyAssetBatch(context.Background(), batch); err != nil {
		t.Fatalf("logs only: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func TestApplyAssetBatch_TransfersOnly(t *testing.T) {
	w, mock := newJournalMock(t)

	batch := AssetBatch{
		Transfers: []TransferRow{{
			TransferID: "saga-x", UserID: "u1",
			FromBiz: "funding", ToBiz: "spot",
			Asset: "USDT", Amount: "10", State: "INIT",
			AssetSeqID: 1, CreatedAtMs: 1, UpdatedAtMs: 1,
		}},
	}
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO transfers`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := w.ApplyAssetBatch(context.Background(), batch); err != nil {
		t.Fatalf("transfers only: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}
