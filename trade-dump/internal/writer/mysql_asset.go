package writer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ApplyAssetBatch writes funding_accounts + funding_account_logs +
// transfers in a single MySQL transaction. All three tables are
// idempotent under replay:
//
//   - funding_accounts     : INSERT ... ON DUPLICATE KEY UPDATE guarded by
//                            asset_seq_id (older events cannot regress
//                            newer balances)
//   - funding_account_logs : INSERT ... ON DUPLICATE KEY UPDATE no-op (PK
//                            is (asset_seq_id, asset))
//   - transfers            : INSERT ... ON DUPLICATE KEY UPDATE guarded by
//                            asset_seq_id; mutable columns LWW
func (m *MySQL) ApplyAssetBatch(ctx context.Context, batch AssetBatch) error {
	if batch.IsEmpty() {
		return nil
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin asset tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := m.applyFundingAccounts(ctx, tx, batch.Accounts); err != nil {
		return err
	}
	if err := m.applyFundingAccountLogs(ctx, tx, batch.Logs); err != nil {
		return err
	}
	if err := m.applyTransfers(ctx, tx, batch.Transfers); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit asset tx: %w", err)
	}
	committed = true
	return nil
}

// -----------------------------------------------------------------------------
// funding_accounts
// -----------------------------------------------------------------------------

func (m *MySQL) applyFundingAccounts(ctx context.Context, tx *sql.Tx, rows []FundingAccountRow) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += m.chunkSize {
		end := start + m.chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := upsertFundingAccountsChunk(ctx, tx, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func upsertFundingAccountsChunk(ctx context.Context, tx *sql.Tx, rows []FundingAccountRow) error {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*7)
	for i, r := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			r.UserID,
			r.Asset,
			zeroIfEmpty(r.Available),
			zeroIfEmpty(r.Frozen),
			r.AssetSeqID,
			r.FundingVersion,
			r.BalanceVersion,
		)
	}
	// asset_seq_id guard: older events must not overwrite newer state.
	// funding_version / balance_version follow the same guard so both
	// layers (ADR-0048 / ADR-0057 §6) stay monotonic in the projection.
	const updateClause = "" +
		"available       = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(available), available), " +
		"frozen          = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(frozen), frozen), " +
		"funding_version = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(funding_version), funding_version), " +
		"balance_version = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(balance_version), balance_version), " +
		"asset_seq_id    = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(asset_seq_id), asset_seq_id)"
	q := "INSERT INTO funding_accounts (user_id, asset, available, frozen, asset_seq_id, funding_version, balance_version) VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE " + updateClause
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("funding_accounts upsert: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// funding_account_logs
// -----------------------------------------------------------------------------

const fundingAccountLogCols = "asset_seq_id, asset, user_id, delta_avail, delta_frozen, avail_after, frozen_after, biz_type, biz_ref_id, peer_biz, ts"

func (m *MySQL) applyFundingAccountLogs(ctx context.Context, tx *sql.Tx, rows []FundingAccountLogRow) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += m.chunkSize {
		end := start + m.chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := insertFundingAccountLogsChunk(ctx, tx, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func insertFundingAccountLogsChunk(ctx context.Context, tx *sql.Tx, rows []FundingAccountLogRow) error {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*11)
	for i, r := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			r.AssetSeqID,
			r.Asset,
			r.UserID,
			zeroIfEmpty(r.DeltaAvail),
			zeroIfEmpty(r.DeltaFrozen),
			zeroIfEmpty(r.AvailAfter),
			zeroIfEmpty(r.FrozenAfter),
			r.BizType,
			r.BizRefID,
			r.PeerBiz,
			r.TsUnixMs,
		)
	}
	q := "INSERT INTO funding_account_logs (" + fundingAccountLogCols + ") VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE asset_seq_id = asset_seq_id"
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("funding_account_logs insert: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// transfers
// -----------------------------------------------------------------------------

const transfersInsertCols = "transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, asset_seq_id, created_at_ms, updated_at_ms"

func (m *MySQL) applyTransfers(ctx context.Context, tx *sql.Tx, rows []TransferRow) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += m.chunkSize {
		end := start + m.chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := upsertTransfersChunk(ctx, tx, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func upsertTransfersChunk(ctx context.Context, tx *sql.Tx, rows []TransferRow) error {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*11)
	for i, r := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			r.TransferID,
			r.UserID,
			r.FromBiz,
			r.ToBiz,
			r.Asset,
			zeroIfEmpty(r.Amount),
			r.State,
			r.RejectReason,
			r.AssetSeqID,
			r.CreatedAtMs,
			r.UpdatedAtMs,
		)
	}
	// LWW on asset_seq_id for every mutable column. `created_at_ms`
	// behaves as a monotonic min: the first event we see (smallest
	// asset_seq_id) sets it, later events preserve it. We emulate that
	// in SQL by only advancing created_at_ms when the stored row's
	// created_at_ms is 0 (i.e. has never been set) — since the schema
	// requires it NOT NULL and it's always set on insert, LWW here
	// would otherwise overwrite the real "INIT" timestamp with a later
	// transition's timestamp, which makes "when did the saga start"
	// inaccurate.
	const updateClause = "" +
		"user_id       = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(user_id), user_id), " +
		"from_biz      = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(from_biz), from_biz), " +
		"to_biz        = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(to_biz), to_biz), " +
		"asset         = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(asset), asset), " +
		"amount        = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(amount), amount), " +
		"state         = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(state), state), " +
		"reject_reason = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(reject_reason), reject_reason), " +
		"created_at_ms = LEAST(created_at_ms, VALUES(created_at_ms)), " +
		"updated_at_ms = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(updated_at_ms), updated_at_ms), " +
		"asset_seq_id  = IF(VALUES(asset_seq_id) >= asset_seq_id, VALUES(asset_seq_id), asset_seq_id)"
	q := "INSERT INTO transfers (" + transfersInsertCols + ") VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE " + updateClause
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("transfers upsert: %w", err)
	}
	return nil
}
