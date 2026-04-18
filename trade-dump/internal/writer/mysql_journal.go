package writer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ApplyJournalBatch writes orders + accounts + account_logs in a single
// MySQL transaction. All three tables are idempotent under replay:
//
//   - orders    : INSERT ... ON DUPLICATE KEY UPDATE order_id=order_id
//                 (freeze row re-insert is no-op; status updates are separate
//                  UPDATE statements applied in event order)
//   - accounts  : INSERT ... ON DUPLICATE KEY UPDATE ...
//                 guarded by seq_id (replays with a smaller seq are ignored)
//   - account_logs : INSERT ... ON DUPLICATE KEY UPDATE shard_id=shard_id
//                    (PK is (shard_id, seq_id, asset))
func (m *MySQL) ApplyJournalBatch(ctx context.Context, batch JournalBatch) error {
	if batch.IsEmpty() {
		return nil
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin journal tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := m.applyOrders(ctx, tx, batch.Orders); err != nil {
		return err
	}
	if err := m.applyAccounts(ctx, tx, batch.Accounts); err != nil {
		return err
	}
	if err := m.applyAccountLogs(ctx, tx, batch.AccountLogs); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit journal tx: %w", err)
	}
	committed = true
	return nil
}

// -----------------------------------------------------------------------------
// orders
// -----------------------------------------------------------------------------

const ordersInsertCols = "order_id, client_order_id, user_id, symbol, side, order_type, tif, price, qty, filled_qty, frozen_amt, status, reject_reason, created_at, updated_at"

func (m *MySQL) applyOrders(ctx context.Context, tx *sql.Tx, rows []OrderRow) error {
	if len(rows) == 0 {
		return nil
	}
	// Split into INSERTs (FreezeEvent) and UPDATEs (OrderStatusEvent). Each
	// batched separately — INSERTs can ride a multi-row ON DUPLICATE KEY
	// statement; UPDATEs are issued one per row because MySQL has no
	// equivalent to bulk-UPDATE.
	var inserts, updates []OrderRow
	for _, r := range rows {
		switch r.Kind {
		case OrderRowInsert:
			inserts = append(inserts, r)
		case OrderRowUpdate:
			updates = append(updates, r)
		}
	}

	for start := 0; start < len(inserts); start += m.chunkSize {
		end := start + m.chunkSize
		if end > len(inserts) {
			end = len(inserts)
		}
		if err := insertOrdersChunk(ctx, tx, inserts[start:end]); err != nil {
			return err
		}
	}
	for _, r := range updates {
		if err := updateOrderRow(ctx, tx, r); err != nil {
			return err
		}
	}
	return nil
}

func insertOrdersChunk(ctx context.Context, tx *sql.Tx, rows []OrderRow) error {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*15)
	for i, r := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			r.OrderID,
			r.ClientOrderID,
			r.UserID,
			r.Symbol,
			r.Side,
			r.OrderType,
			r.TIF,
			zeroIfEmpty(r.Price),
			zeroIfEmpty(r.Qty),
			zeroIfEmpty(r.FilledQty),
			zeroIfEmpty(r.FrozenAmt),
			r.Status,
			r.RejectReason,
			msToSQL(r.CreatedAtMs),
			msToSQL(r.UpdatedAtMs),
		)
	}
	q := "INSERT INTO orders (" + ordersInsertCols + ") VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE order_id = order_id"
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("orders insert: %w", err)
	}
	return nil
}

func updateOrderRow(ctx context.Context, tx *sql.Tx, r OrderRow) error {
	// Single-row UPDATE. MVP expects counter-journal to be consumed in
	// order (shard-monotonic, single consumer goroutine), so the latest
	// UPDATE wins by natural order — no seq_id guard needed here.
	const q = "UPDATE orders SET status = ?, filled_qty = ?, reject_reason = ?, updated_at = ? WHERE order_id = ?"
	_, err := tx.ExecContext(ctx, q,
		r.Status,
		zeroIfEmpty(r.FilledQty),
		r.RejectReason,
		msToSQL(r.UpdatedAtMs),
		r.OrderID,
	)
	if err != nil {
		return fmt.Errorf("orders update %d: %w", r.OrderID, err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// accounts
// -----------------------------------------------------------------------------

func (m *MySQL) applyAccounts(ctx context.Context, tx *sql.Tx, rows []AccountRow) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += m.chunkSize {
		end := start + m.chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := upsertAccountsChunk(ctx, tx, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func upsertAccountsChunk(ctx context.Context, tx *sql.Tx, rows []AccountRow) error {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*7)
	for i, r := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			r.UserID,
			r.Asset,
			zeroIfEmpty(r.Available),
			zeroIfEmpty(r.Frozen),
			r.SeqID,
			r.AccountVersion,
			r.BalanceVersion,
		)
	}
	// seq_id guard: older events must not overwrite newer state on replay.
	// account_version / balance_version follow the same guard so the
	// double-layer counters (ADR-0048 backlog 方案 B) stay monotonic in
	// the projection.
	const updateClause = "" +
		"available       = IF(VALUES(seq_id) >= seq_id, VALUES(available), available), " +
		"frozen          = IF(VALUES(seq_id) >= seq_id, VALUES(frozen), frozen), " +
		"account_version = IF(VALUES(seq_id) >= seq_id, VALUES(account_version), account_version), " +
		"balance_version = IF(VALUES(seq_id) >= seq_id, VALUES(balance_version), balance_version), " +
		"seq_id          = IF(VALUES(seq_id) >= seq_id, VALUES(seq_id), seq_id)"
	q := "INSERT INTO accounts (user_id, asset, available, frozen, seq_id, account_version, balance_version) VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE " + updateClause
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("accounts upsert: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// account_logs
// -----------------------------------------------------------------------------

const accountLogCols = "shard_id, seq_id, asset, user_id, delta_avail, delta_frozen, avail_after, frozen_after, biz_type, biz_ref_id, ts"

func (m *MySQL) applyAccountLogs(ctx context.Context, tx *sql.Tx, rows []AccountLogRow) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += m.chunkSize {
		end := start + m.chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := insertAccountLogsChunk(ctx, tx, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func insertAccountLogsChunk(ctx context.Context, tx *sql.Tx, rows []AccountLogRow) error {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*11)
	for i, r := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			r.ShardID,
			r.SeqID,
			r.Asset,
			r.UserID,
			zeroIfEmpty(r.DeltaAvail),
			zeroIfEmpty(r.DeltaFrozen),
			zeroIfEmpty(r.AvailAfter),
			zeroIfEmpty(r.FrozenAfter),
			r.BizType,
			r.BizRefID,
			r.TsUnixMs,
		)
	}
	q := "INSERT INTO account_logs (" + accountLogCols + ") VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE shard_id = shard_id"
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("account_logs insert: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// formatters
// -----------------------------------------------------------------------------

// msToSQL formats a ms-since-epoch timestamp as a MySQL DATETIME(3) string.
// UTC to match the schema default (no TZ conversion at read time).
func msToSQL(ms int64) string {
	if ms <= 0 {
		ms = time.Now().UnixMilli()
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05.000")
}

func zeroIfEmpty(s string) string {
	if s == "" {
		return "0"
	}
	return s
}
