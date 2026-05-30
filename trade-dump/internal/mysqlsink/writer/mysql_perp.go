package writer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ApplyPerpBatch writes the perp projection (positions / wallets / orders +
// the append-only settlement / funding / liquidation / margin ledgers) in one
// MySQL transaction (ADR-0068 M7). All statements are replay-idempotent:
// state tables upsert latest-wins guarded by perp_seq_id; append ledgers
// INSERT ... ON DUPLICATE KEY UPDATE no-op on their (user_id, perp_seq_id) PK.
func (m *MySQL) ApplyPerpBatch(ctx context.Context, batch PerpBatch) error {
	if batch.IsEmpty() {
		return nil
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin perp tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := m.upsertPerpPositions(ctx, tx, batch.Positions); err != nil {
		return err
	}
	if err := m.upsertPerpWallets(ctx, tx, batch.Wallets); err != nil {
		return err
	}
	for _, o := range batch.Orders {
		if err := upsertPerpOrder(ctx, tx, o); err != nil {
			return err
		}
	}
	if err := m.insertPerpSettlements(ctx, tx, batch.Settlements); err != nil {
		return err
	}
	if err := m.insertPerpFunding(ctx, tx, batch.Funding); err != nil {
		return err
	}
	if err := m.insertPerpLiquidations(ctx, tx, batch.Liquidations); err != nil {
		return err
	}
	if err := m.insertPerpMargins(ctx, tx, batch.Margins); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit perp tx: %w", err)
	}
	committed = true
	return nil
}

// chunk runs fn over rows in m.chunkSize-sized slices.
func chunk[T any](rows []T, size int, fn func([]T) error) error {
	if size <= 0 {
		size = len(rows)
	}
	for start := 0; start < len(rows); start += size {
		end := start + size
		if end > len(rows) {
			end = len(rows)
		}
		if err := fn(rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (m *MySQL) upsertPerpPositions(ctx context.Context, tx *sql.Tx, rows []PerpPositionRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpPositionRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*10)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.UserID, r.Symbol, r.Side, zeroIfEmpty(r.Size), zeroIfEmpty(r.EntryPrice),
				zeroIfEmpty(r.Margin), zeroIfEmpty(r.Leverage), zeroIfEmpty(r.RealizedPnl), r.Version, r.PerpSeqID)
		}
		// perp_seq_id guard so a replay never regresses the latest snapshot.
		const upd = "" +
			"side = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(side), side), " +
			"size = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(size), size), " +
			"entry_price = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(entry_price), entry_price), " +
			"margin = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(margin), margin), " +
			"leverage = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(leverage), leverage), " +
			"realized_pnl = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(realized_pnl), realized_pnl), " +
			"version = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(version), version), " +
			"perp_seq_id = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(perp_seq_id), perp_seq_id)"
		q := "INSERT INTO perp_positions (user_id, symbol, side, size, entry_price, margin, leverage, realized_pnl, version, perp_seq_id) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE " + upd
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_positions upsert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) upsertPerpWallets(ctx context.Context, tx *sql.Tx, rows []PerpWalletRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpWalletRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*5)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?)"
			args = append(args, r.UserID, r.Asset, zeroIfEmpty(r.Available), zeroIfEmpty(r.Reserved), r.PerpSeqID)
		}
		const upd = "" +
			"available = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(available), available), " +
			"reserved = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(reserved), reserved), " +
			"perp_seq_id = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(perp_seq_id), perp_seq_id)"
		q := "INSERT INTO perp_wallets (user_id, asset, available, reserved, perp_seq_id) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE " + upd
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_wallets upsert: %w", err)
		}
		return nil
	})
}

func upsertPerpOrder(ctx context.Context, tx *sql.Tx, r PerpOrderRow) error {
	const q = "INSERT INTO perp_orders (order_id, user_id, symbol, status, filled_qty, reduce_only, reject_reason, updated_at) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE " +
		"status = VALUES(status), filled_qty = VALUES(filled_qty), reject_reason = VALUES(reject_reason), updated_at = VALUES(updated_at)"
	_, err := tx.ExecContext(ctx, q, r.OrderID, r.UserID, r.Symbol, r.Status, zeroIfEmpty(r.FilledQty),
		r.ReduceOnly, r.RejectReason, msToSQL(r.UpdatedAtMs))
	if err != nil {
		return fmt.Errorf("perp_orders upsert %d: %w", r.OrderID, err)
	}
	return nil
}

func (m *MySQL) insertPerpSettlements(ctx context.Context, tx *sql.Tx, rows []PerpSettlementRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpSettlementRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*13)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.OrderID, r.TradeID, r.Symbol, r.FillSide,
				zeroIfEmpty(r.Price), zeroIfEmpty(r.Qty), zeroIfEmpty(r.RealizedPnl), zeroIfEmpty(r.Fee),
				zeroIfEmpty(r.MarginAdded), zeroIfEmpty(r.MarginReleased), r.TsUnixMs)
		}
		q := "INSERT INTO perp_settlements (perp_seq_id, user_id, order_id, trade_id, symbol, fill_side, price, qty, realized_pnl, fee, margin_added, margin_released, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE perp_seq_id = perp_seq_id"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_settlements insert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) insertPerpFunding(ctx context.Context, tx *sql.Tx, rows []PerpFundingRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpFundingRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*8)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, r.FundingRoundID,
				zeroIfEmpty(r.FundingRate), zeroIfEmpty(r.MarkPrice), zeroIfEmpty(r.Payment), r.TsUnixMs)
		}
		q := "INSERT INTO perp_funding (perp_seq_id, user_id, symbol, funding_round_id, funding_rate, mark_price, payment, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE perp_seq_id = perp_seq_id"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_funding insert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) insertPerpLiquidations(ctx context.Context, tx *sql.Tx, rows []PerpLiquidationRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpLiquidationRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*11)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, r.LiqOrderID, zeroIfEmpty(r.BankruptcyPrice),
				zeroIfEmpty(r.MarkPrice), zeroIfEmpty(r.ClosedQty), zeroIfEmpty(r.RealizedPnl),
				zeroIfEmpty(r.InsuranceDelta), r.AdlQueued, r.TsUnixMs)
		}
		q := "INSERT INTO perp_liquidations (perp_seq_id, user_id, symbol, liq_order_id, bankruptcy_price, mark_price, closed_qty, realized_pnl, insurance_delta, adl_queued, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE perp_seq_id = perp_seq_id"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_liquidations insert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) insertPerpMargins(ctx context.Context, tx *sql.Tx, rows []PerpMarginRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpMarginRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*9)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Kind, r.Asset, zeroIfEmpty(r.Amount),
				zeroIfEmpty(r.AvailableAfter), zeroIfEmpty(r.ReservedAfter), r.RefID, r.TsUnixMs)
		}
		q := "INSERT INTO perp_margin_logs (perp_seq_id, user_id, kind, asset, amount, available_after, reserved_after, ref_id, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE perp_seq_id = perp_seq_id"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_margin_logs insert: %w", err)
		}
		return nil
	})
}
