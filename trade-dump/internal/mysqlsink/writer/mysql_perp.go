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
	if err := m.upsertPerpTakeoverLots(ctx, tx, batch.TakeoverLots); err != nil {
		return err
	}
	if err := m.insertPerpADL(ctx, tx, batch.ADL); err != nil {
		return err
	}
	if err := m.insertRiskPoolSettlements(ctx, tx, batch.RiskPool); err != nil {
		return err
	}
	if err := m.insertPerpMargins(ctx, tx, batch.Margins); err != nil {
		return err
	}
	if err := m.insertPerpConfigLogs(ctx, tx, batch.ConfigLogs); err != nil {
		return err
	}
	if err := m.insertPerpMarginAdjustments(ctx, tx, batch.MarginAdjust); err != nil {
		return err
	}
	if err := m.insertPerpRiskLimits(ctx, tx, batch.RiskLimits); err != nil {
		return err
	}
	if err := m.insertPerpBreaches(ctx, tx, batch.Breaches); err != nil {
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
		args := make([]any, 0, len(rs)*13)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.UserID, r.Symbol, r.PositionIdx, r.Side, zeroIfEmpty(r.Size), zeroIfEmpty(r.EntryPrice),
				zeroIfEmpty(r.Margin), zeroIfEmpty(r.Leverage), zeroIfEmpty(r.RealizedPnl),
				r.MarginMode, r.RiskID, r.Version, r.PerpSeqID)
		}
		// perp_seq_id guard so a replay never regresses the latest snapshot.
		const upd = "" +
			"side = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(side), side), " +
			"size = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(size), size), " +
			"entry_price = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(entry_price), entry_price), " +
			"margin = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(margin), margin), " +
			"leverage = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(leverage), leverage), " +
			"realized_pnl = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(realized_pnl), realized_pnl), " +
			"margin_mode = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(margin_mode), margin_mode), " +
			"risk_id = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(risk_id), risk_id), " +
			"version = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(version), version), " +
			"perp_seq_id = IF(VALUES(perp_seq_id) >= perp_seq_id, VALUES(perp_seq_id), perp_seq_id)"
		q := "INSERT INTO perp_positions (user_id, symbol, position_idx, side, size, entry_price, margin, leverage, realized_pnl, margin_mode, risk_id, version, perp_seq_id) VALUES " +
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
	const q = "INSERT INTO perp_orders (order_id, user_id, symbol, status, filled_qty, reduce_only, position_idx, reject_reason, updated_at) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE " +
		"status = VALUES(status), filled_qty = VALUES(filled_qty), reject_reason = VALUES(reject_reason), updated_at = VALUES(updated_at)"
	_, err := tx.ExecContext(ctx, q, r.OrderID, r.UserID, r.Symbol, r.Status, zeroIfEmpty(r.FilledQty),
		r.ReduceOnly, r.PositionIdx, r.RejectReason, msToSQL(r.UpdatedAtMs))
	if err != nil {
		return fmt.Errorf("perp_orders upsert %d: %w", r.OrderID, err)
	}
	return nil
}

func (m *MySQL) insertPerpSettlements(ctx context.Context, tx *sql.Tx, rows []PerpSettlementRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpSettlementRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*14)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.OrderID, r.TradeID, r.Symbol, r.PositionIdx, r.FillSide,
				zeroIfEmpty(r.Price), zeroIfEmpty(r.Qty), zeroIfEmpty(r.RealizedPnl), zeroIfEmpty(r.Fee),
				zeroIfEmpty(r.MarginAdded), zeroIfEmpty(r.MarginReleased), r.TsUnixMs)
		}
		q := "INSERT INTO perp_settlements (perp_seq_id, user_id, order_id, trade_id, symbol, position_idx, fill_side, price, qty, realized_pnl, fee, margin_added, margin_released, ts_unix_ms) VALUES " +
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
		args := make([]any, 0, len(rs)*9)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, r.PositionIdx, r.FundingRoundID,
				zeroIfEmpty(r.FundingRate), zeroIfEmpty(r.MarkPrice), zeroIfEmpty(r.Payment), r.TsUnixMs)
		}
		q := "INSERT INTO perp_funding (perp_seq_id, user_id, symbol, position_idx, funding_round_id, funding_rate, mark_price, payment, ts_unix_ms) VALUES " +
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
		args := make([]any, 0, len(rs)*12)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, r.PositionIdx, r.LiqOrderID, zeroIfEmpty(r.BankruptcyPrice),
				zeroIfEmpty(r.MarkPrice), zeroIfEmpty(r.ClosedQty), zeroIfEmpty(r.RealizedPnl),
				zeroIfEmpty(r.InsuranceDelta), r.AdlQueued, r.TsUnixMs)
		}
		q := "INSERT INTO perp_liquidations (perp_seq_id, user_id, symbol, position_idx, liq_order_id, bankruptcy_price, mark_price, closed_qty, realized_pnl, insurance_delta, adl_queued, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE perp_seq_id = perp_seq_id"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_liquidations insert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) upsertPerpTakeoverLots(ctx context.Context, tx *sql.Tx, rows []PerpTakeoverLotRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpTakeoverLotRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*14)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.LotID, r.PerpSeqID, r.UserID, r.Symbol, r.PositionIdx, r.Side,
				zeroIfEmpty(r.TotalQty), zeroIfEmpty(r.LeavesQty), zeroIfEmpty(r.TakeoverPrice),
				zeroIfEmpty(r.TriggerMarkPrice), zeroIfEmpty(r.TakenOverBalance),
				r.WorkingCapitalRef, r.Status, r.TsUnixMs)
		}
		q := "INSERT INTO perp_takeover_lots (lot_id, perp_seq_id, user_id, symbol, position_idx, side, total_qty, leaves_qty, takeover_price, trigger_mark_price, taken_over_balance, working_capital_ref, status, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE perp_seq_id = perp_seq_id"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_takeover_lots upsert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) insertPerpADL(ctx context.Context, tx *sql.Tx, rows []PerpADLRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpADLRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*11)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, r.PositionIdx, r.LotID, r.AdlRound,
				zeroIfEmpty(r.Price), zeroIfEmpty(r.RequestedQty), zeroIfEmpty(r.FactQty),
				zeroIfEmpty(r.RealizedPnl), r.TsUnixMs)
		}
		q := "INSERT INTO perp_adl_events (perp_seq_id, user_id, symbol, position_idx, lot_id, adl_round, price, requested_qty, fact_qty, realized_pnl, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE perp_seq_id = perp_seq_id"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_adl_events insert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) insertRiskPoolSettlements(ctx context.Context, tx *sql.Tx, rows []RiskPoolSettlementRow) error {
	return chunk(rows, m.chunkSize, func(rs []RiskPoolSettlementRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*13)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.LotID, r.PerpSeqID, r.Symbol, r.Coin, r.WorkingCapitalRef,
				zeroIfEmpty(r.TakenOverBalance), zeroIfEmpty(r.LiqAdlRealisedPnl), zeroIfEmpty(r.CumFee),
				zeroIfEmpty(r.WorkingCapitalDrawn), zeroIfEmpty(r.BorrowedBalance),
				zeroIfEmpty(r.FinalPoolDelta), r.Status, r.TsUnixMs)
		}
		q := "INSERT INTO perp_risk_pool_settlements (lot_id, perp_seq_id, symbol, coin, working_capital_ref, taken_over_balance, liq_adl_realised_pnl, cum_fee, working_capital_drawn, borrowed_balance, final_pool_delta, status, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ") + " ON DUPLICATE KEY UPDATE lot_id = lot_id"
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_risk_pool_settlements insert: %w", err)
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

func (m *MySQL) insertPerpConfigLogs(ctx context.Context, tx *sql.Tx, rows []PerpPositionConfigLogRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpPositionConfigLogRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*14)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, r.MarginMode, r.PositionMode, r.PositionIdx,
				zeroIfEmpty(r.Leverage),
				r.RiskID, r.AutoAddMargin, zeroIfEmpty(r.AutoAddMax), r.PositionVersion,
				r.Reason, r.ClientOpID, r.TsUnixMs)
		}
		q := "INSERT IGNORE INTO perp_position_config_logs (perp_seq_id, user_id, symbol, margin_mode, position_mode, position_idx, leverage, risk_id, auto_add_margin, auto_add_max, position_version, reason, client_op_id, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ")
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_position_config_logs insert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) insertPerpMarginAdjustments(ctx context.Context, tx *sql.Tx, rows []PerpMarginAdjustmentRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpMarginAdjustmentRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*13)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, r.PositionIdx, r.Kind, zeroIfEmpty(r.Amount),
				zeroIfEmpty(r.MarginBefore), zeroIfEmpty(r.MarginAfter), zeroIfEmpty(r.WalletAfter),
				r.PositionVersion, r.ClientOpID, zeroIfEmpty(r.MarkPrice), r.TsUnixMs)
		}
		q := "INSERT IGNORE INTO perp_margin_adjustments (perp_seq_id, user_id, symbol, position_idx, kind, amount, margin_before, margin_after, wallet_after, position_version, client_op_id, mark_price, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ")
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_margin_adjustments insert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) insertPerpRiskLimits(ctx context.Context, tx *sql.Tx, rows []PerpCustomerRiskLimitRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpCustomerRiskLimitRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*7)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, zeroIfEmpty(r.MaxLeverage),
				r.Reason, r.UpdatedBy, r.TsUnixMs)
		}
		q := "INSERT IGNORE INTO perp_customer_risk_limits (perp_seq_id, user_id, symbol, max_leverage, reason, updated_by, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ")
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_customer_risk_limits insert: %w", err)
		}
		return nil
	})
}

func (m *MySQL) insertPerpBreaches(ctx context.Context, tx *sql.Tx, rows []PerpInvariantBreachRow) error {
	return chunk(rows, m.chunkSize, func(rs []PerpInvariantBreachRow) error {
		ph := make([]string, len(rs))
		args := make([]any, 0, len(rs)*10)
		for i, r := range rs {
			ph[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			args = append(args, r.PerpSeqID, r.UserID, r.Symbol, r.PositionIdx, r.OrderID, r.TradeID,
				r.Kind, zeroIfEmpty(r.ExcessQty), zeroIfEmpty(r.FillPrice), r.TsUnixMs)
		}
		q := "INSERT IGNORE INTO perp_invariant_breaches (perp_seq_id, user_id, symbol, position_idx, order_id, trade_id, kind, excess_qty, fill_price, ts_unix_ms) VALUES " +
			strings.Join(ph, ", ")
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("perp_invariant_breaches insert: %w", err)
		}
		return nil
	})
}
