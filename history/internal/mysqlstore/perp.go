package mysqlstore

// perp.go is HistoryService's read path over trade-dump's perp_* projection
// (ADR-0068 M7). Positions are current state (small, unpaged); funding and
// liquidations are append-only ledgers paged newest-first by (ts, perp_seq_id).

import (
	"context"
	"strings"
	"time"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/cursor"
)

// PerpLedgerFilter is the shared filter for the perp funding / liquidation
// ledgers.
type PerpLedgerFilter struct {
	UserID  uint64
	Symbol  string
	SinceMs int64
	UntilMs int64
}

// ListPerpPositions returns a user's current positions (state, not paged),
// ordered by symbol.
func (s *Store) ListPerpPositions(ctx context.Context, userID uint64, symbol string) ([]*historypb.PerpPosition, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	conds := []string{"user_id = ?"}
	args := []any{userID}
	if symbol != "" {
		conds = append(conds, "symbol = ?")
		args = append(args, symbol)
	}
	q := `
		SELECT user_id, symbol, position_idx, side, CAST(size AS CHAR), CAST(entry_price AS CHAR),
		       CAST(margin AS CHAR), CAST(leverage AS CHAR), CAST(realized_pnl AS CHAR),
		       CAST(UNIX_TIMESTAMP(updated_at) * 1000 AS SIGNED)
		FROM perp_positions
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY symbol ASC, position_idx ASC`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*historypb.PerpPosition
	for rows.Next() {
		var p historypb.PerpPosition
		var side int8
		if err := rows.Scan(&p.UserId, &p.Symbol, &p.PositionIdx, &side, &p.Size, &p.EntryPrice,
			&p.Margin, &p.Leverage, &p.RealizedPnl, &p.UpdatedAtUnixMs); err != nil {
			return nil, err
		}
		p.Side = sideFromInt(side)
		out = append(out, &p)
	}
	return out, rows.Err()
}

// ListPerpFunding pages a user's funding payments, newest first.
func (s *Store) ListPerpFunding(ctx context.Context, f PerpLedgerFilter, rawCursor string, limit int) ([]*historypb.PerpFunding, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	limit = clampLimit(limit)

	conds, args, err := perpLedgerConds(f, rawCursor)
	if err != nil {
		return nil, "", err
	}
	q := `
		SELECT perp_seq_id, symbol, position_idx, funding_round_id, CAST(funding_rate AS CHAR),
		       CAST(mark_price AS CHAR), CAST(payment AS CHAR), ts_unix_ms
		FROM perp_funding
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY ts_unix_ms DESC, perp_seq_id DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.PerpFunding
	for rows.Next() {
		var r historypb.PerpFunding
		if err := rows.Scan(&r.PerpSeqId, &r.Symbol, &r.PositionIdx, &r.FundingRoundId, &r.FundingRate,
			&r.MarkPrice, &r.Payment, &r.TsUnixMs); err != nil {
			return nil, "", err
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		next, err = cursor.Encode(cursor.PerpLedgerCursor{Ts: last.TsUnixMs, PerpSeqID: last.PerpSeqId})
		if err != nil {
			return nil, "", err
		}
	}
	return out, next, nil
}

// ListPerpLiquidations pages a user's liquidations, newest first.
func (s *Store) ListPerpLiquidations(ctx context.Context, f PerpLedgerFilter, rawCursor string, limit int) ([]*historypb.PerpLiquidation, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	limit = clampLimit(limit)

	conds, args, err := perpLedgerConds(f, rawCursor)
	if err != nil {
		return nil, "", err
	}
	q := `
		SELECT perp_seq_id, symbol, position_idx, liq_order_id, CAST(bankruptcy_price AS CHAR),
		       CAST(mark_price AS CHAR), CAST(closed_qty AS CHAR), CAST(realized_pnl AS CHAR),
		       CAST(insurance_delta AS CHAR), adl_queued, ts_unix_ms
		FROM perp_liquidations
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY ts_unix_ms DESC, perp_seq_id DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.PerpLiquidation
	for rows.Next() {
		var r historypb.PerpLiquidation
		if err := rows.Scan(&r.PerpSeqId, &r.Symbol, &r.PositionIdx, &r.LiqOrderId, &r.BankruptcyPrice,
			&r.MarkPrice, &r.ClosedQty, &r.RealizedPnl, &r.InsuranceDelta, &r.AdlQueued, &r.TsUnixMs); err != nil {
			return nil, "", err
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		next, err = cursor.Encode(cursor.PerpLedgerCursor{Ts: last.TsUnixMs, PerpSeqID: last.PerpSeqId})
		if err != nil {
			return nil, "", err
		}
	}
	return out, next, nil
}

// ListPerpADL pages a user's ADL forced-close events, newest first.
func (s *Store) ListPerpADL(ctx context.Context, f PerpLedgerFilter, rawCursor string, limit int) ([]*historypb.PerpADL, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	limit = clampLimit(limit)

	conds, args, err := perpLedgerConds(f, rawCursor)
	if err != nil {
		return nil, "", err
	}
	q := `
		SELECT perp_seq_id, symbol, position_idx, lot_id, adl_round, CAST(price AS CHAR),
		       CAST(requested_qty AS CHAR), CAST(fact_qty AS CHAR), CAST(realized_pnl AS CHAR),
		       ts_unix_ms
		FROM perp_adl_events
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY ts_unix_ms DESC, perp_seq_id DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.PerpADL
	for rows.Next() {
		var r historypb.PerpADL
		if err := rows.Scan(&r.PerpSeqId, &r.Symbol, &r.PositionIdx, &r.LotId, &r.AdlRound, &r.Price,
			&r.RequestedQty, &r.FactQty, &r.RealizedPnl, &r.TsUnixMs); err != nil {
			return nil, "", err
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		next, err = cursor.Encode(cursor.PerpLedgerCursor{Ts: last.TsUnixMs, PerpSeqID: last.PerpSeqId})
		if err != nil {
			return nil, "", err
		}
	}
	return out, next, nil
}

// perpLedgerConds builds the shared WHERE for the perp ledgers: user scope,
// optional symbol + time window, and the keyset cursor tail.
func perpLedgerConds(f PerpLedgerFilter, rawCursor string) ([]string, []any, error) {
	var cur cursor.PerpLedgerCursor
	if err := cursor.Decode(rawCursor, &cur); err != nil {
		return nil, nil, err
	}
	conds := []string{"user_id = ?"}
	args := []any{f.UserID}
	if f.Symbol != "" {
		conds = append(conds, "symbol = ?")
		args = append(args, f.Symbol)
	}
	if f.SinceMs > 0 {
		conds = append(conds, "ts_unix_ms >= ?")
		args = append(args, f.SinceMs)
	}
	if f.UntilMs > 0 {
		conds = append(conds, "ts_unix_ms < ?")
		args = append(args, f.UntilMs)
	}
	if rawCursor != "" {
		conds = append(conds, "(ts_unix_ms < ? OR (ts_unix_ms = ? AND perp_seq_id < ?))")
		args = append(args, cur.Ts, cur.Ts, cur.PerpSeqID)
	}
	return conds, args, nil
}

// ListPerpMarginAdjustments pages a user's isolated-margin movements
// (ADR-0074 §6/§7), newest first.
func (s *Store) ListPerpMarginAdjustments(ctx context.Context, f PerpLedgerFilter, rawCursor string, limit int) ([]*historypb.PerpMarginAdjustment, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	limit = clampLimit(limit)

	conds, args, err := perpLedgerConds(f, rawCursor)
	if err != nil {
		return nil, "", err
	}
	q := `
		SELECT perp_seq_id, symbol, position_idx, kind, CAST(amount AS CHAR),
		       CAST(margin_before AS CHAR), CAST(margin_after AS CHAR),
		       CAST(wallet_after AS CHAR), position_version, client_op_id,
		       CAST(mark_price AS CHAR), ts_unix_ms
		FROM perp_margin_adjustments
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY ts_unix_ms DESC, perp_seq_id DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.PerpMarginAdjustment
	for rows.Next() {
		var r historypb.PerpMarginAdjustment
		if err := rows.Scan(&r.PerpSeqId, &r.Symbol, &r.PositionIdx, &r.Kind, &r.Amount,
			&r.MarginBefore, &r.MarginAfter, &r.WalletAfter, &r.PositionVersion,
			&r.ClientOpId, &r.MarkPrice, &r.TsUnixMs); err != nil {
			return nil, "", err
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		next, err = cursor.Encode(cursor.PerpLedgerCursor{Ts: last.TsUnixMs, PerpSeqID: last.PerpSeqId})
		if err != nil {
			return nil, "", err
		}
	}
	return out, next, nil
}

// ListPerpConfigLogs pages a user's position-config change history
// (ADR-0074 §13), newest first.
func (s *Store) ListPerpConfigLogs(ctx context.Context, f PerpLedgerFilter, rawCursor string, limit int) ([]*historypb.PerpConfigLog, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	limit = clampLimit(limit)

	conds, args, err := perpLedgerConds(f, rawCursor)
	if err != nil {
		return nil, "", err
	}
	q := `
		SELECT perp_seq_id, symbol, margin_mode, position_mode, position_idx,
		       CAST(leverage AS CHAR), risk_id,
		       auto_add_margin, CAST(auto_add_max AS CHAR), position_version,
		       reason, client_op_id, ts_unix_ms
		FROM perp_position_config_logs
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY ts_unix_ms DESC, perp_seq_id DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.PerpConfigLog
	for rows.Next() {
		var r historypb.PerpConfigLog
		if err := rows.Scan(&r.PerpSeqId, &r.Symbol, &r.MarginMode, &r.PositionMode, &r.PositionIdx,
			&r.Leverage, &r.RiskId,
			&r.AutoAddMargin, &r.AutoAddMax, &r.PositionVersion,
			&r.Reason, &r.ClientOpId, &r.TsUnixMs); err != nil {
			return nil, "", err
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		next, err = cursor.Encode(cursor.PerpLedgerCursor{Ts: last.TsUnixMs, PerpSeqID: last.PerpSeqId})
		if err != nil {
			return nil, "", err
		}
	}
	return out, next, nil
}

// ListPerpSettlements pages a user's fills with their ADR-0079 fee
// attribution (perp_settlements doubles as the trade-fee ledger), newest
// first.
func (s *Store) ListPerpSettlements(ctx context.Context, f PerpLedgerFilter, rawCursor string, limit int) ([]*historypb.PerpSettlement, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	limit = clampLimit(limit)

	conds, args, err := perpLedgerConds(f, rawCursor)
	if err != nil {
		return nil, "", err
	}
	q := `
		SELECT perp_seq_id, order_id, trade_id, symbol, position_idx, fill_side,
		       CAST(price AS CHAR), CAST(qty AS CHAR), CAST(realized_pnl AS CHAR),
		       CAST(fee AS CHAR), CAST(margin_added AS CHAR), CAST(margin_released AS CHAR),
		       liquidity_role, fee_rule_id, CAST(fee_rate AS CHAR), fee_asset,
		       CAST(fee_deficit AS CHAR), rebate_suppressed, ts_unix_ms
		FROM perp_settlements
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY ts_unix_ms DESC, perp_seq_id DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.PerpSettlement
	for rows.Next() {
		var r historypb.PerpSettlement
		if err := rows.Scan(&r.PerpSeqId, &r.OrderId, &r.TradeId, &r.Symbol, &r.PositionIdx, &r.FillSide,
			&r.Price, &r.Qty, &r.RealizedPnl,
			&r.Fee, &r.MarginAdded, &r.MarginReleased,
			&r.LiquidityRole, &r.FeeRuleId, &r.FeeRate, &r.FeeAsset,
			&r.FeeDeficit, &r.RebateSuppressed, &r.TsUnixMs); err != nil {
			return nil, "", err
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		next, err = cursor.Encode(cursor.PerpLedgerCursor{Ts: last.TsUnixMs, PerpSeqID: last.PerpSeqId})
		if err != nil {
			return nil, "", err
		}
	}
	return out, next, nil
}

// ListPerpDailyStats pages a user's ADR-0079 §6 daily aggregates, newest
// date first. The three daily tables are sparse (a day may have trades but
// no funding, or funding but no trades), so the key set is the UNION of all
// three, LEFT JOINed back for the values — absent rows read as zeros.
func (s *Store) ListPerpDailyStats(ctx context.Context, f PerpLedgerFilter, rawCursor string, limit int) ([]*historypb.PerpDailyStat, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	limit = clampLimit(limit)

	var cur cursor.PerpDailyStatCursor
	if err := cursor.Decode(rawCursor, &cur); err != nil {
		return nil, "", err
	}

	// Per-table key conditions (user scope + optional symbol + date window).
	conds := []string{"user_id = ?"}
	args := []any{f.UserID}
	if f.Symbol != "" {
		conds = append(conds, "symbol = ?")
		args = append(args, f.Symbol)
	}
	if f.SinceMs > 0 {
		conds = append(conds, "stat_date >= ?")
		args = append(args, msToUTCDate(f.SinceMs))
	}
	if f.UntilMs > 0 {
		conds = append(conds, "stat_date <= ?")
		args = append(args, msToUTCDate(f.UntilMs))
	}
	where := strings.Join(conds, " AND ")
	keyQ := `SELECT symbol, stat_date FROM perp_user_fee_stats_daily WHERE ` + where + `
		UNION SELECT symbol, stat_date FROM perp_funding_stats_daily WHERE ` + where + `
		UNION SELECT symbol, stat_date FROM perp_realized_pnl_stats_daily WHERE ` + where

	outer := []string{"1=1"}
	outerArgs := []any{}
	if rawCursor != "" {
		outer = append(outer, "(k.stat_date < ? OR (k.stat_date = ? AND k.symbol > ?))")
		outerArgs = append(outerArgs, cur.Date, cur.Date, cur.Symbol)
	}

	q := `
		SELECT k.symbol, CAST(k.stat_date AS CHAR),
		       CAST(COALESCE(fe.trading_fee, 0) AS CHAR), CAST(COALESCE(fe.rebate, 0) AS CHAR),
		       CAST(COALESCE(fe.fee_deficit, 0) AS CHAR),
		       CAST(COALESCE(fu.funding_paid, 0) AS CHAR), CAST(COALESCE(fu.funding_received, 0) AS CHAR),
		       CAST(COALESCE(rp.realized_pnl, 0) AS CHAR)
		FROM (` + keyQ + `) k
		LEFT JOIN perp_user_fee_stats_daily fe
		  ON fe.user_id = ? AND fe.symbol = k.symbol AND fe.stat_date = k.stat_date
		LEFT JOIN perp_funding_stats_daily fu
		  ON fu.user_id = ? AND fu.symbol = k.symbol AND fu.stat_date = k.stat_date
		LEFT JOIN perp_realized_pnl_stats_daily rp
		  ON rp.user_id = ? AND rp.symbol = k.symbol AND rp.stat_date = k.stat_date
		WHERE ` + strings.Join(outer, " AND ") + `
		ORDER BY k.stat_date DESC, k.symbol ASC
		LIMIT ?`

	all := make([]any, 0, len(args)*3+len(outerArgs)+4)
	all = append(all, args...) // fee keys
	all = append(all, args...) // funding keys
	all = append(all, args...) // realized keys
	all = append(all, f.UserID, f.UserID, f.UserID)
	all = append(all, outerArgs...)
	all = append(all, limit+1)

	rows, err := s.db.QueryContext(ctx, q, all...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.PerpDailyStat
	for rows.Next() {
		var r historypb.PerpDailyStat
		if err := rows.Scan(&r.Symbol, &r.StatDate, &r.TradingFee, &r.Rebate, &r.FeeDeficit,
			&r.FundingPaid, &r.FundingReceived, &r.RealizedPnl); err != nil {
			return nil, "", err
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		next, err = cursor.Encode(cursor.PerpDailyStatCursor{Date: last.StatDate, Symbol: last.Symbol})
		if err != nil {
			return nil, "", err
		}
	}
	return out, next, nil
}

// msToUTCDate converts a unix-ms timestamp to its UTC calendar date string.
func msToUTCDate(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02")
}
