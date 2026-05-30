package mysqlstore

// perp.go is HistoryService's read path over trade-dump's perp_* projection
// (ADR-0068 M7). Positions are current state (small, unpaged); funding and
// liquidations are append-only ledgers paged newest-first by (ts, perp_seq_id).

import (
	"context"
	"strings"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/cursor"
)

// PerpLedgerFilter is the shared filter for the perp funding / liquidation
// ledgers.
type PerpLedgerFilter struct {
	UserID  string
	Symbol  string
	SinceMs int64
	UntilMs int64
}

// ListPerpPositions returns a user's current positions (state, not paged),
// ordered by symbol.
func (s *Store) ListPerpPositions(ctx context.Context, userID, symbol string) ([]*historypb.PerpPosition, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	conds := []string{"user_id = ?"}
	args := []any{userID}
	if symbol != "" {
		conds = append(conds, "symbol = ?")
		args = append(args, symbol)
	}
	q := `
		SELECT user_id, symbol, side, CAST(size AS CHAR), CAST(entry_price AS CHAR),
		       CAST(margin AS CHAR), CAST(leverage AS CHAR), CAST(realized_pnl AS CHAR),
		       CAST(UNIX_TIMESTAMP(updated_at) * 1000 AS SIGNED)
		FROM perp_positions
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY symbol ASC`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*historypb.PerpPosition
	for rows.Next() {
		var p historypb.PerpPosition
		var side int8
		if err := rows.Scan(&p.UserId, &p.Symbol, &side, &p.Size, &p.EntryPrice,
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
		SELECT perp_seq_id, symbol, funding_round_id, CAST(funding_rate AS CHAR),
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
		if err := rows.Scan(&r.PerpSeqId, &r.Symbol, &r.FundingRoundId, &r.FundingRate,
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
		SELECT perp_seq_id, symbol, liq_order_id, CAST(bankruptcy_price AS CHAR),
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
		if err := rows.Scan(&r.PerpSeqId, &r.Symbol, &r.LiqOrderId, &r.BankruptcyPrice,
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
