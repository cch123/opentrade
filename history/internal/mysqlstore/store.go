// Package mysqlstore implements HistoryService's read path over the
// trade-dump MySQL projection (ADR-0023 / ADR-0028). All statements are
// read-only and parameterised; the caller (server layer) is responsible
// for user-scoped WHERE clauses via the supplied user_id.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // driver init

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/cursor"
)

// Config tunes the shared *sql.DB backing the store. Sensible zeroes
// handled by NewStore so callers can leave most fields empty.
type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	QueryTimeout    time.Duration // per-statement timeout; 0 → 2s
}

// Defaults applied for zero-valued Config fields.
const (
	DefaultMaxOpen         = 16
	DefaultMaxIdle         = 4
	DefaultConnMaxLifetime = 30 * time.Minute
	DefaultQueryTimeout    = 2 * time.Second

	// DefaultLimit and MaxLimit bound ListOrders/ListTrades/ListAccountLogs.
	DefaultLimit = 100
	MaxLimit     = 500
)

// Store is the concrete MySQL-backed implementation. All methods are safe
// for concurrent use.
type Store struct {
	db           *sql.DB
	queryTimeout time.Duration
}

// ErrNotFound signals a single-row lookup (GetOrder) hit no rows.
var ErrNotFound = errors.New("history: not found")

// NewStore opens the pool. `db` is initialised with the DSN; callers
// should `Close()` when done.
func NewStore(cfg Config) (*Store, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("mysqlstore: empty DSN")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("mysqlstore: open: %w", err)
	}
	open := cfg.MaxOpenConns
	if open <= 0 {
		open = DefaultMaxOpen
	}
	idle := cfg.MaxIdleConns
	if idle <= 0 {
		idle = DefaultMaxIdle
	}
	life := cfg.ConnMaxLifetime
	if life <= 0 {
		life = DefaultConnMaxLifetime
	}
	db.SetMaxOpenConns(open)
	db.SetMaxIdleConns(idle)
	db.SetConnMaxLifetime(life)

	qto := cfg.QueryTimeout
	if qto <= 0 {
		qto = DefaultQueryTimeout
	}
	return &Store{db: db, queryTimeout: qto}, nil
}

// NewStoreWithDB injects an already-configured *sql.DB. Intended for
// tests using sqlmock; production code should use NewStore.
func NewStoreWithDB(db *sql.DB, queryTimeout time.Duration) *Store {
	if queryTimeout <= 0 {
		queryTimeout = DefaultQueryTimeout
	}
	return &Store{db: db, queryTimeout: queryTimeout}
}

// Close releases the pool.
func (s *Store) Close() error { return s.db.Close() }

// Ping reports connection health.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// ---------------------------------------------------------------------------
// GetOrder
// ---------------------------------------------------------------------------

// GetOrder returns the single order by id scoped to userID. Returns
// ErrNotFound when the row does not exist or belongs to another user —
// the caller decides whether to split NotFound vs PermissionDenied at
// the RPC boundary (we don't, to avoid leaking existence).
func (s *Store) GetOrder(ctx context.Context, userID string, orderID uint64) (*historypb.Order, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	// UNIX_TIMESTAMP()*1000 + MICROSECOND() DIV 1000 yields a DECIMAL in
	// MySQL 8 which the Go driver returns as []byte — int64 scan would fail
	// with "invalid syntax". CAST to SIGNED forces a proper integer column.
	const q = `
		SELECT order_id, client_order_id, user_id, symbol, side, order_type, tif,
		       CAST(price AS CHAR), CAST(qty AS CHAR), CAST(filled_qty AS CHAR),
		       CAST(frozen_amt AS CHAR), status, reject_reason,
		       CAST(UNIX_TIMESTAMP(created_at) * 1000 + MICROSECOND(created_at) DIV 1000 AS SIGNED),
		       CAST(UNIX_TIMESTAMP(updated_at) * 1000 + MICROSECOND(updated_at) DIV 1000 AS SIGNED)
		FROM orders WHERE order_id = ? AND user_id = ? LIMIT 1`

	row := s.db.QueryRowContext(ctx, q, orderID, userID)
	o, err := scanOrder(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return o, nil
}

// ---------------------------------------------------------------------------
// ListOrders
// ---------------------------------------------------------------------------

// OrdersFilter is the decoded ListOrdersRequest for the store.
type OrdersFilter struct {
	UserID   string
	Symbol   string
	Statuses []int8 // projected from scope/statuses at the server layer; empty = any
	SinceMs  int64
	UntilMs  int64
}

// ListOrders returns up to `limit` orders matching filter, newest first.
// rawCursor (if non-empty) must have been produced by a prior ListOrders
// response. nextCursor is empty when fewer than limit+1 rows are found
// (end of stream).
func (s *Store) ListOrders(ctx context.Context, f OrdersFilter, rawCursor string, limit int) ([]*historypb.Order, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	limit = clampLimit(limit)

	var cur cursor.OrdersCursor
	if err := cursor.Decode(rawCursor, &cur); err != nil {
		return nil, "", err
	}

	var (
		conds = []string{"user_id = ?"}
		args  = []any{f.UserID}
	)
	if f.Symbol != "" {
		conds = append(conds, "symbol = ?")
		args = append(args, f.Symbol)
	}
	if len(f.Statuses) > 0 {
		placeholders := make([]string, len(f.Statuses))
		for i, st := range f.Statuses {
			placeholders[i] = "?"
			args = append(args, st)
		}
		conds = append(conds, "status IN ("+strings.Join(placeholders, ",")+")")
	}
	if f.SinceMs > 0 {
		conds = append(conds, "created_at >= FROM_UNIXTIME(? / 1000)")
		args = append(args, f.SinceMs)
	}
	if f.UntilMs > 0 {
		conds = append(conds, "created_at < FROM_UNIXTIME(? / 1000)")
		args = append(args, f.UntilMs)
	}
	if rawCursor != "" {
		// Strict < tuple to avoid returning the boundary row twice.
		conds = append(conds,
			"(UNIX_TIMESTAMP(created_at) * 1000 + MICROSECOND(created_at) DIV 1000 < ? "+
				"OR (UNIX_TIMESTAMP(created_at) * 1000 + MICROSECOND(created_at) DIV 1000 = ? AND order_id < ?))")
		args = append(args, cur.CreatedAt, cur.CreatedAt, cur.OrderID)
	}

	q := `
		SELECT order_id, client_order_id, user_id, symbol, side, order_type, tif,
		       CAST(price AS CHAR), CAST(qty AS CHAR), CAST(filled_qty AS CHAR),
		       CAST(frozen_amt AS CHAR), status, reject_reason,
		       CAST(UNIX_TIMESTAMP(created_at) * 1000 + MICROSECOND(created_at) DIV 1000 AS SIGNED),
		       CAST(UNIX_TIMESTAMP(updated_at) * 1000 + MICROSECOND(updated_at) DIV 1000 AS SIGNED)
		FROM orders
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY created_at DESC, order_id DESC
		LIMIT ?`
	args = append(args, limit+1) // probe for next cursor

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		c, err := cursor.Encode(cursor.OrdersCursor{
			CreatedAt: last.CreatedAtUnixMs,
			OrderID:   last.OrderId,
		})
		if err != nil {
			return nil, "", err
		}
		next = c
	}
	return out, next, nil
}

// ---------------------------------------------------------------------------
// ListTrades
// ---------------------------------------------------------------------------

// TradesFilter is the decoded ListTradesRequest for the store.
type TradesFilter struct {
	UserID  string
	Symbol  string
	SinceMs int64
	UntilMs int64
}

// ListTrades fans the user's trades out of both maker and taker indexes
// via UNION ALL, sorted newest-first. We over-fetch limit+1 per leg then
// keep the top limit overall to provide a next cursor boundary.
func (s *Store) ListTrades(ctx context.Context, f TradesFilter, rawCursor string, limit int) ([]*historypb.Trade, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	limit = clampLimit(limit)

	var cur cursor.TradesCursor
	if err := cursor.Decode(rawCursor, &cur); err != nil {
		return nil, "", err
	}

	// sideExpr folds taker_side to the requesting user's side. If the user
	// is the taker, their side equals taker_side; if maker, it's the
	// opposite. We compute this in SQL so the row scan stays uniform.
	buildLeg := func(roleColumn, userCol, sideExpr, orderCol string) (string, []any) {
		conds := []string{userCol + " = ?"}
		args := []any{f.UserID}
		if f.Symbol != "" {
			conds = append(conds, "symbol = ?")
			args = append(args, f.Symbol)
		}
		if f.SinceMs > 0 {
			conds = append(conds, "ts >= ?")
			args = append(args, f.SinceMs)
		}
		if f.UntilMs > 0 {
			conds = append(conds, "ts < ?")
			args = append(args, f.UntilMs)
		}
		if rawCursor != "" {
			conds = append(conds, "(ts < ? OR (ts = ? AND trade_id < ?))")
			args = append(args, cur.Ts, cur.Ts, cur.TradeID)
		}
		q := fmt.Sprintf(
			`SELECT trade_id, symbol, CAST(price AS CHAR), CAST(qty AS CHAR), %s AS role, %s AS user_order_id, %s AS user_side, ts
			 FROM trades WHERE %s ORDER BY ts DESC, trade_id DESC LIMIT ?`,
			roleColumn, orderCol, sideExpr, strings.Join(conds, " AND "))
		args = append(args, limit+1)
		return q, args
	}

	takerQ, takerArgs := buildLeg("2", "taker_user_id", "taker_side", "taker_order_id")
	// maker side = opposite of taker_side. Side enum: 1=BUY, 2=SELL.
	makerQ, makerArgs := buildLeg("1", "maker_user_id", "CASE taker_side WHEN 1 THEN 2 WHEN 2 THEN 1 ELSE 0 END", "maker_order_id")

	q := `SELECT * FROM ((` + takerQ + `) UNION ALL (` + makerQ + `)) t
	      ORDER BY ts DESC, trade_id DESC LIMIT ?`
	args := append(append([]any{}, takerArgs...), makerArgs...)
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.Trade
	for rows.Next() {
		t, err := scanTrade(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		c, err := cursor.Encode(cursor.TradesCursor{
			Ts:      last.TsUnixMs,
			TradeID: last.TradeId,
		})
		if err != nil {
			return nil, "", err
		}
		next = c
	}
	return out, next, nil
}

// ---------------------------------------------------------------------------
// ListAccountLogs
// ---------------------------------------------------------------------------

// AccountLogsFilter is the decoded ListAccountLogsRequest for the store.
type AccountLogsFilter struct {
	UserID   string
	Asset    string
	BizTypes []string
	SinceMs  int64
	UntilMs  int64
}

// ListAccountLogs walks `account_logs` newest-first for the user.
func (s *Store) ListAccountLogs(ctx context.Context, f AccountLogsFilter, rawCursor string, limit int) ([]*historypb.AccountLog, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	limit = clampLimit(limit)

	var cur cursor.AccountLogsCursor
	if err := cursor.Decode(rawCursor, &cur); err != nil {
		return nil, "", err
	}

	conds := []string{"user_id = ?"}
	args := []any{f.UserID}
	if f.Asset != "" {
		conds = append(conds, "asset = ?")
		args = append(args, f.Asset)
	}
	if len(f.BizTypes) > 0 {
		placeholders := make([]string, len(f.BizTypes))
		for i, t := range f.BizTypes {
			placeholders[i] = "?"
			args = append(args, t)
		}
		conds = append(conds, "biz_type IN ("+strings.Join(placeholders, ",")+")")
	}
	if f.SinceMs > 0 {
		conds = append(conds, "ts >= ?")
		args = append(args, f.SinceMs)
	}
	if f.UntilMs > 0 {
		conds = append(conds, "ts < ?")
		args = append(args, f.UntilMs)
	}
	if rawCursor != "" {
		conds = append(conds,
			"(ts < ? OR (ts = ? AND (shard_id < ? OR "+
				"(shard_id = ? AND (counter_seq_id < ? OR (counter_seq_id = ? AND asset < ?))))))")
		args = append(args, cur.Ts, cur.Ts, cur.ShardID, cur.ShardID, cur.CounterSeqID, cur.CounterSeqID, cur.Asset)
	}

	q := `
		SELECT shard_id, counter_seq_id, asset, user_id,
		       CAST(delta_avail AS CHAR), CAST(delta_frozen AS CHAR),
		       CAST(avail_after AS CHAR), CAST(frozen_after AS CHAR),
		       biz_type, biz_ref_id, ts
		FROM account_logs
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY ts DESC, shard_id DESC, counter_seq_id DESC, asset DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.AccountLog
	for rows.Next() {
		l, err := scanAccountLog(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		c, err := cursor.Encode(cursor.AccountLogsCursor{
			Ts:           last.TsUnixMs,
			ShardID:      last.ShardId,
			CounterSeqID: last.CounterSeqId,
			Asset:        last.Asset,
		})
		if err != nil {
			return nil, "", err
		}
		next = c
	}
	return out, next, nil
}

// ---------------------------------------------------------------------------
// GetConditional
// ---------------------------------------------------------------------------

// GetConditional fetches a single conditional by id, scoped to userID.
// Returns ErrNotFound when the row does not exist or belongs to another
// user — same policy as GetOrder.
func (s *Store) GetConditional(ctx context.Context, userID string, id uint64) (*historypb.Conditional, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
		SELECT id, client_conditional_id, user_id, symbol, side, type,
		       CAST(stop_price AS CHAR), CAST(limit_price AS CHAR),
		       CAST(qty AS CHAR), CAST(quote_qty AS CHAR),
		       tif, status, triggered_order_id, reject_reason,
		       expires_at_unix_ms, oco_group_id,
		       trailing_delta_bps, CAST(activation_price AS CHAR),
		       CAST(trailing_watermark AS CHAR), trailing_active,
		       created_at_unix_ms, triggered_at_unix_ms
		FROM conditionals WHERE id = ? AND user_id = ? LIMIT 1`

	row := s.db.QueryRowContext(ctx, q, id, userID)
	c, err := scanConditional(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// ListConditionals
// ---------------------------------------------------------------------------

// ConditionalsFilter is the decoded ListConditionalsRequest for the store.
type ConditionalsFilter struct {
	UserID   string
	Symbol   string
	Statuses []int8
	SinceMs  int64
	UntilMs  int64
}

// ListConditionals pages a user's conditionals newest-first by
// created_at_unix_ms. Follows the same over-fetch-by-one-to-detect-next
// convention as ListOrders.
func (s *Store) ListConditionals(ctx context.Context, f ConditionalsFilter, rawCursor string, limit int) ([]*historypb.Conditional, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	limit = clampLimit(limit)

	var cur cursor.ConditionalsCursor
	if err := cursor.Decode(rawCursor, &cur); err != nil {
		return nil, "", err
	}

	conds := []string{"user_id = ?"}
	args := []any{f.UserID}
	if f.Symbol != "" {
		conds = append(conds, "symbol = ?")
		args = append(args, f.Symbol)
	}
	if len(f.Statuses) > 0 {
		placeholders := make([]string, len(f.Statuses))
		for i, st := range f.Statuses {
			placeholders[i] = "?"
			args = append(args, st)
		}
		conds = append(conds, "status IN ("+strings.Join(placeholders, ",")+")")
	}
	if f.SinceMs > 0 {
		conds = append(conds, "created_at_unix_ms >= ?")
		args = append(args, f.SinceMs)
	}
	if f.UntilMs > 0 {
		conds = append(conds, "created_at_unix_ms < ?")
		args = append(args, f.UntilMs)
	}
	if rawCursor != "" {
		conds = append(conds,
			"(created_at_unix_ms < ? OR (created_at_unix_ms = ? AND id < ?))")
		args = append(args, cur.CreatedAt, cur.CreatedAt, cur.ID)
	}

	q := `
		SELECT id, client_conditional_id, user_id, symbol, side, type,
		       CAST(stop_price AS CHAR), CAST(limit_price AS CHAR),
		       CAST(qty AS CHAR), CAST(quote_qty AS CHAR),
		       tif, status, triggered_order_id, reject_reason,
		       expires_at_unix_ms, oco_group_id,
		       trailing_delta_bps, CAST(activation_price AS CHAR),
		       CAST(trailing_watermark AS CHAR), trailing_active,
		       created_at_unix_ms, triggered_at_unix_ms
		FROM conditionals
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY created_at_unix_ms DESC, id DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var out []*historypb.Conditional
	for rows.Next() {
		c, err := scanConditional(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var next string
	if len(out) > limit {
		last := out[limit-1]
		out = out[:limit]
		c, err := cursor.Encode(cursor.ConditionalsCursor{
			CreatedAt: last.CreatedAtUnixMs,
			ID:        last.Id,
		})
		if err != nil {
			return nil, "", err
		}
		next = c
	}
	return out, next, nil
}

// ---------------------------------------------------------------------------
// scan helpers — tolerant of both *sql.Row and *sql.Rows via the Scanner
// ---------------------------------------------------------------------------

type rowScanner interface {
	Scan(dest ...any) error
}

func scanOrder(r rowScanner) (*historypb.Order, error) {
	var (
		o         historypb.Order
		side      int8
		orderType int8
		tif       int8
		status    int8
		reject    int8
	)
	if err := r.Scan(
		&o.OrderId, &o.ClientOrderId, &o.UserId, &o.Symbol,
		&side, &orderType, &tif,
		&o.Price, &o.Qty, &o.FilledQty, &o.FrozenAmt,
		&status, &reject,
		&o.CreatedAtUnixMs, &o.UpdatedAtUnixMs,
	); err != nil {
		return nil, err
	}
	o.Side = sideFromInt(side)
	o.OrderType = orderTypeFromInt(orderType)
	o.Tif = tifFromInt(tif)
	o.Status = externalStatusFromInternal(status)
	o.RejectReason = rejectReasonToString(reject)
	return &o, nil
}

func scanTrade(r rowScanner) (*historypb.Trade, error) {
	var (
		t    historypb.Trade
		role int8
		side int8
	)
	if err := r.Scan(
		&t.TradeId, &t.Symbol, &t.Price, &t.Qty,
		&role, &t.OrderId, &side, &t.TsUnixMs,
	); err != nil {
		return nil, err
	}
	t.Role = tradeRoleFromInt(role)
	t.Side = sideFromInt(side)
	return &t, nil
}

func scanConditional(r rowScanner) (*historypb.Conditional, error) {
	var (
		c         historypb.Conditional
		side      int8
		typ       int8
		tif       int8
		status    int8
		trailing  int8
	)
	if err := r.Scan(
		&c.Id, &c.ClientConditionalId, &c.UserId, &c.Symbol,
		&side, &typ,
		&c.StopPrice, &c.LimitPrice, &c.Qty, &c.QuoteQty,
		&tif, &status, &c.TriggeredOrderId, &c.RejectReason,
		&c.ExpiresAtUnixMs, &c.OcoGroupId,
		&c.TrailingDeltaBps, &c.ActivationPrice,
		&c.TrailingWatermark, &trailing,
		&c.CreatedAtUnixMs, &c.TriggeredAtUnixMs,
	); err != nil {
		return nil, err
	}
	c.Side = sideFromInt(side)
	c.Type = conditionalTypeFromInt(typ)
	c.Tif = tifFromInt(tif)
	c.Status = conditionalStatusFromInt(status)
	c.TrailingActive = trailing != 0
	return &c, nil
}

func scanAccountLog(r rowScanner) (*historypb.AccountLog, error) {
	var l historypb.AccountLog
	if err := r.Scan(
		&l.ShardId, &l.CounterSeqId, &l.Asset, &l.UserId,
		&l.DeltaAvail, &l.DeltaFrozen, &l.AvailAfter, &l.FrozenAfter,
		&l.BizType, &l.BizRefId, &l.TsUnixMs,
	); err != nil {
		return nil, err
	}
	return &l, nil
}

func clampLimit(n int) int {
	if n <= 0 {
		return DefaultLimit
	}
	if n > MaxLimit {
		return MaxLimit
	}
	return n
}
