package writer

// perp_stats.go maintains the ADR-0079 §6 daily aggregate tables. The
// idempotency mechanism is recompute-on-touch: for every (user, symbol,
// UTC day) key a batch touches, the whole aggregate row is RECOMPUTED from
// the deduplicated base ledgers inside the same transaction as the base
// inserts (INSERT ... SELECT ... ON DUPLICATE KEY UPDATE). The row is a pure
// function of the base tables, so Kafka redelivery / batch replay converges
// to the same value instead of double-counting the way naive increments
// would.

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const dayMs = int64(24 * time.Hour / time.Millisecond)

// perpStatKey is one touched (user, symbol, UTC day) aggregation key.
type perpStatKey struct {
	UserID     uint64
	Symbol     string
	DayStartMs int64
}

func perpStatKeyOf(user uint64, symbol string, tsMs int64) perpStatKey {
	day := tsMs - tsMs%dayMs
	if tsMs < 0 { // defensive; production timestamps are epoch-positive
		day = tsMs - (tsMs%dayMs+dayMs)%dayMs
	}
	return perpStatKey{UserID: user, Symbol: symbol, DayStartMs: day}
}

func (k perpStatKey) date() string {
	return time.UnixMilli(k.DayStartMs).UTC().Format("2006-01-02")
}

func (k perpStatKey) dayEndMs() int64 { return k.DayStartMs + dayMs }

// recomputePerpDailyStats refreshes every daily-stat row the batch touched.
// Runs inside the batch transaction, after the base ledger inserts.
func (m *MySQL) recomputePerpDailyStats(ctx context.Context, tx *sql.Tx, batch PerpBatch) error {
	feeKeys := map[perpStatKey]struct{}{}
	fundingKeys := map[perpStatKey]struct{}{}
	realizedKeys := map[perpStatKey]struct{}{}
	for _, r := range batch.Settlements {
		k := perpStatKeyOf(r.UserID, r.Symbol, r.TsUnixMs)
		feeKeys[k] = struct{}{}
		realizedKeys[k] = struct{}{}
	}
	for _, r := range batch.Funding {
		fundingKeys[perpStatKeyOf(r.UserID, r.Symbol, r.TsUnixMs)] = struct{}{}
	}
	// Liquidations carries both PerpLiquidationEvent and PerpTakeoverEvent
	// rows (the user-facing forced close is realized PnL either way); ADL is
	// the third realized source. Funding is excluded — it has its own table.
	for _, r := range batch.Liquidations {
		realizedKeys[perpStatKeyOf(r.UserID, r.Symbol, r.TsUnixMs)] = struct{}{}
	}
	for _, r := range batch.ADL {
		realizedKeys[perpStatKeyOf(r.UserID, r.Symbol, r.TsUnixMs)] = struct{}{}
	}

	for k := range feeKeys {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO perp_user_fee_stats_daily (user_id, symbol, stat_date, fee_asset, trading_fee, rebate, fee_deficit)
SELECT ?, ?, ?,
       COALESCE(MAX(NULLIF(fee_asset, '')), ''),
       COALESCE(SUM(CASE WHEN fee > 0 THEN fee ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN fee < 0 THEN -fee ELSE 0 END), 0),
       COALESCE(SUM(fee_deficit), 0)
FROM perp_settlements
WHERE user_id = ? AND symbol = ? AND ts_unix_ms >= ? AND ts_unix_ms < ?
ON DUPLICATE KEY UPDATE
  fee_asset = VALUES(fee_asset), trading_fee = VALUES(trading_fee),
  rebate = VALUES(rebate), fee_deficit = VALUES(fee_deficit)`,
			k.UserID, k.Symbol, k.date(), k.UserID, k.Symbol, k.DayStartMs, k.dayEndMs()); err != nil {
			return fmt.Errorf("perp_user_fee_stats_daily recompute: %w", err)
		}
	}
	for k := range fundingKeys {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO perp_funding_stats_daily (user_id, symbol, stat_date, funding_paid, funding_received)
SELECT ?, ?, ?,
       COALESCE(SUM(CASE WHEN payment < 0 THEN -payment ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN payment > 0 THEN payment ELSE 0 END), 0)
FROM perp_funding
WHERE user_id = ? AND symbol = ? AND ts_unix_ms >= ? AND ts_unix_ms < ?
ON DUPLICATE KEY UPDATE
  funding_paid = VALUES(funding_paid), funding_received = VALUES(funding_received)`,
			k.UserID, k.Symbol, k.date(), k.UserID, k.Symbol, k.DayStartMs, k.dayEndMs()); err != nil {
			return fmt.Errorf("perp_funding_stats_daily recompute: %w", err)
		}
	}
	for k := range realizedKeys {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO perp_realized_pnl_stats_daily (user_id, symbol, stat_date, realized_pnl)
SELECT ?, ?, ?,
    COALESCE((SELECT SUM(realized_pnl) FROM perp_settlements
              WHERE user_id = ? AND symbol = ? AND ts_unix_ms >= ? AND ts_unix_ms < ?), 0)
  + COALESCE((SELECT SUM(realized_pnl) FROM perp_liquidations
              WHERE user_id = ? AND symbol = ? AND ts_unix_ms >= ? AND ts_unix_ms < ?), 0)
  + COALESCE((SELECT SUM(realized_pnl) FROM perp_adl_events
              WHERE user_id = ? AND symbol = ? AND ts_unix_ms >= ? AND ts_unix_ms < ?), 0)
ON DUPLICATE KEY UPDATE realized_pnl = VALUES(realized_pnl)`,
			k.UserID, k.Symbol, k.date(),
			k.UserID, k.Symbol, k.DayStartMs, k.dayEndMs(),
			k.UserID, k.Symbol, k.DayStartMs, k.dayEndMs(),
			k.UserID, k.Symbol, k.DayStartMs, k.dayEndMs()); err != nil {
			return fmt.Errorf("perp_realized_pnl_stats_daily recompute: %w", err)
		}
	}
	return nil
}
