package writer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ApplyConditionalBatch upserts the `conditionals` projection in one tx.
// Each incoming row carries last_update_ms; the ON DUPLICATE KEY UPDATE
// clause gates every mutable column on `VALUES(last_update_ms) >=
// last_update_ms`, so an older event (HA-handover duplicate, retry that
// races the newer update, etc.) cannot regress fresher state (ADR-0047).
func (m *MySQL) ApplyConditionalBatch(ctx context.Context, batch ConditionalBatch) error {
	if batch.IsEmpty() {
		return nil
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin conditional tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for start := 0; start < len(batch.Rows); start += m.chunkSize {
		end := start + m.chunkSize
		if end > len(batch.Rows) {
			end = len(batch.Rows)
		}
		if err := upsertConditionalsChunk(ctx, tx, batch.Rows[start:end]); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit conditional tx: %w", err)
	}
	committed = true
	return nil
}

const conditionalInsertCols = "" +
	"id, client_conditional_id, user_id, symbol, side, type, " +
	"stop_price, limit_price, qty, quote_qty, tif, status, " +
	"triggered_order_id, reject_reason, expires_at_unix_ms, oco_group_id, " +
	"trailing_delta_bps, activation_price, trailing_watermark, trailing_active, " +
	"created_at_unix_ms, triggered_at_unix_ms, last_update_ms"

func upsertConditionalsChunk(ctx context.Context, tx *sql.Tx, rows []ConditionalRow) error {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*23)
	for i, r := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		active := int8(0)
		if r.TrailingActive {
			active = 1
		}
		args = append(args,
			r.ID,
			r.ClientConditionalID,
			r.UserID,
			r.Symbol,
			r.Side,
			r.Type,
			zeroIfEmpty(r.StopPrice),
			zeroIfEmpty(r.LimitPrice),
			zeroIfEmpty(r.Qty),
			zeroIfEmpty(r.QuoteQty),
			r.TIF,
			r.Status,
			r.TriggeredOrderID,
			r.RejectReason,
			r.ExpiresAtMs,
			r.OCOGroupID,
			r.TrailingDeltaBps,
			zeroIfEmpty(r.ActivationPrice),
			zeroIfEmpty(r.TrailingWatermark),
			active,
			r.CreatedAtMs,
			r.TriggeredAtMs,
			r.LastUpdateMs,
		)
	}
	// Every mutable column is last-write-wins on last_update_ms, so
	// duplicates / out-of-order events never regress fresher rows.
	const updateClause = "" +
		"client_conditional_id = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(client_conditional_id), client_conditional_id), " +
		"user_id               = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(user_id), user_id), " +
		"symbol                = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(symbol), symbol), " +
		"side                  = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(side), side), " +
		"type                  = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(type), type), " +
		"stop_price            = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(stop_price), stop_price), " +
		"limit_price           = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(limit_price), limit_price), " +
		"qty                   = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(qty), qty), " +
		"quote_qty             = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(quote_qty), quote_qty), " +
		"tif                   = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(tif), tif), " +
		"status                = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(status), status), " +
		"triggered_order_id    = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(triggered_order_id), triggered_order_id), " +
		"reject_reason         = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(reject_reason), reject_reason), " +
		"expires_at_unix_ms    = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(expires_at_unix_ms), expires_at_unix_ms), " +
		"oco_group_id          = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(oco_group_id), oco_group_id), " +
		"trailing_delta_bps    = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(trailing_delta_bps), trailing_delta_bps), " +
		"activation_price      = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(activation_price), activation_price), " +
		"trailing_watermark    = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(trailing_watermark), trailing_watermark), " +
		"trailing_active       = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(trailing_active), trailing_active), " +
		"triggered_at_unix_ms  = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(triggered_at_unix_ms), triggered_at_unix_ms), " +
		"last_update_ms        = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(last_update_ms), last_update_ms)"

	q := "INSERT INTO conditionals (" + conditionalInsertCols + ") VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE " + updateClause
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("conditionals upsert: %w", err)
	}
	return nil
}
