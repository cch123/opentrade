package writer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ApplyTriggerBatch upserts the `triggers` projection in one tx.
// Each incoming row carries last_update_ms; the ON DUPLICATE KEY UPDATE
// clause gates every mutable column on `VALUES(last_update_ms) >=
// last_update_ms`, so an older event (HA-handover duplicate, retry that
// races the newer update, etc.) cannot regress fresher state (ADR-0047).
func (m *MySQL) ApplyTriggerBatch(ctx context.Context, batch TriggerBatch) error {
	if batch.IsEmpty() {
		return nil
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin trigger tx: %w", err)
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
		if err := upsertTriggersChunk(ctx, tx, batch.Rows[start:end]); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit trigger tx: %w", err)
	}
	committed = true
	return nil
}

const triggerInsertCols = "" +
	"id, client_trigger_id, user_id, symbol, side, type, " +
	"stop_price, limit_price, qty, quote_qty, tif, status, " +
	"triggered_order_id, reject_reason, expires_at_unix_ms, oco_group_id, " +
	"trailing_delta_bps, activation_price, trailing_watermark, trailing_active, " +
	"created_at_unix_ms, triggered_at_unix_ms, last_update_ms"

func upsertTriggersChunk(ctx context.Context, tx *sql.Tx, rows []TriggerRow) error {
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
			r.ClientTriggerID,
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
		"client_trigger_id = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(client_trigger_id), client_trigger_id), " +
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

	q := "INSERT INTO triggers (" + triggerInsertCols + ") VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE " + updateClause
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("triggers upsert: %w", err)
	}
	return nil
}
