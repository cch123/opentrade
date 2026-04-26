package writer

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// TriggerRow mirrors the `triggers` table. One row per
// TriggerUpdate event; trade-dump upserts by id with a
// last_update_ms guard so out-of-order / duplicate events never regress
// fresher state (ADR-0047 §Guard).
type TriggerRow struct {
	ID                  uint64
	ClientTriggerID string
	UserID              string
	Symbol              string
	Side                int8
	Type                int8
	StopPrice           string
	LimitPrice          string
	Qty                 string
	QuoteQty            string
	TIF                 int8
	Status              int8
	TriggeredOrderID    uint64
	RejectReason        string
	ExpiresAtMs         int64
	OCOGroupID          string
	TrailingDeltaBps    int32
	ActivationPrice     string
	TrailingWatermark   string
	TrailingActive      bool
	CreatedAtMs         int64
	TriggeredAtMs       int64
	LastUpdateMs        int64
}

// TriggerBatch is the aggregate of decoded events. The writer applies
// the whole slice in one transaction so a full consumer fetch lands
// atomically.
type TriggerBatch struct {
	Rows []TriggerRow
}

// IsEmpty reports whether the batch has nothing to write.
func (b *TriggerBatch) IsEmpty() bool { return len(b.Rows) == 0 }

// BuildTriggerBatch projects a slice of TriggerUpdate events into
// MySQL rows. Malformed events (nil, unspecified type/status) are dropped
// with no error so a single bad record doesn't stall the pipeline.
func BuildTriggerBatch(events []*eventpb.TriggerUpdate) TriggerBatch {
	out := TriggerBatch{Rows: make([]TriggerRow, 0, len(events))}
	for _, e := range events {
		if e == nil || e.Id == 0 || e.UserId == "" {
			continue
		}
		meta := e.Meta
		var tsUnixMs int64
		if meta != nil {
			tsUnixMs = meta.TsUnixMs
		}
		trailingActive := int8(0)
		if e.TrailingActive {
			trailingActive = 1
		}
		out.Rows = append(out.Rows, TriggerRow{
			ID:                  e.Id,
			ClientTriggerID: e.ClientTriggerId,
			UserID:              e.UserId,
			Symbol:              e.Symbol,
			Side:                int8(e.Side),
			Type:                int8(e.Type),
			StopPrice:           e.StopPrice,
			LimitPrice:          e.LimitPrice,
			Qty:                 e.Qty,
			QuoteQty:            e.QuoteQty,
			TIF:                 int8(e.Tif),
			Status:              int8(e.Status),
			TriggeredOrderID:    e.PlacedOrderId,
			RejectReason:        e.RejectReason,
			ExpiresAtMs:         e.ExpiresAtUnixMs,
			OCOGroupID:          e.OcoGroupId,
			TrailingDeltaBps:    e.TrailingDeltaBps,
			ActivationPrice:     e.ActivationPrice,
			TrailingWatermark:   e.TrailingWatermark,
			TrailingActive:      trailingActive != 0,
			CreatedAtMs:         e.CreatedAtUnixMs,
			TriggeredAtMs:       e.TriggeredAtUnixMs,
			LastUpdateMs:        tsUnixMs,
		})
	}
	return out
}
