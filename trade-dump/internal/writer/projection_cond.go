package writer

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// ConditionalRow mirrors the `conditionals` table. One row per
// ConditionalUpdate event; trade-dump upserts by id with a
// last_update_ms guard so out-of-order / duplicate events never regress
// fresher state (ADR-0047 §Guard).
type ConditionalRow struct {
	ID                  uint64
	ClientConditionalID string
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

// ConditionalBatch is the aggregate of decoded events. The writer applies
// the whole slice in one transaction so a full consumer fetch lands
// atomically.
type ConditionalBatch struct {
	Rows []ConditionalRow
}

// IsEmpty reports whether the batch has nothing to write.
func (b *ConditionalBatch) IsEmpty() bool { return len(b.Rows) == 0 }

// BuildConditionalBatch projects a slice of ConditionalUpdate events into
// MySQL rows. Malformed events (nil, unspecified type/status) are dropped
// with no error so a single bad record doesn't stall the pipeline.
func BuildConditionalBatch(events []*eventpb.ConditionalUpdate) ConditionalBatch {
	out := ConditionalBatch{Rows: make([]ConditionalRow, 0, len(events))}
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
		out.Rows = append(out.Rows, ConditionalRow{
			ID:                  e.Id,
			ClientConditionalID: e.ClientConditionalId,
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
