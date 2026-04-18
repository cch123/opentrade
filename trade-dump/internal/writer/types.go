// Package writer turns decoded OpenTrade events into idempotent MySQL writes.
//
// The sidecar consumes Kafka as the source of truth (ADR-0001, ADR-0008) and
// projects a subset of events onto the MySQL schema defined in
// deploy/docker/mysql-init/01-schema.sql. Because Kafka delivery is
// at-least-once, every write must be idempotent — see InsertTrades.
package writer

import (
	"context"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// TradeRow is the MySQL projection of a Trade payload from trade-event.
// Fields map 1:1 onto deploy/docker/mysql-init/01-schema.sql#trades.
type TradeRow struct {
	TradeID      string
	Symbol       string
	Price        string
	Qty          string
	MakerUserID  string
	MakerOrderID uint64
	TakerUserID  string
	TakerOrderID uint64
	TakerSide    int8  // opentrade.event.Side (0 unspecified / 1 buy / 2 sell)
	TS           int64 // ms since epoch, copied from EventMeta.ts_unix_ms
	SymbolSeqID  uint64
}

// TradeWriter is what the consumer talks to. A real implementation writes to
// MySQL; tests can swap in fakes.
type TradeWriter interface {
	InsertTrades(ctx context.Context, rows []TradeRow) error
}

// TradeRowFromEvent projects a TradeEvent onto a TradeRow. The second return
// value is false when the event is not a Trade payload (e.g. OrderAccepted,
// OrderCancelled) — callers should skip these for the trades table.
//
// The EventMeta.seq_id is treated as the per-symbol monotonic id (see
// api/event/common.proto EventMeta docs and match/internal/journal/convert.go).
func TradeRowFromEvent(evt *eventpb.TradeEvent) (TradeRow, bool) {
	if evt == nil {
		return TradeRow{}, false
	}
	trade, ok := evt.Payload.(*eventpb.TradeEvent_Trade)
	if !ok || trade.Trade == nil {
		return TradeRow{}, false
	}
	t := trade.Trade
	row := TradeRow{
		TradeID:      t.TradeId,
		Symbol:       t.Symbol,
		Price:        t.Price,
		Qty:          t.Qty,
		MakerUserID:  t.MakerUserId,
		MakerOrderID: t.MakerOrderId,
		TakerUserID:  t.TakerUserId,
		TakerOrderID: t.TakerOrderId,
		TakerSide:    int8(t.TakerSide),
	}
	if evt.Meta != nil {
		row.TS = evt.Meta.TsUnixMs
		row.SymbolSeqID = evt.Meta.SeqId
	}
	return row, true
}
