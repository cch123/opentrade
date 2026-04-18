// Package trades projects trade-event Trade payloads onto the public
// PublicTrade market-data message (strips maker/taker user ids).
package trades

import eventpb "github.com/xargin/opentrade/api/gen/event"

// FromTrade builds a PublicTrade from a trade-event Trade. Returns nil if the
// payload is not usable (nil trade or missing price/qty).
func FromTrade(evt *eventpb.TradeEvent) *eventpb.PublicTrade {
	if evt == nil {
		return nil
	}
	p, ok := evt.Payload.(*eventpb.TradeEvent_Trade)
	if !ok || p.Trade == nil {
		return nil
	}
	t := p.Trade
	var ts int64
	if evt.Meta != nil {
		ts = evt.Meta.TsUnixMs
	}
	return &eventpb.PublicTrade{
		TradeId:   t.TradeId,
		Symbol:    t.Symbol,
		Price:     t.Price,
		Qty:       t.Qty,
		TakerSide: t.TakerSide,
		TsUnixMs:  ts,
	}
}
