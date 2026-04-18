package writer

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestTradeRowFromEvent_Trade(t *testing.T) {
	evt := &eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{SeqId: 17, TsUnixMs: 1_700_000_000_000, ProducerId: "match-shard-0-main"},
		Payload: &eventpb.TradeEvent_Trade{
			Trade: &eventpb.Trade{
				TradeId:      "BTC-USDT:17",
				Symbol:       "BTC-USDT",
				Price:        "42000.5",
				Qty:          "0.125",
				MakerUserId:  "u-maker",
				MakerOrderId: 1001,
				TakerUserId:  "u-taker",
				TakerOrderId: 2002,
				TakerSide:    eventpb.Side_SIDE_BUY,
			},
		},
	}

	row, ok := TradeRowFromEvent(evt)
	if !ok {
		t.Fatalf("expected ok=true for Trade payload")
	}
	if row.TradeID != "BTC-USDT:17" {
		t.Errorf("TradeID: got %q", row.TradeID)
	}
	if row.Symbol != "BTC-USDT" || row.Price != "42000.5" || row.Qty != "0.125" {
		t.Errorf("base fields: %+v", row)
	}
	if row.MakerUserID != "u-maker" || row.MakerOrderID != 1001 {
		t.Errorf("maker fields: %+v", row)
	}
	if row.TakerUserID != "u-taker" || row.TakerOrderID != 2002 {
		t.Errorf("taker fields: %+v", row)
	}
	if row.TakerSide != int8(eventpb.Side_SIDE_BUY) {
		t.Errorf("TakerSide: got %d want %d", row.TakerSide, eventpb.Side_SIDE_BUY)
	}
	if row.TS != 1_700_000_000_000 {
		t.Errorf("TS: got %d", row.TS)
	}
	if row.SymbolSeqID != 17 {
		t.Errorf("SymbolSeqID: got %d", row.SymbolSeqID)
	}
}

func TestTradeRowFromEvent_NonTrade(t *testing.T) {
	cases := []struct {
		name string
		evt  *eventpb.TradeEvent
	}{
		{"nil", nil},
		{"accepted", &eventpb.TradeEvent{
			Meta:    &eventpb.EventMeta{SeqId: 1},
			Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{OrderId: 1}},
		}},
		{"rejected", &eventpb.TradeEvent{
			Meta:    &eventpb.EventMeta{SeqId: 2},
			Payload: &eventpb.TradeEvent_Rejected{Rejected: &eventpb.OrderRejected{OrderId: 2}},
		}},
		{"cancelled", &eventpb.TradeEvent{
			Meta:    &eventpb.EventMeta{SeqId: 3},
			Payload: &eventpb.TradeEvent_Cancelled{Cancelled: &eventpb.OrderCancelled{OrderId: 3}},
		}},
		{"expired", &eventpb.TradeEvent{
			Meta:    &eventpb.EventMeta{SeqId: 4},
			Payload: &eventpb.TradeEvent_Expired{Expired: &eventpb.OrderExpired{OrderId: 4}},
		}},
		{"trade payload nil", &eventpb.TradeEvent{
			Meta:    &eventpb.EventMeta{SeqId: 5},
			Payload: &eventpb.TradeEvent_Trade{Trade: nil},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := TradeRowFromEvent(tc.evt); ok {
				t.Fatalf("expected ok=false for %s", tc.name)
			}
		})
	}
}

func TestTradeRowFromEvent_MissingMeta(t *testing.T) {
	evt := &eventpb.TradeEvent{
		Payload: &eventpb.TradeEvent_Trade{
			Trade: &eventpb.Trade{TradeId: "X:1", Symbol: "X-Y", Price: "1", Qty: "1"},
		},
	}
	row, ok := TradeRowFromEvent(evt)
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if row.TS != 0 || row.SymbolSeqID != 0 {
		t.Errorf("expected zero meta-derived fields, got TS=%d SeqID=%d", row.TS, row.SymbolSeqID)
	}
}
