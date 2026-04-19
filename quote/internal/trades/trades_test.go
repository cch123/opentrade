package trades

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestFromTrade_OK(t *testing.T) {
	evt := &eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{TsUnixMs: 42},
		MatchSeqId: 1,
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId:   "BTC-USDT:1",
			Symbol:    "BTC-USDT",
			Price:     "100",
			Qty:       "2",
			TakerSide: eventpb.Side_SIDE_BUY,
		}},
	}
	pt := FromTrade(evt)
	if pt == nil {
		t.Fatalf("expected non-nil")
	}
	if pt.TradeId != "BTC-USDT:1" || pt.Symbol != "BTC-USDT" ||
		pt.Price != "100" || pt.Qty != "2" ||
		pt.TakerSide != eventpb.Side_SIDE_BUY || pt.TsUnixMs != 42 {
		t.Fatalf("unexpected: %+v", pt)
	}
}

func TestFromTrade_NonTradePayloads(t *testing.T) {
	cases := []*eventpb.TradeEvent{
		nil,
		{},
		{Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{}}},
		{Payload: &eventpb.TradeEvent_Trade{Trade: nil}},
	}
	for i, c := range cases {
		if pt := FromTrade(c); pt != nil {
			t.Errorf("case %d: expected nil, got %+v", i, pt)
		}
	}
}
