package engine

import (
	"testing"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/quote/internal/kline"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return New(Config{
		ProducerID: "quote-test",
		Intervals:  []kline.IntervalSpec{{Interval: eventpb.KlineInterval_KLINE_INTERVAL_1M, Millis: 60_000}},
		Clock:      func() int64 { return 1_700_000_000_000 },
	}, zap.NewNop())
}

// ADR-0055: Quote no longer projects orderbook state from OrderAccepted /
// OrderCancelled / OrderExpired events — those paths are now silently
// dropped in dispatch. Tests below cover only the remaining Trade path
// (PublicTrade + Kline).

func TestHandle_Accepted_NoEmit(t *testing.T) {
	e := newTestEngine(t)
	evts := e.Handle(&eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{TsUnixMs: 120_001},
		MatchSeqId: 1,
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			UserId: "u1", OrderId: 42, Symbol: "BTC-USDT",
			Side: eventpb.Side_SIDE_BUY, Price: "100", RemainingQty: "2",
		}},
	})
	if len(evts) != 0 {
		t.Errorf("accepted should emit nothing on Quote since ADR-0055: %+v", evts)
	}
}

func TestHandle_Trade_EmitsPublicTradeAndKline(t *testing.T) {
	e := newTestEngine(t)
	evts := e.Handle(&eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{TsUnixMs: 130_000},
		MatchSeqId: 2,
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId: "BTC-USDT:2", Symbol: "BTC-USDT",
			Price: "101", Qty: "2",
			MakerOrderId: 50, TakerOrderId: 60,
			TakerSide: eventpb.Side_SIDE_BUY,
		}},
	})

	kinds := map[string]int{}
	for _, ev := range evts {
		switch ev.Payload.(type) {
		case *eventpb.MarketDataEvent_PublicTrade:
			kinds["trade"]++
		case *eventpb.MarketDataEvent_KlineUpdate:
			kinds["kline_update"]++
		case *eventpb.MarketDataEvent_KlineClosed:
			kinds["kline_closed"]++
		}
	}
	if kinds["trade"] != 1 {
		t.Errorf("expected 1 public trade: %+v", kinds)
	}
	if kinds["kline_update"] < 1 {
		t.Errorf("expected at least 1 kline update: %+v", kinds)
	}
	for _, ev := range evts {
		if ev.Meta == nil || ev.QuoteSeqId == 0 {
			t.Errorf("every event must be stamped with meta + quote_seq_id: %+v", ev)
		}
	}
}

func TestHandle_Cancelled_NoEmit(t *testing.T) {
	e := newTestEngine(t)
	evts := e.Handle(&eventpb.TradeEvent{
		Payload: &eventpb.TradeEvent_Cancelled{Cancelled: &eventpb.OrderCancelled{
			OrderId: 1, Symbol: "X-Y",
		}},
	})
	if len(evts) != 0 {
		t.Errorf("cancelled should emit nothing on Quote since ADR-0055: %+v", evts)
	}
}

func TestHandle_Rejected_NoEmit(t *testing.T) {
	e := newTestEngine(t)
	evts := e.Handle(&eventpb.TradeEvent{
		Payload: &eventpb.TradeEvent_Rejected{Rejected: &eventpb.OrderRejected{OrderId: 1}},
	})
	if len(evts) != 0 {
		t.Errorf("rejected should emit nothing: %+v", evts)
	}
}
