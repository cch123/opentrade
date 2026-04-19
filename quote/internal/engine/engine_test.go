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

func TestHandle_Accepted_EmitsDepthUpdate(t *testing.T) {
	e := newTestEngine(t)
	evts := e.Handle(&eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{TsUnixMs: 120_001},
		MatchSeqId: 1,
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			UserId: "u1", OrderId: 42, Symbol: "BTC-USDT",
			Side: eventpb.Side_SIDE_BUY, Price: "100", RemainingQty: "2",
		}},
	})
	if len(evts) != 1 {
		t.Fatalf("events: %d", len(evts))
	}
	du, ok := evts[0].Payload.(*eventpb.MarketDataEvent_DepthUpdate)
	if !ok {
		t.Fatalf("payload: %T", evts[0].Payload)
	}
	if len(du.DepthUpdate.Bids) != 1 || du.DepthUpdate.Bids[0].Price != "100" {
		t.Errorf("bids: %+v", du.DepthUpdate)
	}
	if evts[0].Meta == nil || evts[0].Meta.ProducerId != "quote-test" {
		t.Errorf("meta: %+v", evts[0].Meta)
	}
}

func TestHandle_Trade_EmitsPublicTradeKlineAndDepth(t *testing.T) {
	e := newTestEngine(t)
	// Seed the book with a maker.
	_ = e.Handle(&eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{TsUnixMs: 120_000},
		MatchSeqId: 1,
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			OrderId: 50, Symbol: "BTC-USDT",
			Side: eventpb.Side_SIDE_SELL, Price: "101", RemainingQty: "5",
		}},
	})
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
		case *eventpb.MarketDataEvent_DepthUpdate:
			kinds["depth"]++
		}
	}
	if kinds["trade"] != 1 {
		t.Errorf("expected 1 public trade: %+v", kinds)
	}
	if kinds["kline_update"] < 1 {
		t.Errorf("expected at least 1 kline update: %+v", kinds)
	}
	if kinds["depth"] != 1 {
		t.Errorf("expected 1 depth update for maker level: %+v", kinds)
	}
	for _, ev := range evts {
		if ev.Meta == nil || ev.QuoteSeqId == 0 {
			t.Errorf("every event must be stamped with meta + quote_seq_id: %+v", ev)
		}
	}
}

func TestHandle_Cancelled_EmitsDepthUpdate(t *testing.T) {
	e := newTestEngine(t)
	_ = e.Handle(&eventpb.TradeEvent{
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			OrderId: 1, Symbol: "X-Y",
			Side: eventpb.Side_SIDE_BUY, Price: "10", RemainingQty: "4",
		}},
	})
	evts := e.Handle(&eventpb.TradeEvent{
		Payload: &eventpb.TradeEvent_Cancelled{Cancelled: &eventpb.OrderCancelled{
			OrderId: 1, Symbol: "X-Y",
		}},
	})
	if len(evts) != 1 {
		t.Fatalf("events: %d", len(evts))
	}
	du, ok := evts[0].Payload.(*eventpb.MarketDataEvent_DepthUpdate)
	if !ok || du.DepthUpdate.Bids[0].Qty != "0" {
		t.Errorf("expected bid qty=0 delete, got %+v", evts[0])
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

func TestSnapshotAll_EmitsOnePerActiveSymbol(t *testing.T) {
	e := newTestEngine(t)
	_ = e.Handle(&eventpb.TradeEvent{Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
		OrderId: 1, Symbol: "A", Side: eventpb.Side_SIDE_BUY, Price: "1", RemainingQty: "1",
	}}})
	_ = e.Handle(&eventpb.TradeEvent{Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
		OrderId: 2, Symbol: "B", Side: eventpb.Side_SIDE_SELL, Price: "2", RemainingQty: "1",
	}}})

	snaps := e.SnapshotAll()
	if len(snaps) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(snaps))
	}
	seen := map[string]bool{}
	for _, s := range snaps {
		if _, ok := s.Payload.(*eventpb.MarketDataEvent_DepthSnapshot); !ok {
			t.Errorf("unexpected payload: %T", s.Payload)
		}
		seen[s.Symbol] = true
	}
	if !seen["A"] || !seen["B"] {
		t.Errorf("symbols: %+v", seen)
	}
}
