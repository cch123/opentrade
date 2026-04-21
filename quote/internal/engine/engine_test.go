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

// TestHandle_Trade_DualEmitDedup simulates the ADR-0058 §2 dual-emit:
// Match publishes the same Trade twice (once per party partition) with
// the same match_seq_id. Quote must fold it exactly once — a second
// copy leaves PublicTrade not duplicated and Kline volume unchanged.
func TestHandle_Trade_DualEmitDedup(t *testing.T) {
	e := newTestEngine(t)
	build := func() *eventpb.TradeEvent {
		return &eventpb.TradeEvent{
			Meta:       &eventpb.EventMeta{TsUnixMs: 130_000},
			MatchSeqId: 42, // same seq → dual-emit
			Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
				TradeId: "BTC-USDT:42", Symbol: "BTC-USDT",
				Price: "100", Qty: "2",
				MakerUserId:  "maker",
				MakerOrderId: 1,
				TakerUserId:  "taker",
				TakerOrderId: 2,
				TakerSide:    eventpb.Side_SIDE_BUY,
			}},
		}
	}
	first := e.Handle(build())
	second := e.Handle(build())

	countTrades := func(evts []*eventpb.MarketDataEvent) int {
		n := 0
		for _, ev := range evts {
			if _, ok := ev.Payload.(*eventpb.MarketDataEvent_PublicTrade); ok {
				n++
			}
		}
		return n
	}
	if got := countTrades(first); got != 1 {
		t.Fatalf("first emit: %d public trades, want 1", got)
	}
	if len(second) != 0 {
		t.Fatalf("second emit (dual-emit duplicate): %d events, want 0: %+v", len(second), second)
	}

	// Verify the watermark was persisted so a fresh Restore+re-play of
	// that same match_seq would also skip. This is the bit that makes
	// the dedup survive restarts.
	snap := e.Capture()
	if snap.Symbols["BTC-USDT"].LastTradeMatchSeq != 42 {
		t.Errorf("watermark = %d, want 42", snap.Symbols["BTC-USDT"].LastTradeMatchSeq)
	}

	dst := newTestEngine(t)
	if err := dst.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	again := dst.Handle(build())
	if len(again) != 0 {
		t.Errorf("after restore, duplicate match_seq=42 should be skipped: %+v", again)
	}
}
