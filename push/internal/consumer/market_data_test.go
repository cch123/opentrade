package consumer

import (
	"encoding/json"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestBuildStreamFrame_PublicTrade(t *testing.T) {
	evt := &eventpb.MarketDataEvent{
		Symbol: "BTC-USDT",
		Payload: &eventpb.MarketDataEvent_PublicTrade{PublicTrade: &eventpb.PublicTrade{
			TradeId: "1", Symbol: "BTC-USDT", Price: "100", Qty: "2",
			TakerSide: eventpb.Side_SIDE_BUY, TsUnixMs: 42,
		}},
	}
	key, payload, coal := buildStreamFrame(evt)
	if key != "trade@BTC-USDT" {
		t.Errorf("key: %q", key)
	}
	if !json.Valid(payload) {
		t.Errorf("payload not valid json: %s", payload)
	}
	if coal {
		t.Errorf("public trade must not be coalescable")
	}
}

func TestBuildStreamFrame_KlineKeyIncludesInterval(t *testing.T) {
	k := &eventpb.Kline{
		Symbol: "BTC-USDT", Interval: eventpb.KlineInterval_KLINE_INTERVAL_5M,
		OpenTimeMs: 1, CloseTimeMs: 300_001,
		Open: "1", High: "1", Low: "1", Close: "1", Volume: "1", QuoteVolume: "1", TradeCount: 1,
	}
	upd := &eventpb.MarketDataEvent{
		Symbol:  "BTC-USDT",
		Payload: &eventpb.MarketDataEvent_KlineUpdate{KlineUpdate: &eventpb.KlineUpdate{Kline: k}},
	}
	keyU, _, coalU := buildStreamFrame(upd)
	if keyU != "kline@BTC-USDT:5m" {
		t.Errorf("update key: %q", keyU)
	}
	if !coalU {
		t.Errorf("kline update must be coalescable")
	}
	closed := &eventpb.MarketDataEvent{
		Symbol:  "BTC-USDT",
		Payload: &eventpb.MarketDataEvent_KlineClosed{KlineClosed: &eventpb.KlineClosed{Kline: k}},
	}
	keyC, _, coalC := buildStreamFrame(closed)
	if keyC != "kline@BTC-USDT:5m" {
		t.Errorf("closed key: %q", keyC)
	}
	if coalC {
		t.Errorf("kline closed must not be coalescable (transition event)")
	}
}

func TestBuildStreamFrame_OrderBook(t *testing.T) {
	delta := &eventpb.MarketDataEvent{
		Symbol:     "X-Y",
		MatchSeqId: 42,
		Payload: &eventpb.MarketDataEvent_OrderBook{OrderBook: &eventpb.OrderBook{
			Data: &eventpb.OrderBook_Delta{Delta: &eventpb.OrderBookDelta{
				Bids: []*eventpb.OrderBookLevel{{Price: "1", Qty: "2"}},
			}},
		}},
	}
	key, _, coal := buildStreamFrame(delta)
	if key != "depth@X-Y" {
		t.Errorf("orderbook delta key: %q", key)
	}
	if coal {
		t.Errorf("orderbook delta must not be coalescable (carries a diff)")
	}
	full := &eventpb.MarketDataEvent{
		Symbol:     "X-Y",
		MatchSeqId: 43,
		Payload: &eventpb.MarketDataEvent_OrderBook{OrderBook: &eventpb.OrderBook{
			Data: &eventpb.OrderBook_Full{Full: &eventpb.OrderBookFull{}},
		}},
	}
	key, _, coal = buildStreamFrame(full)
	if key != "depth.snapshot@X-Y" {
		t.Errorf("orderbook full key: %q", key)
	}
	if !coal {
		t.Errorf("orderbook full must be coalescable (newer Full subsumes older)")
	}
}

func TestBuildStreamFrame_Unknown(t *testing.T) {
	key, payload, _ := buildStreamFrame(&eventpb.MarketDataEvent{Symbol: "X"})
	if key != "" || payload != nil {
		t.Errorf("expected empty for unknown payload: key=%q payload=%v", key, payload)
	}
}

func TestKlineIntervalLabel(t *testing.T) {
	cases := []struct {
		in   eventpb.KlineInterval
		want string
	}{
		{eventpb.KlineInterval_KLINE_INTERVAL_1M, "1m"},
		{eventpb.KlineInterval_KLINE_INTERVAL_5M, "5m"},
		{eventpb.KlineInterval_KLINE_INTERVAL_15M, "15m"},
		{eventpb.KlineInterval_KLINE_INTERVAL_1H, "1h"},
		{eventpb.KlineInterval_KLINE_INTERVAL_1D, "1d"},
		{eventpb.KlineInterval_KLINE_INTERVAL_UNSPECIFIED, "unknown"},
	}
	for _, c := range cases {
		if got := klineIntervalLabel(c.in); got != c.want {
			t.Errorf("%v: got %q want %q", c.in, got, c.want)
		}
	}
}
