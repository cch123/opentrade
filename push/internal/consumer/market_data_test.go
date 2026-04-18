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
	key, payload := buildStreamFrame(evt)
	if key != "trade@BTC-USDT" {
		t.Errorf("key: %q", key)
	}
	if !json.Valid(payload) {
		t.Errorf("payload not valid json: %s", payload)
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
	keyU, _ := buildStreamFrame(upd)
	if keyU != "kline@BTC-USDT:5m" {
		t.Errorf("update key: %q", keyU)
	}
	closed := &eventpb.MarketDataEvent{
		Symbol:  "BTC-USDT",
		Payload: &eventpb.MarketDataEvent_KlineClosed{KlineClosed: &eventpb.KlineClosed{Kline: k}},
	}
	keyC, _ := buildStreamFrame(closed)
	if keyC != "kline@BTC-USDT:5m" {
		t.Errorf("closed key: %q", keyC)
	}
}

func TestBuildStreamFrame_Depth(t *testing.T) {
	upd := &eventpb.MarketDataEvent{
		Symbol: "X-Y",
		Payload: &eventpb.MarketDataEvent_DepthUpdate{DepthUpdate: &eventpb.DepthUpdate{
			Symbol: "X-Y",
			Bids:   []*eventpb.DepthLevel{{Price: "1", Qty: "2"}},
		}},
	}
	key, _ := buildStreamFrame(upd)
	if key != "depth@X-Y" {
		t.Errorf("depth update key: %q", key)
	}
	snap := &eventpb.MarketDataEvent{
		Symbol: "X-Y",
		Payload: &eventpb.MarketDataEvent_DepthSnapshot{DepthSnapshot: &eventpb.DepthSnapshot{
			Symbol: "X-Y",
		}},
	}
	key, _ = buildStreamFrame(snap)
	if key != "depth.snapshot@X-Y" {
		t.Errorf("depth snapshot key: %q", key)
	}
}

func TestBuildStreamFrame_Unknown(t *testing.T) {
	key, payload := buildStreamFrame(&eventpb.MarketDataEvent{Symbol: "X"})
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
