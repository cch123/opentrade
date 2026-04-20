package marketcache

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestOrderBook_LastWriteWins(t *testing.T) {
	c := New(Config{})
	first := &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{{Price: "99", Qty: "1"}},
	}
	second := &eventpb.OrderBookFull{
		Bids: []*eventpb.OrderBookLevel{{Price: "100", Qty: "2"}},
	}
	c.PutOrderBookFull("BTC-USDT", 1, first)
	c.PutOrderBookFull("BTC-USDT", 2, second)
	got := c.OrderBook("BTC-USDT")
	if got == nil || got.Bids[0].Price != "100" || got.Bids[0].Qty != "2" {
		t.Errorf("latest-wins broken: %+v", got)
	}
	if got.MatchSeqID != 2 {
		t.Errorf("match_seq_id: want 2 got %d", got.MatchSeqID)
	}
}

func TestOrderBook_UnknownSymbol(t *testing.T) {
	c := New(Config{})
	if got := c.OrderBook("X"); got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestOrderBook_NilInputIgnored(t *testing.T) {
	c := New(Config{})
	c.PutOrderBookFull("BTC-USDT", 1, nil)
	c.PutOrderBookFull("", 1, &eventpb.OrderBookFull{}) // empty symbol
	if len(c.books) != 0 {
		t.Errorf("expected empty book map, got %d entries", len(c.books))
	}
}

func TestKlineRing_OldestEvicted(t *testing.T) {
	c := New(Config{KlineBuffer: 3})
	for i := int64(0); i < 5; i++ {
		c.AppendKlineClosed("BTC-USDT", &eventpb.KlineClosed{Kline: &eventpb.Kline{
			Interval:   eventpb.KlineInterval_KLINE_INTERVAL_1M,
			OpenTimeMs: 60_000 * i,
		}})
	}
	// Only the most recent 3 should survive (open times 2,3,4).
	got := c.RecentKlines("BTC-USDT", eventpb.KlineInterval_KLINE_INTERVAL_1M, 10)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for i, k := range got {
		want := 60_000 * int64(i+2)
		if k.OpenTimeMs != want {
			t.Errorf("got[%d].open = %d, want %d", i, k.OpenTimeMs, want)
		}
	}
}

func TestKlineRing_LimitClamp(t *testing.T) {
	c := New(Config{KlineBuffer: 10})
	for i := int64(0); i < 4; i++ {
		c.AppendKlineClosed("BTC-USDT", &eventpb.KlineClosed{Kline: &eventpb.Kline{
			Interval: eventpb.KlineInterval_KLINE_INTERVAL_5M, OpenTimeMs: i,
		}})
	}
	if got := c.RecentKlines("BTC-USDT", eventpb.KlineInterval_KLINE_INTERVAL_5M, 2); len(got) != 2 {
		t.Errorf("limit=2 → len=%d", len(got))
	}
	if got := c.RecentKlines("BTC-USDT", eventpb.KlineInterval_KLINE_INTERVAL_5M, 100); len(got) != 4 {
		t.Errorf("limit>size → len=%d", len(got))
	}
	if got := c.RecentKlines("BTC-USDT", eventpb.KlineInterval_KLINE_INTERVAL_5M, 0); len(got) != 4 {
		t.Errorf("limit=0 should return all; got %d", len(got))
	}
}

func TestKlineRing_IntervalIsolation(t *testing.T) {
	c := New(Config{KlineBuffer: 3})
	c.AppendKlineClosed("X", &eventpb.KlineClosed{Kline: &eventpb.Kline{Interval: eventpb.KlineInterval_KLINE_INTERVAL_1M, OpenTimeMs: 1}})
	c.AppendKlineClosed("X", &eventpb.KlineClosed{Kline: &eventpb.Kline{Interval: eventpb.KlineInterval_KLINE_INTERVAL_5M, OpenTimeMs: 2}})
	if got := c.RecentKlines("X", eventpb.KlineInterval_KLINE_INTERVAL_1M, 10); len(got) != 1 || got[0].OpenTimeMs != 1 {
		t.Errorf("1m bucket: %+v", got)
	}
	if got := c.RecentKlines("X", eventpb.KlineInterval_KLINE_INTERVAL_5M, 10); len(got) != 1 || got[0].OpenTimeMs != 2 {
		t.Errorf("5m bucket: %+v", got)
	}
}
