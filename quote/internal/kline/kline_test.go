package kline

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func oneMinOnly() []IntervalSpec {
	return []IntervalSpec{{eventpb.KlineInterval_KLINE_INTERVAL_1M, 60_000}}
}

// Helper: extract the single Kline payload from a MarketDataEvent. Returns
// nil if it is not a Kline update/closed.
func klineOf(ev *eventpb.MarketDataEvent) (*eventpb.Kline, bool /*closed*/) {
	switch p := ev.Payload.(type) {
	case *eventpb.MarketDataEvent_KlineUpdate:
		return p.KlineUpdate.Kline, false
	case *eventpb.MarketDataEvent_KlineClosed:
		return p.KlineClosed.Kline, true
	default:
		return nil, false
	}
}

func TestOnTrade_FirstTradeOpensBarAndEmitsUpdate(t *testing.T) {
	a := New("BTC-USDT", oneMinOnly())
	evs, err := a.OnTrade("100", "2", 123_000) // 123_000 ms → bucket 120_000
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("events: %d", len(evs))
	}
	k, closed := klineOf(evs[0])
	if closed {
		t.Fatalf("expected KlineUpdate, got closed")
	}
	if k.OpenTimeMs != 120_000 || k.CloseTimeMs != 180_000 {
		t.Errorf("bucket bounds: %+v", k)
	}
	if k.Open != "100" || k.High != "100" || k.Low != "100" || k.Close != "100" {
		t.Errorf("OHLC: %+v", k)
	}
	if k.Volume != "2" || k.QuoteVolume != "200" || k.TradeCount != 1 {
		t.Errorf("volumes/count: %+v", k)
	}
}

func TestOnTrade_SameBarAggregates(t *testing.T) {
	a := New("BTC-USDT", oneMinOnly())
	_, _ = a.OnTrade("100", "1", 120_001)
	_, _ = a.OnTrade("105", "2", 140_000)
	evs, err := a.OnTrade("98", "3", 175_000)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(evs) != 1 {
		t.Fatalf("events: %d", len(evs))
	}
	k, closed := klineOf(evs[0])
	if closed {
		t.Fatalf("unexpected closed")
	}
	if k.Open != "100" {
		t.Errorf("open: %s", k.Open)
	}
	if k.High != "105" || k.Low != "98" || k.Close != "98" {
		t.Errorf("HLC: %+v", k)
	}
	if k.Volume != "6" {
		t.Errorf("volume: %s", k.Volume)
	}
	if k.QuoteVolume != "604" { // 100*1 + 105*2 + 98*3 = 100+210+294
		t.Errorf("quote_volume: %s", k.QuoteVolume)
	}
	if k.TradeCount != 3 {
		t.Errorf("count: %d", k.TradeCount)
	}
}

func TestOnTrade_BarRollClosesAndOpens(t *testing.T) {
	a := New("BTC-USDT", oneMinOnly())
	_, _ = a.OnTrade("100", "1", 120_000) // opens bar 120_000
	evs, err := a.OnTrade("110", "2", 190_000) // bucket 180_000 — new bar
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(evs) != 2 {
		t.Fatalf("events: %d", len(evs))
	}
	// First is the closed previous bar.
	cl, closed := klineOf(evs[0])
	if !closed {
		t.Fatalf("expected closed first, got update")
	}
	if cl.OpenTimeMs != 120_000 || cl.CloseTimeMs != 180_000 {
		t.Errorf("closed bounds: %+v", cl)
	}
	if cl.Close != "100" || cl.Volume != "1" {
		t.Errorf("closed payload: %+v", cl)
	}
	// Second is the new open bar update.
	up, closedB := klineOf(evs[1])
	if closedB {
		t.Fatalf("expected update second, got closed")
	}
	if up.OpenTimeMs != 180_000 || up.Open != "110" || up.High != "110" || up.Close != "110" || up.Volume != "2" {
		t.Errorf("new bar payload: %+v", up)
	}
}

func TestOnTrade_OutOfOrderIsDropped(t *testing.T) {
	a := New("BTC-USDT", oneMinOnly())
	_, _ = a.OnTrade("100", "1", 180_000)
	evs, err := a.OnTrade("99", "1", 120_000) // ts before current open
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(evs) != 0 {
		t.Fatalf("expected no events for out-of-order, got %d", len(evs))
	}
}

func TestOnTrade_MultiIntervalEmitsOnePerInterval(t *testing.T) {
	a := New("BTC-USDT", []IntervalSpec{
		{eventpb.KlineInterval_KLINE_INTERVAL_1M, 60_000},
		{eventpb.KlineInterval_KLINE_INTERVAL_5M, 300_000},
	})
	evs, err := a.OnTrade("100", "1", 120_000)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(evs) != 2 {
		t.Fatalf("expected 2 events (one per interval), got %d", len(evs))
	}
	intervals := map[eventpb.KlineInterval]bool{}
	for _, ev := range evs {
		k, _ := klineOf(ev)
		intervals[k.Interval] = true
	}
	if !intervals[eventpb.KlineInterval_KLINE_INTERVAL_1M] ||
		!intervals[eventpb.KlineInterval_KLINE_INTERVAL_5M] {
		t.Fatalf("intervals: %+v", intervals)
	}
}

func TestOnTrade_BadInputsReturnError(t *testing.T) {
	a := New("X", oneMinOnly())
	if _, err := a.OnTrade("bad", "1", 1); err == nil {
		t.Error("bad price should error")
	}
	if _, err := a.OnTrade("1", "bad", 1); err == nil {
		t.Error("bad qty should error")
	}
	if _, err := a.OnTrade("0", "1", 1); err == nil {
		t.Error("zero price should error")
	}
	if _, err := a.OnTrade("1", "-1", 1); err == nil {
		t.Error("negative qty should error")
	}
	if _, err := a.OnTrade("1", "1", 0); err == nil {
		t.Error("ts<=0 should error")
	}
}
