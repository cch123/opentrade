package index

import (
	"testing"
	"time"

	"github.com/xargin/opentrade/pkg/dec"
)

func testCfg() Config {
	return Config{
		Quorum:        2,
		SourceMaxAge:  5 * time.Second,
		DeviationBand: dec.New("0.05"),
		Sources: []SourceConfig{
			{Name: "self:BTC-USDT", Weight: dec.New("1"), Self: true},
			{Name: "binance:BTCUSDT", Weight: dec.New("2")},
			{Name: "okx:BTC-USDT", Weight: dec.New("1")},
		},
	}
}

func snap(ts int64, qs ...SourceQuote) []SourceQuote {
	for i := range qs {
		if qs[i].TsUnixMs == 0 {
			qs[i].TsUnixMs = ts
		}
		qs[i].OK = true
	}
	return qs
}

func q(name, price, weight string, self bool) SourceQuote {
	return SourceQuote{
		SourceConfig: SourceConfig{Name: name, Weight: dec.New(weight), Self: self},
		Price:        dec.New(price),
	}
}

func TestCompositeWeightedAverageRenormalizesSurvivors(t *testing.T) {
	res := Composite(snap(1000,
		q("self:BTC-USDT", "100", "1", true),
		q("binance:BTCUSDT", "101", "2", false),
	), testCfg(), 1000)
	if res.Stale {
		t.Fatal("expected fresh composite at quorum")
	}
	// (100*1 + 101*2) / (1+2) = 100.666...
	if got := res.Index.StringFixed(8); got != "100.66666667" {
		t.Fatalf("index = %s, want 100.66666667", got)
	}
}

func TestCompositeStaleWhenBelowQuorum(t *testing.T) {
	cfg := testCfg()
	res := Composite(snap(1000, q("self:BTC-USDT", "100", "1", true)), cfg, 1000)
	if !res.Stale || res.LiveCount != 1 {
		t.Fatalf("one live source with quorum=2 should be stale: %+v", res)
	}
}

func TestCompositeMaxAgeBoundary(t *testing.T) {
	cfg := testCfg()
	res := Composite(snap(1000,
		q("self:BTC-USDT", "100", "1", true),
		q("binance:BTCUSDT", "101", "1", false),
	), cfg, 6000)
	if res.Stale {
		t.Fatalf("age exactly maxAge should still be live: %+v", res)
	}
	res = Composite(snap(1000,
		q("self:BTC-USDT", "100", "1", true),
		q("binance:BTCUSDT", "101", "1", false),
	), cfg, 6001)
	if !res.Stale {
		t.Fatalf("age over maxAge should be stale: %+v", res)
	}
}

func TestCompositeOutlierFilterRequiresThreeSources(t *testing.T) {
	cfg := testCfg()
	two := Composite(snap(1000,
		q("self:BTC-USDT", "100", "1", true),
		q("binance:BTCUSDT", "150", "1", false),
	), cfg, 1000)
	if two.DroppedCount != 0 || two.UsedCount != 2 {
		t.Fatalf("two sources cannot do median-band filtering: %+v", two)
	}

	three := Composite(snap(1000,
		q("self:BTC-USDT", "100", "1", true),
		q("binance:BTCUSDT", "101", "1", false),
		q("okx:BTC-USDT", "150", "1", false),
	), cfg, 1000)
	if three.DroppedCount != 1 || three.UsedCount != 2 {
		t.Fatalf("third source should let the 150 outlier drop: %+v", three)
	}
	if got := three.Index.String(); got != "100.5" {
		t.Fatalf("filtered index = %s, want 100.5", got)
	}
}

func TestCompositeDegradedWhenOnlySelfSurvivesQuorum(t *testing.T) {
	cfg := Config{
		Quorum:        1,
		SourceMaxAge:  time.Second,
		DeviationBand: dec.New("0.05"),
		Sources:       []SourceConfig{{Name: "self:BTC-USDT", Weight: dec.New("1"), Self: true}},
	}
	res := Composite(snap(1000, q("self:BTC-USDT", "100", "1", true)), cfg, 1000)
	if res.Stale || !res.Degraded {
		t.Fatalf("self-only fresh result should be degraded, got %+v", res)
	}
}

func TestEvaluatorFreezesLastGoodOnStale(t *testing.T) {
	ev, err := NewEvaluator(testCfg())
	if err != nil {
		t.Fatal(err)
	}
	fresh := ev.Eval(time.UnixMilli(1000), snap(1000,
		q("self:BTC-USDT", "100", "1", true),
		q("binance:BTCUSDT", "102", "1", false),
	))
	if fresh.Stale || !fresh.HasIndex {
		t.Fatalf("expected first fresh result, got %+v", fresh)
	}
	stale := ev.Eval(time.UnixMilli(7001), snap(1000,
		q("self:BTC-USDT", "100", "1", true),
		q("binance:BTCUSDT", "102", "1", false),
	))
	if !stale.Stale || !stale.HasIndex || stale.Index.Cmp(fresh.Index) != 0 {
		t.Fatalf("stale should carry last-good index %s, got %+v", fresh.Index, stale)
	}
}

func TestSourceBookSnapshotKeepsMissingSources(t *testing.T) {
	book := NewSourceBook()
	book.Upsert("self:BTC-USDT", dec.New("100"), 123)
	out := book.Snapshot(testCfg().Sources)
	if len(out) != 3 {
		t.Fatalf("snapshot len = %d, want configured source count", len(out))
	}
	if !out[0].OK || out[1].OK {
		t.Fatalf("snapshot should mark only upserted source OK: %+v", out)
	}
}
