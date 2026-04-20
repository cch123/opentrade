package engine

import (
	"reflect"
	"testing"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/quote/internal/kline"
)

// TestHandleRecord_AdvancesOffsets verifies that the per-partition offset
// watermark moves together with state mutation so a post-crash restore
// skips already-applied records. Trade events drive Kline aggregation,
// which is the only state Quote still maintains post ADR-0055.
func TestHandleRecord_AdvancesOffsets(t *testing.T) {
	e := newTestEngine(t)
	e.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_000},
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId: "t1", Symbol: "BTC-USDT", Price: "100", Qty: "1",
			MakerOrderId: 1, TakerOrderId: 2, TakerSide: eventpb.Side_SIDE_BUY,
		}},
	}, 2, 41)
	e.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_500},
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId: "t2", Symbol: "BTC-USDT", Price: "101", Qty: "1",
			MakerOrderId: 3, TakerOrderId: 4, TakerSide: eventpb.Side_SIDE_SELL,
		}},
	}, 3, 99)

	got := e.Offsets()
	want := map[int32]int64{2: 42, 3: 100} // next-to-consume
	if !reflect.DeepEqual(got, want) {
		t.Errorf("offsets = %+v, want %+v", got, want)
	}
}

// TestCaptureRestore_RoundTrip seeds the engine with trades that build up
// an open Kline bar, captures, restores into a fresh engine, and verifies
// the kline state came back intact.
func TestCaptureRestore_RoundTrip(t *testing.T) {
	src := newTestEngine(t)
	src.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_000},
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId: "t1", Symbol: "BTC-USDT", Price: "100", Qty: "1",
			MakerOrderId: 1, TakerOrderId: 2, TakerSide: eventpb.Side_SIDE_BUY,
		}},
	}, 0, 0)
	src.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 121_000},
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId: "t2", Symbol: "BTC-USDT", Price: "103", Qty: "2",
			MakerOrderId: 3, TakerOrderId: 4, TakerSide: eventpb.Side_SIDE_SELL,
		}},
	}, 0, 1)

	snap := src.Capture()
	if snap.Offsets[0] != 2 {
		t.Fatalf("src offset after 2 records on p0 = %d, want 2", snap.Offsets[0])
	}
	if bt, ok := snap.Symbols["BTC-USDT"]; !ok || bt.Kline == nil {
		t.Fatal("missing BTC-USDT kline in snapshot.Symbols")
	}

	dst := New(Config{
		ProducerID: "quote-test",
		Intervals:  []kline.IntervalSpec{{Interval: eventpb.KlineInterval_KLINE_INTERVAL_1M, Millis: 60_000}},
		Clock:      func() int64 { return 1_700_000_000_000 },
	}, zap.NewNop())
	if err := dst.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}

	snapAfter := dst.Capture()
	if bt := snapAfter.Symbols["BTC-USDT"]; bt == nil || bt.Kline == nil {
		t.Fatal("missing kline after restore")
	}
	if snapAfter.Offsets[0] != 2 {
		t.Errorf("dst offset = %d, want 2", snapAfter.Offsets[0])
	}
}

// TestRestore_NilIsNoop allows main to pass a nil snapshot (cold start) and
// keep a pristine engine.
func TestRestore_NilIsNoop(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Restore(nil); err != nil {
		t.Fatalf("Restore(nil) failed: %v", err)
	}
	if len(e.Offsets()) != 0 {
		t.Errorf("expected empty offsets after nil restore")
	}
}

// TestRestore_VersionMismatch guards the JSON schema.
func TestRestore_VersionMismatch(t *testing.T) {
	e := newTestEngine(t)
	src := e.Capture()
	src.Version = 9999
	if err := e.Restore(src); err == nil {
		t.Fatal("expected version mismatch error")
	}
}
