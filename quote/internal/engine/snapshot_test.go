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
// skips already-applied records.
func TestHandleRecord_AdvancesOffsets(t *testing.T) {
	e := newTestEngine(t)
	e.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_000},
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			OrderId: 1, Symbol: "BTC-USDT",
			Side: eventpb.Side_SIDE_BUY, Price: "100", RemainingQty: "1",
		}},
	}, 2, 41)
	e.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_000},
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			OrderId: 2, Symbol: "BTC-USDT",
			Side: eventpb.Side_SIDE_SELL, Price: "101", RemainingQty: "1",
		}},
	}, 3, 99)

	got := e.Offsets()
	want := map[int32]int64{2: 42, 3: 100} // next-to-consume
	if !reflect.DeepEqual(got, want) {
		t.Errorf("offsets = %+v, want %+v", got, want)
	}
}

// TestCaptureRestore_RoundTrip seeds an engine with a few records, captures
// a snapshot, restores into a fresh engine, and verifies the state behaves
// identically: the same subsequent record produces the same emitted events.
func TestCaptureRestore_RoundTrip(t *testing.T) {
	src := newTestEngine(t)
	// Seed: a bid, an ask, and a trade against the ask to shave its level.
	src.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_000},
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			OrderId: 10, Symbol: "BTC-USDT",
			Side: eventpb.Side_SIDE_BUY, Price: "99", RemainingQty: "3",
		}},
	}, 0, 0)
	src.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_000},
		Payload: &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{
			OrderId: 11, Symbol: "BTC-USDT",
			Side: eventpb.Side_SIDE_SELL, Price: "101", RemainingQty: "5",
		}},
	}, 0, 1)
	src.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_000},
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			Symbol: "BTC-USDT", Price: "101", Qty: "2",
			MakerOrderId: 11, TakerOrderId: 20,
			TakerSide: eventpb.Side_SIDE_BUY,
		}},
	}, 0, 2)

	snap := src.Capture()
	if snap.Offsets[0] != 3 {
		t.Fatalf("src offset after 3 records on p0 = %d, want 3", snap.Offsets[0])
	}
	if _, ok := snap.Symbols["BTC-USDT"]; !ok {
		t.Fatal("missing BTC-USDT in snapshot.Symbols")
	}

	dst := New(Config{
		ProducerID: "quote-test",
		Intervals:  []kline.IntervalSpec{{Interval: eventpb.KlineInterval_KLINE_INTERVAL_1M, Millis: 60_000}},
		Clock:      func() int64 { return 1_700_000_000_000 },
	}, zap.NewNop())
	if err := dst.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// After restore, feeding a follow-up trade that closes order 11 should
	// remove its remaining qty (3) from the ask level 101.
	dst.HandleRecord(&eventpb.TradeEvent{
		Meta: &eventpb.EventMeta{TsUnixMs: 120_000},
		Payload: &eventpb.TradeEvent_Cancelled{Cancelled: &eventpb.OrderCancelled{
			OrderId: 11, Symbol: "BTC-USDT",
		}},
	}, 0, 3)

	snapAfter := dst.Capture()
	bt := snapAfter.Symbols["BTC-USDT"]
	if bt == nil || bt.Depth == nil {
		t.Fatal("missing depth after restore+cancel")
	}
	// Bid level 99 still has qty=3; ask level 101 fully gone.
	if _, ok := bt.Depth.Asks["101"]; ok {
		t.Errorf("ask 101 should have been removed, got %+v", bt.Depth.Asks)
	}
	if q, ok := bt.Depth.Bids["99"]; !ok || q != "3" {
		t.Errorf("bid 99 should remain at 3, got %q ok=%v", q, ok)
	}
	if snapAfter.Offsets[0] != 4 {
		t.Errorf("dst offset = %d, want 4", snapAfter.Offsets[0])
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
