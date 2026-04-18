package depth

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func update(ev *eventpb.MarketDataEvent) *eventpb.DepthUpdate {
	if ev == nil {
		return nil
	}
	p, ok := ev.Payload.(*eventpb.MarketDataEvent_DepthUpdate)
	if !ok {
		return nil
	}
	return p.DepthUpdate
}

func snapshot(ev *eventpb.MarketDataEvent) *eventpb.DepthSnapshot {
	if ev == nil {
		return nil
	}
	p, ok := ev.Payload.(*eventpb.MarketDataEvent_DepthSnapshot)
	if !ok {
		return nil
	}
	return p.DepthSnapshot
}

func TestOnOrderAccepted_AddsLevelEmitsUpdate(t *testing.T) {
	b := New("BTC-USDT")
	ev, err := b.OnOrderAccepted(1, eventpb.Side_SIDE_BUY, "100", "2")
	if err != nil {
		t.Fatalf("%v", err)
	}
	u := update(ev)
	if u == nil || len(u.Bids) != 1 || len(u.Asks) != 0 {
		t.Fatalf("update shape: %+v", ev)
	}
	if u.Bids[0].Price != "100" || u.Bids[0].Qty != "2" {
		t.Errorf("bid level: %+v", u.Bids[0])
	}
}

func TestOnOrderAccepted_MergesSamePrice(t *testing.T) {
	b := New("BTC-USDT")
	_, _ = b.OnOrderAccepted(1, eventpb.Side_SIDE_BUY, "100", "2")
	ev, _ := b.OnOrderAccepted(2, eventpb.Side_SIDE_BUY, "100", "3")
	u := update(ev)
	if u.Bids[0].Qty != "5" {
		t.Errorf("merged qty: %+v", u.Bids[0])
	}
}

func TestOnOrderAccepted_DuplicateIgnored(t *testing.T) {
	b := New("BTC-USDT")
	_, _ = b.OnOrderAccepted(1, eventpb.Side_SIDE_BUY, "100", "2")
	ev, err := b.OnOrderAccepted(1, eventpb.Side_SIDE_BUY, "100", "5")
	if err != nil || ev != nil {
		t.Fatalf("expected no-op on duplicate; got ev=%v err=%v", ev, err)
	}
	snap := snapshot(b.Snapshot())
	if snap.Bids[0].Qty != "2" {
		t.Errorf("qty must not be re-added: %+v", snap.Bids[0])
	}
}

func TestOnTrade_ReducesMakerLevel(t *testing.T) {
	b := New("BTC-USDT")
	_, _ = b.OnOrderAccepted(1, eventpb.Side_SIDE_SELL, "101", "5") // maker ask
	ev, err := b.OnTrade(1, "2")
	if err != nil {
		t.Fatalf("%v", err)
	}
	u := update(ev)
	if u.Asks[0].Price != "101" || u.Asks[0].Qty != "3" {
		t.Errorf("level after trade: %+v", u.Asks[0])
	}
}

func TestOnTrade_RemovesLevelWhenDepleted(t *testing.T) {
	b := New("BTC-USDT")
	_, _ = b.OnOrderAccepted(1, eventpb.Side_SIDE_SELL, "101", "2")
	ev, err := b.OnTrade(1, "2")
	if err != nil {
		t.Fatalf("%v", err)
	}
	u := update(ev)
	if u.Asks[0].Qty != "0" {
		t.Errorf("expected qty=0 delete marker, got %+v", u.Asks[0])
	}
	snap := snapshot(b.Snapshot())
	if len(snap.Asks) != 0 {
		t.Errorf("asks should be empty: %+v", snap)
	}
}

func TestOnTrade_UnknownMakerIsNoop(t *testing.T) {
	b := New("BTC-USDT")
	ev, err := b.OnTrade(999, "1")
	if err != nil || ev != nil {
		t.Fatalf("expected no-op: ev=%v err=%v", ev, err)
	}
}

func TestOnOrderClosed_RemovesRemaining(t *testing.T) {
	b := New("BTC-USDT")
	_, _ = b.OnOrderAccepted(1, eventpb.Side_SIDE_BUY, "99", "4")
	_, _ = b.OnTrade(1, "1")
	ev, err := b.OnOrderClosed(1)
	if err != nil {
		t.Fatalf("%v", err)
	}
	u := update(ev)
	if u.Bids[0].Qty != "0" {
		t.Errorf("level after close should be 0, got %+v", u.Bids[0])
	}
}

func TestOnOrderClosed_UnknownIsNoop(t *testing.T) {
	b := New("BTC-USDT")
	ev, err := b.OnOrderClosed(42)
	if err != nil || ev != nil {
		t.Fatalf("expected no-op: ev=%v err=%v", ev, err)
	}
}

func TestSnapshot_OrdersBidsDescAsksAsc(t *testing.T) {
	b := New("BTC-USDT")
	_, _ = b.OnOrderAccepted(1, eventpb.Side_SIDE_BUY, "100", "1")
	_, _ = b.OnOrderAccepted(2, eventpb.Side_SIDE_BUY, "99.5", "2")
	_, _ = b.OnOrderAccepted(3, eventpb.Side_SIDE_BUY, "101", "3")
	_, _ = b.OnOrderAccepted(4, eventpb.Side_SIDE_SELL, "102", "1")
	_, _ = b.OnOrderAccepted(5, eventpb.Side_SIDE_SELL, "101.5", "2")
	_, _ = b.OnOrderAccepted(6, eventpb.Side_SIDE_SELL, "103", "3")

	snap := snapshot(b.Snapshot())
	if snap == nil {
		t.Fatalf("nil snapshot")
	}
	// Bids descending: 101, 100, 99.5
	if snap.Bids[0].Price != "101" || snap.Bids[1].Price != "100" || snap.Bids[2].Price != "99.5" {
		t.Errorf("bid order: %+v", snap.Bids)
	}
	// Asks ascending: 101.5, 102, 103
	if snap.Asks[0].Price != "101.5" || snap.Asks[1].Price != "102" || snap.Asks[2].Price != "103" {
		t.Errorf("ask order: %+v", snap.Asks)
	}
}

func TestOnOrderAccepted_UnspecifiedSideError(t *testing.T) {
	b := New("BTC-USDT")
	if _, err := b.OnOrderAccepted(1, eventpb.Side_SIDE_UNSPECIFIED, "100", "1"); err == nil {
		t.Fatal("expected error")
	}
}

func TestOnOrderAccepted_ZeroRemainingIsNoop(t *testing.T) {
	b := New("BTC-USDT")
	ev, err := b.OnOrderAccepted(1, eventpb.Side_SIDE_BUY, "100", "0")
	if err != nil || ev != nil {
		t.Fatalf("expected no-op: ev=%v err=%v", ev, err)
	}
}
