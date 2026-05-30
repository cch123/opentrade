package service

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// restoreInto builds a fresh service/engine pair and restores both halves of a
// captured image into it (mirrors what main.go does on startup).
func restoreInto(engSnap engine.Snapshot, svcSnap Snapshot, cfg Config) (*Service, *engine.Engine) {
	eng := engine.New()
	eng.Restore(engSnap)
	svc := New(eng, &fakeDispatcher{}, &fakeJournal{}, func() uint64 { return 0 }, cfg)
	svc.Restore(svcSnap)
	return svc, eng
}

func TestSnapshot_OrderAndOffsetRoundTrip(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit("u1", dec.New("1000"))
	r, _ := svc.PlaceOrder(placeReq("u1", perpSym, eventpb.Side_SIDE_BUY, "100", "2", "10", false))
	svc.HandleTradeEvent(acceptedEvt(1, "u1", r.OrderId), 3, 70) // NEW + offset[3]=71

	engSnap, svcSnap, err := svc.Capture(nil)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	svc2, eng2 := restoreInto(engSnap, svcSnap, Config{MaxLeverage: dec.New("100"), ProducerID: "p"})

	q, ok := svc2.QueryOrder(queryReq("u1", r.OrderId))
	if !ok {
		t.Fatal("order not restored")
	}
	if q.Status != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW {
		t.Fatalf("restored status = %v, want NEW", q.GetStatus())
	}
	if q.Qty != "2" || q.Symbol != perpSym {
		t.Fatalf("restored order fields wrong: qty=%s symbol=%s", q.Qty, q.Symbol)
	}
	if got := svc2.ConsumedOffsets()[3]; got != 71 {
		t.Fatalf("restored offset[3] = %d, want 71", got)
	}
	// Wallet (reserved IM) survived via the engine half.
	if w := eng2.WalletOf("u1"); w.Reserved.String() != "20" {
		t.Fatalf("restored reserved = %s, want 20", w.Reserved)
	}
}

func TestSnapshot_SeqCountersRoundTrip(t *testing.T) {
	svc, eng, _, _ := newSvc()
	eng.Deposit("u1", dec.New("1000"))
	// Two placements advance both the order-event and perp-journal sequences.
	svc.PlaceOrder(placeReq("u1", perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	svc.PlaceOrder(placeReq("u1", perpSym, eventpb.Side_SIDE_BUY, "100", "1", "10", false))
	_, svcSnap, _ := svc.Capture(nil)
	if svcSnap.OrderSeq == 0 || svcSnap.PerpSeq == 0 {
		t.Fatalf("sequences not captured: order=%d perp=%d", svcSnap.OrderSeq, svcSnap.PerpSeq)
	}
	svc2, _ := restoreInto(engine.Snapshot{}, svcSnap, Config{MaxLeverage: dec.New("100"), ProducerID: "p"})
	if got := svc2.nextOrderSeq(); got != svcSnap.OrderSeq+1 {
		t.Fatalf("order seq did not resume: got %d, want %d", got, svcSnap.OrderSeq+1)
	}
	if got := svc2.nextPerpSeq(); got != svcSnap.PerpSeq+1 {
		t.Fatalf("perp seq did not resume: got %d, want %d", got, svcSnap.PerpSeq+1)
	}
}

func TestSnapshot_InFlightLiquidationRoundTrip(t *testing.T) {
	svc, eng, disp, _ := newLiqSvc()
	bankID := triggerLiquidation(t, svc, eng, disp)

	engSnap, svcSnap, err := svc.Capture(nil)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	liqCfg := Config{MaxLeverage: dec.New("100"), MMR: dec.New("0.05"), ProducerID: "p"}
	svc2, eng2 := restoreInto(engSnap, svcSnap, liqCfg)

	if !svc2.hasLiquidation(liqKey("u1", perpSym)) {
		t.Fatal("in-flight liquidation guard not restored")
	}
	if svc2.liquidationFor(bankID) == nil {
		t.Fatal("bankruptcy order → liquidation mapping not restored")
	}
	if _, ok := eng2.PositionOf("u1", perpSym); !ok {
		t.Fatal("liquidated position not restored")
	}
	// The restored bankruptcy order can still settle to insurance.
	liqFill(svc2, bankID, "90", "1", 5)
	if _, ok := eng2.PositionOf("u1", perpSym); ok {
		t.Fatal("position should close on the post-restore liquidation fill")
	}
}

func TestSnapshot_CaptureFlushErrorAborts(t *testing.T) {
	svc, _, _, _ := newSvc()
	boom := func() error { return errSentinel }
	if _, _, err := svc.Capture(boom); err != errSentinel {
		t.Fatalf("capture should surface the flush error, got %v", err)
	}
}

var errSentinel = sentinelErr("flush boom")

type sentinelErr string

func (e sentinelErr) Error() string { return string(e) }
