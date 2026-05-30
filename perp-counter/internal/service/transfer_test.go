package service

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

func marginEvents(jr *fakeJournal, kind eventpb.PerpMarginEvent_Kind) int {
	return jr.count(func(e *eventpb.PerpJournalEvent) bool {
		m := e.GetMargin()
		return m != nil && m.GetKind() == kind
	})
}

func TestFuturesTransferIn_CreditsAndJournals(t *testing.T) {
	svc, eng, _, jr := newSvc()
	r := svc.FuturesTransferIn("u1", "tx1", "USDT", dec.New("500"))
	if r.Status != TransferConfirmed {
		t.Fatalf("status = %d, want confirmed", r.Status)
	}
	eqd(t, eng.WalletOf("u1").Available, "500", "wallet credited")
	eqd(t, r.AvailableAfter, "500", "result available")
	if marginEvents(jr, eventpb.PerpMarginEvent_KIND_TRANSFER_IN) != 1 {
		t.Fatal("expected one TRANSFER_IN margin journal event")
	}
}

func TestFuturesTransfer_Idempotent(t *testing.T) {
	svc, eng, _, jr := newSvc()
	svc.FuturesTransferIn("u1", "tx1", "USDT", dec.New("500"))
	r2 := svc.FuturesTransferIn("u1", "tx1", "USDT", dec.New("500")) // same id
	if r2.Status != TransferDuplicated {
		t.Fatalf("repeat transfer status = %d, want duplicated", r2.Status)
	}
	eqd(t, eng.WalletOf("u1").Available, "500", "no double credit")
	if marginEvents(jr, eventpb.PerpMarginEvent_KIND_TRANSFER_IN) != 1 {
		t.Fatal("duplicate must not emit a second journal event")
	}
}

func TestFuturesTransferOut_DebitsAndRejectsInsufficient(t *testing.T) {
	svc, eng, _, _ := newSvc()
	svc.FuturesTransferIn("u1", "in", "USDT", dec.New("100"))
	r := svc.FuturesTransferOut("u1", "out1", "USDT", dec.New("30"))
	if r.Status != TransferConfirmed {
		t.Fatalf("out status = %d, want confirmed", r.Status)
	}
	eqd(t, eng.WalletOf("u1").Available, "70", "debited")

	r2 := svc.FuturesTransferOut("u1", "out2", "USDT", dec.New("1000"))
	if r2.Status != TransferRejected || r2.RejectReason != "insufficient_available" {
		t.Fatalf("over-withdraw should reject, got status=%d reason=%s", r2.Status, r2.RejectReason)
	}
	eqd(t, eng.WalletOf("u1").Available, "70", "balance unchanged on reject")
}

func TestFuturesTransferOut_RejectIsCachedByID(t *testing.T) {
	svc, eng, _, _ := newSvc()
	svc.FuturesTransferIn("u1", "in", "USDT", dec.New("10"))
	r1 := svc.FuturesTransferOut("u1", "outX", "USDT", dec.New("50")) // reject (insufficient)
	if r1.Status != TransferRejected {
		t.Fatalf("first out status = %d, want rejected", r1.Status)
	}
	svc.FuturesTransferIn("u1", "in2", "USDT", dec.New("1000")) // now plenty
	r2 := svc.FuturesTransferOut("u1", "outX", "USDT", dec.New("50"))
	if r2.Status != TransferDuplicated {
		t.Fatalf("same id must return the cached outcome, got %d", r2.Status)
	}
	eqd(t, eng.WalletOf("u1").Available, "1010", "cached-reject retry must not move funds")
}

func TestFuturesCompensate_CreditsBack(t *testing.T) {
	svc, eng, _, _ := newSvc()
	svc.FuturesTransferIn("u1", "in", "USDT", dec.New("100"))
	svc.FuturesTransferOut("u1", "out1", "USDT", dec.New("100")) // wallet → 0
	r := svc.FuturesCompensateTransferOut("u1", "out1-comp", "USDT", dec.New("100"))
	if r.Status != TransferConfirmed {
		t.Fatalf("compensate status = %d, want confirmed", r.Status)
	}
	eqd(t, eng.WalletOf("u1").Available, "100", "compensate credited the amount back")
}

func TestFuturesTransfer_DedupSurvivesSnapshot(t *testing.T) {
	svc, _, _, _ := newSvc()
	svc.FuturesTransferIn("u1", "tx1", "USDT", dec.New("500"))
	engSnap, _, err := svc.Capture(nil)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	eng2 := engine.New()
	eng2.Restore(engSnap)
	svc2 := New(eng2, &fakeDispatcher{}, &fakeJournal{}, func() uint64 { return 0 },
		Config{ProducerID: "p"})
	r := svc2.FuturesTransferIn("u1", "tx1", "USDT", dec.New("500")) // dup after restore
	if r.Status != TransferDuplicated {
		t.Fatalf("dedup must survive snapshot, got status %d", r.Status)
	}
	eqd(t, eng2.WalletOf("u1").Available, "500", "no double credit after restore")
}
