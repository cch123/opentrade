package journal

import (
	"testing"

	"github.com/xargin/opentrade/asset/internal/engine"
)

func TestApplyFundingEvent_RebuildsBalancesAndRing(t *testing.T) {
	state := engine.NewState()

	inRes, err := state.ApplyTransferIn(engine.TransferRequest{
		UserID: "u1", TransferID: "fund-in", Asset: "USDT", Amount: mustDec(t, "100"),
	})
	if err != nil {
		t.Fatalf("seed transfer in: %v", err)
	}
	inEvt := Build(BuildInput{
		Kind:           KindTransferIn,
		AssetSeqID:     1,
		FundingVersion: inRes.FundingVersion,
		UserID:         "u1",
		TransferID:     "fund-in",
		Asset:          "USDT",
		Amount:         "100",
		BalanceAfter:   inRes.BalanceAfter,
	})
	outRes, err := state.ApplyTransferOut(engine.TransferRequest{
		UserID: "u1", TransferID: "fund-out", Asset: "USDT", Amount: mustDec(t, "40"),
	})
	if err != nil {
		t.Fatalf("seed transfer out: %v", err)
	}
	outEvt := Build(BuildInput{
		Kind:           KindTransferOut,
		AssetSeqID:     2,
		FundingVersion: outRes.FundingVersion,
		UserID:         "u1",
		TransferID:     "fund-out",
		Asset:          "USDT",
		Amount:         "40",
		BalanceAfter:   outRes.BalanceAfter,
	})

	restored := engine.NewState()
	if err := ApplyFundingEvent(restored, inEvt); err != nil {
		t.Fatalf("apply in: %v", err)
	}
	if err := ApplyFundingEvent(restored, outEvt); err != nil {
		t.Fatalf("apply out: %v", err)
	}

	bal := restored.Account("u1").Balance("USDT")
	if bal.Available.String() != "60" || bal.Version != 2 {
		t.Fatalf("restored balance = %+v, want available=60 version=2", bal)
	}

	dup, err := restored.ApplyTransferOut(engine.TransferRequest{
		UserID: "u1", TransferID: "fund-out", Asset: "USDT", Amount: mustDec(t, "40"),
	})
	if err != nil {
		t.Fatalf("duplicate after replay: %v", err)
	}
	if !dup.Duplicated {
		t.Fatal("replayed transfer_id was not restored into the idempotency ring")
	}
	if got := restored.Account("u1").Balance("USDT").Available.String(); got != "60" {
		t.Fatalf("duplicate changed balance to %s", got)
	}
}
