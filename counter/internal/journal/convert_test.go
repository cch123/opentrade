package journal

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

func TestBuildTransferEvent(t *testing.T) {
	in := TransferEventInput{
		CounterSeqID: 17,
		TsUnixMS:     1000,
		TraceID:      "trace-1",
		ProducerID:   "counter-shard-0-main",
		Req: engine.TransferRequest{
			TransferID: "tx-1",
			UserID:     "u1",
			Asset:      "USDT",
			Amount:     dec.New("100.5"),
			Type:       engine.TransferDeposit,
			BizRefID:   "chain-hash-1",
			Memo:       "initial deposit",
		},
		BalanceAfter: engine.Balance{
			Available: dec.New("100.5"),
			Frozen:    dec.Zero,
		},
	}
	cje, err := BuildTransferEvent(in)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if cje.CounterSeqId != 17 || cje.Meta.ProducerId != "counter-shard-0-main" || cje.Meta.TraceId != "trace-1" {
		t.Fatalf("meta = %+v counter_seq_id = %d", cje.Meta, cje.CounterSeqId)
	}
	te := cje.GetTransfer()
	if te == nil {
		t.Fatalf("expected Transfer payload, got %T", cje.Payload)
	}
	if te.UserId != "u1" || te.Asset != "USDT" || te.Amount != "100.5" {
		t.Fatalf("payload = %+v", te)
	}
	if te.Type != eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT {
		t.Fatalf("type = %v", te.Type)
	}
	if te.BalanceAfter.Available != "100.5" || te.BalanceAfter.Frozen != "0" {
		t.Fatalf("balance_after = %+v", te.BalanceAfter)
	}
}

func TestTransferTypeCoverage(t *testing.T) {
	cases := []struct {
		in  engine.TransferType
		out eventpb.TransferEvent_TransferType
	}{
		{engine.TransferDeposit, eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT},
		{engine.TransferWithdraw, eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW},
		{engine.TransferFreeze, eventpb.TransferEvent_TRANSFER_TYPE_FREEZE},
		{engine.TransferUnfreeze, eventpb.TransferEvent_TRANSFER_TYPE_UNFREEZE},
	}
	for _, c := range cases {
		got, err := transferTypeToProto(c.in)
		if err != nil {
			t.Fatalf("%v: %v", c.in, err)
		}
		if got != c.out {
			t.Fatalf("%v: got %v want %v", c.in, got, c.out)
		}
	}
	if _, err := transferTypeToProto(99); err == nil {
		t.Fatal("expected error for unknown type")
	}
}
