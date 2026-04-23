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

func TestBuildSettlementEventCarriesReplayFields(t *testing.T) {
	state := engine.NewShardState(0)
	state.Orders().RestoreInsert(&engine.Order{
		ID:           77,
		UserID:       "u1",
		Symbol:       "BTC-USDT",
		Side:         engine.SideBid,
		Type:         engine.OrderTypeLimit,
		Price:        dec.New("50000"),
		Qty:          dec.New("1"),
		FrozenAsset:  "USDT",
		FrozenAmount: dec.New("50000"),
		Status:       engine.OrderStatusNew,
	})
	evt, err := BuildSettlementEvent(SettlementEventInput{
		CounterSeqID:   18,
		ProducerID:     "counter-shard-0-main",
		AccountVersion: 3,
		Symbol:         "BTC-USDT",
		TradeID:        "trade-1",
		Side:           engine.SideBid,
		Price:          "50000",
		Qty:            "0.1",
		Party: engine.PartySettlement{
			UserID:           "u1",
			OrderID:          77,
			BaseDelta:        dec.New("0.1"),
			QuoteDelta:       dec.New("-5000"),
			FrozenQuoteDelta: dec.New("-5000"),
			FilledQtyAfter:   dec.New("0.1"),
			StatusAfter:      engine.OrderStatusPartiallyFilled,
		},
		BaseAfter: engine.Balance{
			Available: dec.New("0.1"),
			Version:   1,
		},
		QuoteAfter: engine.Balance{
			Available: dec.New("45000"),
			Frozen:    dec.New("45000"),
			Version:   2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	settle := evt.GetSettlement()
	if settle == nil {
		t.Fatalf("payload = %T, want Settlement", evt.Payload)
	}
	if settle.Price != "50000" || settle.Qty != "0.1" || settle.Side != eventpb.Side_SIDE_BUY {
		t.Fatalf("settlement replay fields = price %q qty %q side %v", settle.Price, settle.Qty, settle.Side)
	}
	if err := engine.ApplyCounterJournalEvent(state, evt); err != nil {
		t.Fatalf("apply settlement: %v", err)
	}
	o := state.Orders().Get(77)
	if o.FilledQty.String() != "0.1" {
		t.Fatalf("filled_qty = %s, want 0.1", o.FilledQty)
	}
	if o.FrozenSpent.String() != "5000" {
		t.Fatalf("frozen_spent = %s, want 5000", o.FrozenSpent)
	}
}

func TestBuildUnfreezeEvent(t *testing.T) {
	evt := BuildUnfreezeEvent(UnfreezeEventInput{
		CounterSeqID:   19,
		ProducerID:     "counter-shard-0-main",
		AccountVersion: 4,
		UserID:         "u1",
		OrderID:        77,
		Asset:          "USDT",
		Amount:         "2500",
		BalanceAfter: engine.Balance{
			Available: dec.New("47500"),
			Frozen:    dec.New("0"),
			Version:   5,
		},
	})
	unfreeze := evt.GetUnfreeze()
	if unfreeze == nil {
		t.Fatalf("payload = %T, want Unfreeze", evt.Payload)
	}
	if unfreeze.UserId != "u1" || unfreeze.OrderId != 77 || unfreeze.Asset != "USDT" || unfreeze.Amount != "2500" {
		t.Fatalf("unfreeze = %+v", unfreeze)
	}
	if unfreeze.BalanceAfter.Available != "47500" || unfreeze.BalanceAfter.Frozen != "0" || unfreeze.BalanceAfter.Version != 5 {
		t.Fatalf("balance_after = %+v", unfreeze.BalanceAfter)
	}
}
