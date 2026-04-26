package writer

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func newMeta(ts int64, producerID string) *eventpb.EventMeta {
	return &eventpb.EventMeta{TsUnixMs: ts, ProducerId: producerID}
}

// TestShardIDFromProducer covers ADR-0058's stable producer_id format
// "counter-vshard-NNN". The old shard-model format
// "counter-shard-N-<role>" is no longer emitted by counter and must
// not accidentally match (it would insert garbage shard_id into
// account_logs).
func TestShardIDFromProducer(t *testing.T) {
	cases := []struct {
		in     string
		want   int32
		wantOk bool
	}{
		{"counter-vshard-000", 0, true},
		{"counter-vshard-042", 42, true},
		{"counter-vshard-255", 255, true},
		{"counter-vshard-notanumber", 0, false},
		{"counter-vshard-", 0, false},
		// Pre-ADR-0058 legacy names must NOT parse — catches a
		// regression where a deployment mixes old / new counters.
		{"counter-shard-0-main", 0, false},
		{"match-shard-0-main", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		got, ok := shardIDFromProducer(c.in)
		if ok != c.wantOk || got != c.want {
			t.Errorf("%q: got (%d,%v) want (%d,%v)", c.in, got, ok, c.want, c.wantOk)
		}
	}
}

func TestBuildJournalBatch_Freeze(t *testing.T) {
	evt := &eventpb.CounterJournalEvent{
		Meta:         newMeta(1_700_000_000_000, "counter-vshard-000"),
		CounterSeqId: 5,
		Payload: &eventpb.CounterJournalEvent_Freeze{Freeze: &eventpb.FreezeEvent{
			UserId:        "u1",
			OrderId:       42,
			ClientOrderId: "coid-1",
			Symbol:        "BTC-USDT",
			Side:          eventpb.Side_SIDE_BUY,
			OrderType:     eventpb.OrderType_ORDER_TYPE_LIMIT,
			Tif:           eventpb.TimeInForce_TIME_IN_FORCE_GTC,
			Price:         "100",
			Qty:           "1",
			FreezeAsset:   "USDT",
			FreezeAmount:  "100",
			BalanceAfter: &eventpb.BalanceSnapshot{
				UserId: "u1", Asset: "USDT", Available: "400", Frozen: "100",
			},
		}},
	}
	batch := BuildJournalBatch([]DecoratedJournalEvent{{Event: evt}})

	if len(batch.Orders) != 1 {
		t.Fatalf("orders: %+v", batch.Orders)
	}
	if batch.Orders[0].Kind != OrderRowInsert {
		t.Errorf("expected insert, got kind=%d", batch.Orders[0].Kind)
	}
	if batch.Orders[0].OrderID != 42 || batch.Orders[0].Symbol != "BTC-USDT" {
		t.Errorf("order fields: %+v", batch.Orders[0])
	}
	if batch.Orders[0].Status != int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW) {
		t.Errorf("status: %d", batch.Orders[0].Status)
	}
	if batch.Orders[0].FrozenAmt != "100" {
		t.Errorf("frozen_amt: %s", batch.Orders[0].FrozenAmt)
	}

	if len(batch.Accounts) != 1 {
		t.Fatalf("accounts: %+v", batch.Accounts)
	}
	if batch.Accounts[0].Available != "400" || batch.Accounts[0].Frozen != "100" || batch.Accounts[0].CounterSeqID != 5 {
		t.Errorf("account fields: %+v", batch.Accounts[0])
	}

	if len(batch.AccountLogs) != 1 {
		t.Fatalf("logs: %+v", batch.AccountLogs)
	}
	log := batch.AccountLogs[0]
	if log.VShardID != 0 || log.CounterSeqID != 5 || log.Asset != "USDT" {
		t.Errorf("log pk: %+v", log)
	}
	if log.DeltaAvail != "-100" || log.DeltaFrozen != "100" {
		t.Errorf("log deltas: %+v", log)
	}
	if log.BizType != "freeze_place_order" || log.BizRefID != "42" {
		t.Errorf("log biz: %+v", log)
	}
}

func TestBuildJournalBatch_Settlement_TwoAssetLogs(t *testing.T) {
	evt := &eventpb.CounterJournalEvent{
		Meta:         newMeta(1_700_000_000_001, "counter-vshard-003"),
		CounterSeqId: 11,
		Payload: &eventpb.CounterJournalEvent_Settlement{Settlement: &eventpb.SettlementEvent{
			UserId:        "buyer",
			OrderId:       42,
			TradeId:       "BTC-USDT:17",
			Symbol:        "BTC-USDT",
			Side:          eventpb.Side_SIDE_BUY,
			Price:         "100",
			Qty:           "1",
			DeltaBase:     "1",
			DeltaQuote:    "0",
			UnfreezeBase:  "0",
			UnfreezeQuote: "100",
			BaseBalanceAfter: &eventpb.BalanceSnapshot{
				UserId: "buyer", Asset: "BTC", Available: "1", Frozen: "0",
			},
			QuoteBalanceAfter: &eventpb.BalanceSnapshot{
				UserId: "buyer", Asset: "USDT", Available: "300", Frozen: "0",
			},
		}},
	}
	batch := BuildJournalBatch([]DecoratedJournalEvent{{Event: evt}})

	if len(batch.Orders) != 0 {
		t.Errorf("settlement should not emit order rows: %+v", batch.Orders)
	}
	if len(batch.Accounts) != 2 {
		t.Fatalf("expected 2 accounts (base+quote): %+v", batch.Accounts)
	}
	if len(batch.AccountLogs) != 2 {
		t.Fatalf("expected 2 logs (base+quote): %+v", batch.AccountLogs)
	}
	// Logs share (shard_id, counter_seq_id) but differ in asset — this is
	// exactly why the PK expanded to include asset.
	assets := map[string]bool{}
	for _, l := range batch.AccountLogs {
		if l.VShardID != 3 || l.CounterSeqID != 11 {
			t.Errorf("log pk: %+v", l)
		}
		assets[l.Asset] = true
	}
	if !assets["BTC"] || !assets["USDT"] {
		t.Errorf("assets: %+v", assets)
	}
}

func TestBuildJournalBatch_OrderStatusUpdate(t *testing.T) {
	evt := &eventpb.CounterJournalEvent{
		Meta:         newMeta(1_700_000_000_050, "counter-vshard-000"),
		CounterSeqId: 7,
		Payload: &eventpb.CounterJournalEvent_OrderStatus{OrderStatus: &eventpb.OrderStatusEvent{
			UserId:    "u1",
			OrderId:   42,
			OldStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW,
			NewStatus: eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
			FilledQty: "1",
		}},
	}
	batch := BuildJournalBatch([]DecoratedJournalEvent{{Event: evt}})

	if len(batch.Orders) != 1 {
		t.Fatalf("orders: %+v", batch.Orders)
	}
	if batch.Orders[0].Kind != OrderRowUpdate {
		t.Errorf("expected update, got kind=%d", batch.Orders[0].Kind)
	}
	if batch.Orders[0].Status != int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED) {
		t.Errorf("status: %d", batch.Orders[0].Status)
	}
	if batch.Orders[0].FilledQty != "1" {
		t.Errorf("filled_qty: %s", batch.Orders[0].FilledQty)
	}
	// OrderStatusEvent has no BalanceSnapshot → no accounts / logs.
	if len(batch.Accounts) != 0 || len(batch.AccountLogs) != 0 {
		t.Errorf("should be empty: %+v %+v", batch.Accounts, batch.AccountLogs)
	}
}

func TestBuildJournalBatch_Transfer_DepositVsWithdraw(t *testing.T) {
	deposit := &eventpb.CounterJournalEvent{
		Meta:         newMeta(10, "counter-vshard-000"),
		CounterSeqId: 1,
		Payload: &eventpb.CounterJournalEvent_Transfer{Transfer: &eventpb.TransferEvent{
			UserId: "u1", TransferId: "tx-1", Asset: "USDT",
			Amount: "500", Type: eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
			BalanceAfter: &eventpb.BalanceSnapshot{
				UserId: "u1", Asset: "USDT", Available: "500", Frozen: "0",
			},
		}},
	}
	withdraw := &eventpb.CounterJournalEvent{
		Meta:         newMeta(11, "counter-vshard-000"),
		CounterSeqId: 2,
		Payload: &eventpb.CounterJournalEvent_Transfer{Transfer: &eventpb.TransferEvent{
			UserId: "u1", TransferId: "tx-2", Asset: "USDT",
			Amount: "100", Type: eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW,
			BalanceAfter: &eventpb.BalanceSnapshot{
				UserId: "u1", Asset: "USDT", Available: "400", Frozen: "0",
			},
		}},
	}
	batch := BuildJournalBatch([]DecoratedJournalEvent{{Event: deposit}, {Event: withdraw}})

	if len(batch.AccountLogs) != 2 {
		t.Fatalf("logs: %+v", batch.AccountLogs)
	}
	if batch.AccountLogs[0].DeltaAvail != "500" || batch.AccountLogs[0].BizType != "deposit" {
		t.Errorf("deposit: %+v", batch.AccountLogs[0])
	}
	if batch.AccountLogs[1].DeltaAvail != "-100" || batch.AccountLogs[1].BizType != "withdraw" {
		t.Errorf("withdraw: %+v", batch.AccountLogs[1])
	}
}

func TestBuildJournalBatch_CancelRequestedIsNoop(t *testing.T) {
	evt := &eventpb.CounterJournalEvent{
		Meta:         newMeta(1, "counter-vshard-000"),
		CounterSeqId: 9,
		Payload: &eventpb.CounterJournalEvent_CancelReq{CancelReq: &eventpb.CancelRequested{
			UserId: "u1", OrderId: 42, Symbol: "BTC-USDT",
		}},
	}
	batch := BuildJournalBatch([]DecoratedJournalEvent{{Event: evt}})
	if !batch.IsEmpty() {
		t.Errorf("CancelRequested should not produce rows: %+v", batch)
	}
}

func TestBuildJournalBatch_DropsUnparseableShard(t *testing.T) {
	evt := &eventpb.CounterJournalEvent{
		Meta:         newMeta(1, "not-a-counter"),
		CounterSeqId: 1,
		Payload: &eventpb.CounterJournalEvent_Transfer{Transfer: &eventpb.TransferEvent{
			UserId: "u1", TransferId: "tx-1", Asset: "USDT",
			Amount: "1", Type: eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
			BalanceAfter: &eventpb.BalanceSnapshot{UserId: "u1", Asset: "USDT", Available: "1"},
		}},
	}
	batch := BuildJournalBatch([]DecoratedJournalEvent{{Event: evt}})

	// accounts still get projected — they don't need shard_id.
	if len(batch.Accounts) != 1 {
		t.Errorf("accounts: %+v", batch.Accounts)
	}
	// account_logs are dropped because we can't key them.
	if len(batch.AccountLogs) != 0 {
		t.Errorf("logs: %+v", batch.AccountLogs)
	}
}

func TestNegateDecimal(t *testing.T) {
	cases := map[string]string{
		"":      "0",
		"0":     "0",
		"1":     "-1",
		"1.5":   "-1.5",
		"-1":    "1",
		"-1.5":  "1.5",
	}
	for in, want := range cases {
		if got := negateDecimal(in); got != want {
			t.Errorf("negateDecimal(%q)=%q want %q", in, got, want)
		}
	}
}

func TestTransferDeltas(t *testing.T) {
	cases := []struct {
		t     eventpb.TransferEvent_TransferType
		amt   string
		av    string
		fz    string
	}{
		{eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT, "100", "100", "0"},
		{eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW, "50", "-50", "0"},
		{eventpb.TransferEvent_TRANSFER_TYPE_FREEZE, "30", "-30", "30"},
		{eventpb.TransferEvent_TRANSFER_TYPE_UNFREEZE, "20", "20", "-20"},
		{eventpb.TransferEvent_TRANSFER_TYPE_UNSPECIFIED, "1", "0", "0"},
	}
	for _, c := range cases {
		av, fz := transferDeltas(c.t, c.amt)
		if av != c.av || fz != c.fz {
			t.Errorf("%v %s: got (%s,%s) want (%s,%s)", c.t, c.amt, av, fz, c.av, c.fz)
		}
	}
}
