package writer

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// TestBuildAssetBatch_FundingTransferOut: funding is debited (delta=-amount)
// and the accounts row reflects the new balance.
func TestBuildAssetBatch_FundingTransferOut(t *testing.T) {
	evt := &eventpb.AssetJournalEvent{
		Meta:           newMeta(1_700_000_000_000, "asset-main"),
		AssetSeqId:     7,
		FundingVersion: 3,
		Payload: &eventpb.AssetJournalEvent_XferOut{XferOut: &eventpb.FundingTransferOutEvent{
			UserId:     "u1",
			TransferId: "saga-1",
			Asset:      "USDT",
			Amount:     "25",
			PeerBiz:    "spot",
			BalanceAfter: &eventpb.FundingBalanceSnapshot{
				UserId: "u1", Asset: "USDT", Available: "75", Frozen: "0", Version: 4,
			},
		}},
	}
	batch := BuildAssetBatch([]*eventpb.AssetJournalEvent{evt})

	if len(batch.Accounts) != 1 {
		t.Fatalf("accounts: %+v", batch.Accounts)
	}
	acc := batch.Accounts[0]
	if acc.Available != "75" || acc.AssetSeqID != 7 || acc.FundingVersion != 3 || acc.BalanceVersion != 4 {
		t.Errorf("account row = %+v", acc)
	}
	if len(batch.Logs) != 1 {
		t.Fatalf("logs: %+v", batch.Logs)
	}
	log := batch.Logs[0]
	if log.DeltaAvail != "-25" || log.BizType != "transfer_out" || log.BizRefID != "saga-1" || log.PeerBiz != "spot" {
		t.Errorf("log row = %+v", log)
	}
	if log.AssetSeqID != 7 {
		t.Errorf("log seq = %d", log.AssetSeqID)
	}
	// TransferOut event alone does not touch transfers (SagaStateChange does).
	if len(batch.Transfers) != 0 {
		t.Errorf("transfers should be untouched: %+v", batch.Transfers)
	}
}

// TestBuildAssetBatch_FundingTransferIn: funding credited (delta=+amount).
func TestBuildAssetBatch_FundingTransferIn(t *testing.T) {
	evt := &eventpb.AssetJournalEvent{
		Meta:           newMeta(1_700_000_000_000, "asset-main"),
		AssetSeqId:     9,
		FundingVersion: 4,
		Payload: &eventpb.AssetJournalEvent_XferIn{XferIn: &eventpb.FundingTransferInEvent{
			UserId:     "u1",
			TransferId: "saga-2",
			Asset:      "USDT",
			Amount:     "50",
			PeerBiz:    "spot",
			BalanceAfter: &eventpb.FundingBalanceSnapshot{
				UserId: "u1", Asset: "USDT", Available: "125", Frozen: "0", Version: 5,
			},
		}},
	}
	batch := BuildAssetBatch([]*eventpb.AssetJournalEvent{evt})

	if len(batch.Accounts) != 1 || batch.Accounts[0].Available != "125" {
		t.Errorf("account = %+v", batch.Accounts)
	}
	if len(batch.Logs) != 1 {
		t.Fatalf("logs = %+v", batch.Logs)
	}
	if batch.Logs[0].DeltaAvail != "50" || batch.Logs[0].BizType != "transfer_in" {
		t.Errorf("log = %+v", batch.Logs[0])
	}
}

// TestBuildAssetBatch_Compensate: same math as TransferIn; distinct biz_type.
func TestBuildAssetBatch_Compensate(t *testing.T) {
	evt := &eventpb.AssetJournalEvent{
		Meta:           newMeta(1_700_000_000_000, "asset-main"),
		AssetSeqId:     11,
		FundingVersion: 5,
		Payload: &eventpb.AssetJournalEvent_Compensate{Compensate: &eventpb.FundingCompensateEvent{
			UserId:          "u1",
			TransferId:      "saga-3",
			Asset:           "USDT",
			Amount:          "40",
			PeerBiz:         "spot",
			CompensateCause: "peer_in_rejected",
			BalanceAfter: &eventpb.FundingBalanceSnapshot{
				UserId: "u1", Asset: "USDT", Available: "140", Frozen: "0", Version: 6,
			},
		}},
	}
	batch := BuildAssetBatch([]*eventpb.AssetJournalEvent{evt})

	if len(batch.Logs) != 1 || batch.Logs[0].BizType != "compensate" || batch.Logs[0].DeltaAvail != "40" {
		t.Errorf("log = %+v", batch.Logs)
	}
}

// TestBuildAssetBatch_SagaState: updates transfers only, not accounts /
// logs.
func TestBuildAssetBatch_SagaState(t *testing.T) {
	evt := &eventpb.AssetJournalEvent{
		Meta:       newMeta(1_700_000_000_000, "asset-main"),
		AssetSeqId: 13,
		Payload: &eventpb.AssetJournalEvent_SagaState{SagaState: &eventpb.SagaStateChangeEvent{
			TransferId:   "saga-1",
			UserId:       "u1",
			FromBiz:      "funding",
			ToBiz:        "spot",
			Asset:        "USDT",
			Amount:       "25",
			OldState:     "INIT",
			NewState:     "DEBITED",
			RejectReason: "",
		}},
	}
	batch := BuildAssetBatch([]*eventpb.AssetJournalEvent{evt})

	if len(batch.Accounts) != 0 || len(batch.Logs) != 0 {
		t.Errorf("saga_state should not touch accounts/logs: %+v", batch)
	}
	if len(batch.Transfers) != 1 {
		t.Fatalf("transfers: %+v", batch.Transfers)
	}
	tr := batch.Transfers[0]
	if tr.TransferID != "saga-1" || tr.State != "DEBITED" || tr.AssetSeqID != 13 {
		t.Errorf("transfer = %+v", tr)
	}
	if tr.CreatedAtMs != 1_700_000_000_000 || tr.UpdatedAtMs != 1_700_000_000_000 {
		t.Errorf("timestamps = %+v", tr)
	}
}

// TestBuildAssetBatch_SkipsNilAndUnknown: nil events and unknown payload
// types don't break the builder.
func TestBuildAssetBatch_SkipsNilAndUnknown(t *testing.T) {
	batch := BuildAssetBatch([]*eventpb.AssetJournalEvent{
		nil,
		{Meta: newMeta(1, "asset-main"), AssetSeqId: 1}, // no payload
	})
	if !batch.IsEmpty() {
		t.Errorf("expected empty batch, got %+v", batch)
	}
}

// TestBuildAssetBatch_MissingBalanceSnapshot: events without
// BalanceAfter are skipped cleanly (prevents nil deref).
func TestBuildAssetBatch_MissingBalanceSnapshot(t *testing.T) {
	evt := &eventpb.AssetJournalEvent{
		Meta:       newMeta(1, "asset-main"),
		AssetSeqId: 1,
		Payload: &eventpb.AssetJournalEvent_XferOut{XferOut: &eventpb.FundingTransferOutEvent{
			UserId: "u1", TransferId: "saga-x", Asset: "USDT", Amount: "1",
			// BalanceAfter omitted intentionally.
		}},
	}
	batch := BuildAssetBatch([]*eventpb.AssetJournalEvent{evt})
	if !batch.IsEmpty() {
		t.Errorf("missing snapshot should skip: %+v", batch)
	}
}

// TestBuildAssetBatch_MultipleEventsPreserveOrder: a realistic saga emits
// SagaStateChange then FundingTransferOut; projection preserves the
// asset_seq_id so replay idempotency stays intact.
func TestBuildAssetBatch_MultipleEventsPreserveOrder(t *testing.T) {
	events := []*eventpb.AssetJournalEvent{
		{
			Meta: newMeta(1_700_000_000_000, "asset-main"), AssetSeqId: 1,
			Payload: &eventpb.AssetJournalEvent_SagaState{SagaState: &eventpb.SagaStateChangeEvent{
				TransferId: "saga-1", UserId: "u1", FromBiz: "funding", ToBiz: "spot",
				Asset: "USDT", Amount: "25", OldState: "INIT", NewState: "DEBITED",
			}},
		},
		{
			Meta: newMeta(1_700_000_000_100, "asset-main"), AssetSeqId: 2, FundingVersion: 1,
			Payload: &eventpb.AssetJournalEvent_XferOut{XferOut: &eventpb.FundingTransferOutEvent{
				UserId: "u1", TransferId: "saga-1", Asset: "USDT", Amount: "25", PeerBiz: "spot",
				BalanceAfter: &eventpb.FundingBalanceSnapshot{
					UserId: "u1", Asset: "USDT", Available: "75", Version: 1,
				},
			}},
		},
	}
	batch := BuildAssetBatch(events)
	if len(batch.Transfers) != 1 || batch.Transfers[0].AssetSeqID != 1 {
		t.Errorf("transfers = %+v", batch.Transfers)
	}
	if len(batch.Accounts) != 1 || batch.Accounts[0].AssetSeqID != 2 {
		t.Errorf("accounts = %+v", batch.Accounts)
	}
	if len(batch.Logs) != 1 || batch.Logs[0].AssetSeqID != 2 {
		t.Errorf("logs = %+v", batch.Logs)
	}
}
