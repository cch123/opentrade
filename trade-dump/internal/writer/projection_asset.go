package writer

import (
	"context"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// -----------------------------------------------------------------------------
// Row types for asset-journal projection (ADR-0057 M5).
//
// Three tables are touched:
//
//   funding_accounts      — LWW current balance per (user, asset).
//   funding_account_logs  — append-only journal keyed by asset_seq_id.
//   transfers             — LWW saga state per transfer_id.
//
// funding_accounts / transfers use asset_seq_id as the guard; funding_-
// account_logs is PK'd on (asset_seq_id, asset) and uses no-op ON
// DUPLICATE KEY UPDATE so replays are idempotent.
// -----------------------------------------------------------------------------

// FundingAccountRow mirrors the funding_accounts table.
type FundingAccountRow struct {
	UserID         string
	Asset          string
	Available      string
	Frozen         string
	AssetSeqID     uint64
	FundingVersion uint64
	BalanceVersion uint64
}

// FundingAccountLogRow mirrors funding_account_logs. PK (asset_seq_id, asset).
type FundingAccountLogRow struct {
	AssetSeqID  uint64
	Asset       string
	UserID      string
	DeltaAvail  string
	DeltaFrozen string
	AvailAfter  string
	FrozenAfter string
	BizType     string // "transfer_out" / "transfer_in" / "compensate"
	BizRefID    string // saga transfer_id
	PeerBiz     string
	TsUnixMs    int64
}

// TransferRow mirrors the transfers table. One row per saga, upserted
// on every SagaStateChange event; last-write-wins on asset_seq_id.
// Amount is whatever the saga stamped when it was created — SagaStatee-
// Change carries it verbatim so the projection has a stable copy.
type TransferRow struct {
	TransferID   string
	UserID       string
	FromBiz      string
	ToBiz        string
	Asset        string
	Amount       string
	State        string // transferledger.State string values
	RejectReason string
	AssetSeqID   uint64
	CreatedAtMs  int64
	UpdatedAtMs  int64
}

// AssetBatch is the aggregate projection of a consumed slice of
// AssetJournalEvents. Writer applies all three slices inside a single
// MySQL transaction.
type AssetBatch struct {
	Accounts  []FundingAccountRow
	Logs      []FundingAccountLogRow
	Transfers []TransferRow
}

// IsEmpty reports whether the batch has nothing to write.
func (b *AssetBatch) IsEmpty() bool {
	return len(b.Accounts) == 0 && len(b.Logs) == 0 && len(b.Transfers) == 0
}

// AssetWriter is the contract the consumer uses against MySQL. Real
// impl in mysql_asset.go; tests fake it.
type AssetWriter interface {
	ApplyAssetBatch(ctx context.Context, batch AssetBatch) error
}

// -----------------------------------------------------------------------------
// Pure builder: AssetJournalEvent → AssetBatch
// -----------------------------------------------------------------------------

// BuildAssetBatch projects a slice of events into a MySQL write batch.
// Pure function, no I/O. Unknown / malformed events are skipped
// silently so a single bad event can't block the pipeline.
func BuildAssetBatch(events []*eventpb.AssetJournalEvent) AssetBatch {
	var batch AssetBatch
	for _, evt := range events {
		if evt == nil {
			continue
		}
		ts := tsFromMeta(evt.Meta)
		seqID := evt.AssetSeqId
		fundingVer := evt.FundingVersion

		switch p := evt.Payload.(type) {
		case *eventpb.AssetJournalEvent_XferOut:
			appendFromFundingOut(&batch, p.XferOut, seqID, fundingVer, ts)
		case *eventpb.AssetJournalEvent_XferIn:
			appendFromFundingIn(&batch, p.XferIn, seqID, fundingVer, ts)
		case *eventpb.AssetJournalEvent_Compensate:
			appendFromFundingCompensate(&batch, p.Compensate, seqID, fundingVer, ts)
		case *eventpb.AssetJournalEvent_SagaState:
			appendFromSagaState(&batch, p.SagaState, seqID, ts)
		}
	}
	return batch
}

// appendFromFundingOut projects a FundingTransferOutEvent into
// funding_accounts (LWW balance) + funding_account_logs (append delta).
// SagaStateChange is expected to land separately for the transfers row.
func appendFromFundingOut(b *AssetBatch, e *eventpb.FundingTransferOutEvent, seqID, fundingVer uint64, ts int64) {
	if e == nil {
		return
	}
	snap := e.BalanceAfter
	if snap == nil {
		return
	}
	b.Accounts = append(b.Accounts, fundingAccountRowFromSnap(snap, seqID, fundingVer))
	b.Logs = append(b.Logs, FundingAccountLogRow{
		AssetSeqID:  seqID,
		Asset:       snap.Asset,
		UserID:      e.UserId,
		DeltaAvail:  negateDecimal(e.Amount),
		DeltaFrozen: "0",
		AvailAfter:  snap.Available,
		FrozenAfter: snap.Frozen,
		BizType:     "transfer_out",
		BizRefID:    e.TransferId,
		PeerBiz:     e.PeerBiz,
		TsUnixMs:    ts,
	})
}

// appendFromFundingIn projects a FundingTransferInEvent.
func appendFromFundingIn(b *AssetBatch, e *eventpb.FundingTransferInEvent, seqID, fundingVer uint64, ts int64) {
	if e == nil {
		return
	}
	snap := e.BalanceAfter
	if snap == nil {
		return
	}
	b.Accounts = append(b.Accounts, fundingAccountRowFromSnap(snap, seqID, fundingVer))
	b.Logs = append(b.Logs, FundingAccountLogRow{
		AssetSeqID:  seqID,
		Asset:       snap.Asset,
		UserID:      e.UserId,
		DeltaAvail:  trimDecimal(e.Amount),
		DeltaFrozen: "0",
		AvailAfter:  snap.Available,
		FrozenAfter: snap.Frozen,
		BizType:     "transfer_in",
		BizRefID:    e.TransferId,
		PeerBiz:     e.PeerBiz,
		TsUnixMs:    ts,
	})
}

// appendFromFundingCompensate projects a FundingCompensateEvent.
// Balance math matches TransferIn (credit back) but the biz_type
// differs so audit / reconciliation can tell compensations apart from
// normal credits.
func appendFromFundingCompensate(b *AssetBatch, e *eventpb.FundingCompensateEvent, seqID, fundingVer uint64, ts int64) {
	if e == nil {
		return
	}
	snap := e.BalanceAfter
	if snap == nil {
		return
	}
	b.Accounts = append(b.Accounts, fundingAccountRowFromSnap(snap, seqID, fundingVer))
	b.Logs = append(b.Logs, FundingAccountLogRow{
		AssetSeqID:  seqID,
		Asset:       snap.Asset,
		UserID:      e.UserId,
		DeltaAvail:  trimDecimal(e.Amount),
		DeltaFrozen: "0",
		AvailAfter:  snap.Available,
		FrozenAfter: snap.Frozen,
		BizType:     "compensate",
		BizRefID:    e.TransferId,
		PeerBiz:     e.PeerBiz,
		TsUnixMs:    ts,
	})
}

// appendFromSagaState projects a SagaStateChangeEvent into the
// transfers table. The created_at_ms is carried from the event
// timestamp on the FIRST SagaStateChange we see (INIT → DEBITED is
// typically the first one asset-service emits); subsequent LWW upserts
// leave created_at_ms alone via the guard clause.
func appendFromSagaState(b *AssetBatch, e *eventpb.SagaStateChangeEvent, seqID uint64, ts int64) {
	if e == nil || e.TransferId == "" {
		return
	}
	b.Transfers = append(b.Transfers, TransferRow{
		TransferID:   e.TransferId,
		UserID:       e.UserId,
		FromBiz:      e.FromBiz,
		ToBiz:        e.ToBiz,
		Asset:        e.Asset,
		Amount:       e.Amount,
		State:        e.NewState,
		RejectReason: e.RejectReason,
		AssetSeqID:   seqID,
		CreatedAtMs:  ts,
		UpdatedAtMs:  ts,
	})
}

func fundingAccountRowFromSnap(snap *eventpb.FundingBalanceSnapshot, seqID, fundingVer uint64) FundingAccountRow {
	return FundingAccountRow{
		UserID:         snap.UserId,
		Asset:          snap.Asset,
		Available:      snap.Available,
		Frozen:         snap.Frozen,
		AssetSeqID:     seqID,
		FundingVersion: fundingVer,
		BalanceVersion: snap.Version,
	}
}
