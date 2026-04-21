package journal

import (
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// SagaStateChangeInput is what SagaStateChange needs to emit one
// transition. asset_seq_id is assigned inside the builder so callers
// don't need to touch Publisher.
type SagaStateChangeInput struct {
	AssetSeqID   uint64
	TsUnixMS     int64
	TraceID      string
	ProducerID   string

	TransferID   string
	UserID       string
	FromBiz      string
	ToBiz        string
	Asset        string
	Amount       string
	OldState     string // "INIT" / "DEBITED" / ...
	NewState     string
	RejectReason string
}

// BuildSagaStateChange assembles an AssetJournalEvent carrying a
// SagaStateChangeEvent payload. funding_version is 0 here because a
// pure state transition doesn't touch funding balances (the balance-
// moving transitions ride on FundingTransferOut/In/Compensate instead);
// consumers observing funding_version==0 on a saga_state event know to
// skip it for balance-cache invalidation.
func BuildSagaStateChange(in SagaStateChangeInput) *eventpb.AssetJournalEvent {
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	return &eventpb.AssetJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs:   ts,
			TraceId:    in.TraceID,
			ProducerId: in.ProducerID,
		},
		AssetSeqId:     in.AssetSeqID,
		FundingVersion: 0,
		Payload: &eventpb.AssetJournalEvent_SagaState{
			SagaState: &eventpb.SagaStateChangeEvent{
				TransferId:   in.TransferID,
				UserId:       in.UserID,
				FromBiz:      in.FromBiz,
				ToBiz:        in.ToBiz,
				Asset:        in.Asset,
				Amount:       in.Amount,
				OldState:     in.OldState,
				NewState:     in.NewState,
				RejectReason: in.RejectReason,
			},
		},
	}
}
