// Package journal builds asset-journal envelopes (ADR-0057) and
// publishes them to Kafka. The builders are pure and live here; the
// Kafka producer lives in publisher.go.
package journal

import (
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/asset/internal/engine"
)

// EventKind picks which AssetJournalEvent oneof arm to fill.
type EventKind int

const (
	KindTransferOut EventKind = iota + 1
	KindTransferIn
	KindCompensate
)

// BuildInput is the context a builder needs. Most fields are carried
// straight from the gRPC request; BalanceAfter / FundingVersion come
// from engine.Result.
type BuildInput struct {
	Kind            EventKind
	AssetSeqID      uint64
	TsUnixMS        int64
	TraceID         string
	ProducerID      string
	FundingVersion  uint64

	UserID          string
	TransferID      string
	Asset           string
	Amount          string // decimal string; caller pre-formats
	PeerBiz         string
	Memo            string
	CompensateCause string // only used when Kind == KindCompensate

	BalanceAfter engine.Balance
}

// Build assembles one AssetJournalEvent envelope with the matching oneof
// payload filled. Returns an envelope ready to marshal + publish.
func Build(in BuildInput) *eventpb.AssetJournalEvent {
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	env := &eventpb.AssetJournalEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs:   ts,
			TraceId:    in.TraceID,
			ProducerId: in.ProducerID,
		},
		AssetSeqId:     in.AssetSeqID,
		FundingVersion: in.FundingVersion,
	}
	snap := &eventpb.FundingBalanceSnapshot{
		UserId:    in.UserID,
		Asset:     in.Asset,
		Available: in.BalanceAfter.Available.String(),
		Frozen:    in.BalanceAfter.Frozen.String(),
		Version:   in.BalanceAfter.Version,
	}
	switch in.Kind {
	case KindTransferOut:
		env.Payload = &eventpb.AssetJournalEvent_XferOut{
			XferOut: &eventpb.FundingTransferOutEvent{
				UserId:       in.UserID,
				TransferId:   in.TransferID,
				Asset:        in.Asset,
				Amount:       in.Amount,
				PeerBiz:      in.PeerBiz,
				Memo:         in.Memo,
				BalanceAfter: snap,
			},
		}
	case KindTransferIn:
		env.Payload = &eventpb.AssetJournalEvent_XferIn{
			XferIn: &eventpb.FundingTransferInEvent{
				UserId:       in.UserID,
				TransferId:   in.TransferID,
				Asset:        in.Asset,
				Amount:       in.Amount,
				PeerBiz:      in.PeerBiz,
				Memo:         in.Memo,
				BalanceAfter: snap,
			},
		}
	case KindCompensate:
		env.Payload = &eventpb.AssetJournalEvent_Compensate{
			Compensate: &eventpb.FundingCompensateEvent{
				UserId:          in.UserID,
				TransferId:      in.TransferID,
				Asset:           in.Asset,
				Amount:          in.Amount,
				PeerBiz:         in.PeerBiz,
				CompensateCause: in.CompensateCause,
				BalanceAfter:    snap,
			},
		}
	}
	return env
}
