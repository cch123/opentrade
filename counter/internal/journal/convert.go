// Package journal serializes Counter state-machine events to the
// counter-journal Kafka topic (ADR-0004).
//
// This file contains pure protobuf conversion helpers — no Kafka dependency,
// fully unit-testable.
package journal

import (
	"fmt"
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/engine"
)

// TransferEventInput is the internal input used to build a journal envelope.
type TransferEventInput struct {
	SeqID      uint64
	TsUnixMS   int64
	TraceID    string
	ProducerID string

	Req          engine.TransferRequest
	BalanceAfter engine.Balance
}

// BuildTransferEvent builds a CounterJournalEvent with a TransferEvent payload.
func BuildTransferEvent(in TransferEventInput) (*eventpb.CounterJournalEvent, error) {
	ts := in.TsUnixMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	pbType, err := transferTypeToProto(in.Req.Type)
	if err != nil {
		return nil, err
	}
	return &eventpb.CounterJournalEvent{
		Meta: &eventpb.EventMeta{
			SeqId:      in.SeqID,
			TsUnixMs:   ts,
			TraceId:    in.TraceID,
			ProducerId: in.ProducerID,
		},
		Payload: &eventpb.CounterJournalEvent_Transfer{
			Transfer: &eventpb.TransferEvent{
				UserId:     in.Req.UserID,
				TransferId: in.Req.TransferID,
				Asset:      in.Req.Asset,
				Amount:     in.Req.Amount.String(),
				Type:       pbType,
				BizRefId:   in.Req.BizRefID,
				Memo:       in.Req.Memo,
				BalanceAfter: &eventpb.BalanceSnapshot{
					UserId:    in.Req.UserID,
					Asset:     in.Req.Asset,
					Available: in.BalanceAfter.Available.String(),
					Frozen:    in.BalanceAfter.Frozen.String(),
				},
			},
		},
	}, nil
}

// transferTypeToProto maps the internal TransferType to the proto enum inside
// TransferEvent.
func transferTypeToProto(t engine.TransferType) (eventpb.TransferEvent_TransferType, error) {
	switch t {
	case engine.TransferDeposit:
		return eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT, nil
	case engine.TransferWithdraw:
		return eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW, nil
	case engine.TransferFreeze:
		return eventpb.TransferEvent_TRANSFER_TYPE_FREEZE, nil
	case engine.TransferUnfreeze:
		return eventpb.TransferEvent_TRANSFER_TYPE_UNFREEZE, nil
	default:
		return 0, fmt.Errorf("unknown transfer type: %d", t)
	}
}
