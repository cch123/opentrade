package consumer

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func mustMarshalAsset(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestDecodeAssetBatch_DropsMalformed(t *testing.T) {
	good := &eventpb.AssetJournalEvent{
		Meta:           &eventpb.EventMeta{ProducerId: "asset-main"},
		AssetSeqId:     1,
		FundingVersion: 1,
		Payload: &eventpb.AssetJournalEvent_SagaState{SagaState: &eventpb.SagaStateChangeEvent{
			TransferId: "saga-1", UserId: "u1",
			FromBiz: "funding", ToBiz: "spot",
			Asset: "USDT", Amount: "10",
			OldState: "INIT", NewState: "DEBITED",
		}},
	}
	records := []*kgo.Record{
		{Value: []byte("\xffnot-a-proto"), Topic: "asset-journal", Partition: 0, Offset: 0},
		{Value: mustMarshalAsset(t, good)},
	}
	events := decodeAssetBatch(records, zap.NewNop())
	if len(events) != 1 {
		t.Fatalf("events: %d", len(events))
	}
	saga := events[0].GetSagaState()
	if saga == nil || saga.TransferId != "saga-1" || saga.NewState != "DEBITED" {
		t.Errorf("not decoded right: %+v", events[0])
	}
}
