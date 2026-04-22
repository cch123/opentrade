package consumer

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func mustMarshalJournal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestDecodeJournalBatch_DropsMalformed(t *testing.T) {
	good := &eventpb.CounterJournalEvent{
		Meta:         &eventpb.EventMeta{ProducerId: "counter-shard-0-main"},
		CounterSeqId: 1,
		Payload: &eventpb.CounterJournalEvent_Transfer{Transfer: &eventpb.TransferEvent{
			UserId: "u1", TransferId: "t1", Asset: "USDT",
			Amount: "1", Type: eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT,
			BalanceAfter: &eventpb.BalanceSnapshot{UserId: "u1", Asset: "USDT", Available: "1"},
		}},
	}
	records := []*kgo.Record{
		{Value: []byte("\xffnot-a-proto"), Topic: "counter-journal", Partition: 0, Offset: 0},
		{Value: mustMarshalJournal(t, good)},
	}
	events := decodeJournalBatch(records, zap.NewNop())
	if len(events) != 1 {
		t.Fatalf("events: %d", len(events))
	}
	if events[0].Event.GetTransfer().UserId != "u1" {
		t.Errorf("not decoded right: %+v", events[0])
	}
}
