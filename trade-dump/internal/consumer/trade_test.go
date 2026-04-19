package consumer

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func makeTradeEvent(matchSeq uint64) *eventpb.TradeEvent {
	return &eventpb.TradeEvent{
		Meta:       &eventpb.EventMeta{TsUnixMs: int64(1_700_000_000_000 + matchSeq)},
		MatchSeqId: matchSeq,
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId:      "BTC-USDT:" + itoa(matchSeq),
			Symbol:       "BTC-USDT",
			Price:        "42000",
			Qty:          "0.5",
			MakerUserId:  "maker",
			MakerOrderId: 10 + matchSeq,
			TakerUserId:  "taker",
			TakerOrderId: 20 + matchSeq,
			TakerSide:    eventpb.Side_SIDE_BUY,
		}},
	}
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestDecodeBatch_KeepsTradesDropsOthers(t *testing.T) {
	logger := zap.NewNop()
	records := []*kgo.Record{
		{Value: mustMarshal(t, makeTradeEvent(1))},
		{Value: mustMarshal(t, &eventpb.TradeEvent{
			Meta:       &eventpb.EventMeta{},
			MatchSeqId: 2,
			Payload:    &eventpb.TradeEvent_Accepted{Accepted: &eventpb.OrderAccepted{OrderId: 5}},
		})},
		{Value: mustMarshal(t, makeTradeEvent(3))},
		{Value: mustMarshal(t, &eventpb.TradeEvent{
			Meta:       &eventpb.EventMeta{},
			MatchSeqId: 4,
			Payload:    &eventpb.TradeEvent_Cancelled{Cancelled: &eventpb.OrderCancelled{OrderId: 6}},
		})},
	}
	rows := decodeBatch(records, logger)

	if len(rows) != 2 {
		t.Fatalf("rows: got %d want 2", len(rows))
	}
	if rows[0].MatchSeqID != 1 || rows[1].MatchSeqID != 3 {
		t.Errorf("order: %+v", rows)
	}
	if rows[0].TradeID != "BTC-USDT:1" || rows[1].TradeID != "BTC-USDT:3" {
		t.Errorf("trade_ids: %+v", rows)
	}
}

func TestDecodeBatch_SkipsMalformed(t *testing.T) {
	logger := zap.NewNop()
	records := []*kgo.Record{
		{Value: []byte("\xff\xff\xffnot-a-protobuf"), Topic: "trade-event", Partition: 0, Offset: 1},
		{Value: mustMarshal(t, makeTradeEvent(7))},
	}
	rows := decodeBatch(records, logger)
	if len(rows) != 1 || rows[0].MatchSeqID != 7 {
		t.Fatalf("rows: %+v", rows)
	}
}

func TestDecodeBatch_Empty(t *testing.T) {
	rows := decodeBatch(nil, zap.NewNop())
	if len(rows) != 0 {
		t.Errorf("expected no rows, got %d", len(rows))
	}
}
