package journal

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

func TestOrderEventToInternal_Placed(t *testing.T) {
	pb := &eventpb.OrderEvent{
		Meta: &eventpb.EventMeta{SeqId: 1, TsUnixMs: 1000},
		Payload: &eventpb.OrderEvent_Placed{
			Placed: &eventpb.OrderPlaced{
				UserId:        "u1",
				OrderId:       42,
				ClientOrderId: "c1",
				Symbol:        "BTC-USDT",
				Side:          eventpb.Side_SIDE_BUY,
				OrderType:     eventpb.OrderType_ORDER_TYPE_LIMIT,
				Tif:           eventpb.TimeInForce_TIME_IN_FORCE_GTC,
				Price:         "100.5",
				Qty:           "2",
			},
		},
	}
	got, err := OrderEventToInternal(pb, sequencer.SourceMeta{Topic: "order-event", Partition: 0, Offset: 17})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Kind != sequencer.EventOrderPlaced {
		t.Fatalf("kind = %d, want EventOrderPlaced", got.Kind)
	}
	o := got.Order
	if o.ID != 42 || o.UserID != "u1" || o.Symbol != "BTC-USDT" {
		t.Fatalf("order fields: %+v", o)
	}
	if o.Side != orderbook.Bid || o.Type != orderbook.Limit || o.TIF != orderbook.GTC {
		t.Fatalf("enum mapping: side=%d type=%d tif=%d", o.Side, o.Type, o.TIF)
	}
	if o.Price.Cmp(dec.New("100.5")) != 0 || o.Qty.Cmp(dec.New("2")) != 0 {
		t.Fatalf("decimals: price=%s qty=%s", o.Price, o.Qty)
	}
	if got.Source.Offset != 17 {
		t.Fatalf("source offset lost: %+v", got.Source)
	}
}

func TestOrderEventToInternal_Cancel(t *testing.T) {
	pb := &eventpb.OrderEvent{
		Meta: &eventpb.EventMeta{SeqId: 2},
		Payload: &eventpb.OrderEvent_Cancel{
			Cancel: &eventpb.OrderCancel{
				UserId:  "u1",
				OrderId: 42,
				Symbol:  "BTC-USDT",
			},
		},
	}
	got, err := OrderEventToInternal(pb, sequencer.SourceMeta{})
	if err != nil {
		t.Fatalf("%v", err)
	}
	if got.Kind != sequencer.EventOrderCancel || got.OrderID != 42 || got.UserID != "u1" {
		t.Fatalf("cancel mapping: %+v", got)
	}
}

func TestOrderEventToInternal_InvalidQty(t *testing.T) {
	pb := &eventpb.OrderEvent{
		Payload: &eventpb.OrderEvent_Placed{
			Placed: &eventpb.OrderPlaced{
				OrderId:   1,
				Symbol:    "BTC-USDT",
				Side:      eventpb.Side_SIDE_BUY,
				OrderType: eventpb.OrderType_ORDER_TYPE_LIMIT,
				Tif:       eventpb.TimeInForce_TIME_IN_FORCE_GTC,
				Price:     "100",
				Qty:       "-1",
			},
		},
	}
	if _, err := OrderEventToInternal(pb, sequencer.SourceMeta{}); err == nil {
		t.Fatal("expected error for non-positive qty")
	}
}

func TestOutputToTradeEvent_Trade(t *testing.T) {
	out := &sequencer.Output{
		Kind:           sequencer.OutputTrade,
		SeqID:          17,
		Symbol:         "BTC-USDT",
		UserID:         "t1",
		OrderID:        101,
		Side:           orderbook.Bid,
		Price:          dec.New("100"),
		Qty:            dec.New("1"),
		MakerUserID:    "m1",
		MakerOrderID:   50,
		MakerSide:      orderbook.Ask,
		MakerRemaining: dec.New("0"),
		TakerRemaining: dec.New("0"),
	}
	te, err := OutputToTradeEvent(out, "match-shard-0-main")
	if err != nil {
		t.Fatalf("%v", err)
	}
	if te.Meta.SeqId != 17 || te.Meta.ProducerId != "match-shard-0-main" {
		t.Fatalf("meta: %+v", te.Meta)
	}
	trade := te.GetTrade()
	if trade == nil {
		t.Fatalf("expected Trade payload, got %+v", te)
	}
	if trade.Price != "100" || trade.Qty != "1" {
		t.Fatalf("trade amounts: %+v", trade)
	}
	if trade.TakerSide != eventpb.Side_SIDE_BUY {
		t.Fatalf("taker side: %v", trade.TakerSide)
	}
}

func TestOutputToTradeEvent_Accepted(t *testing.T) {
	out := &sequencer.Output{
		Kind:           sequencer.OutputOrderAccepted,
		SeqID:          9,
		Symbol:         "BTC-USDT",
		UserID:         "u1",
		OrderID:        42,
		Side:           orderbook.Ask,
		Price:          dec.New("101.25"),
		TakerRemaining: dec.New("0.75"),
	}
	te, err := OutputToTradeEvent(out, "match-shard-0-main")
	if err != nil {
		t.Fatalf("%v", err)
	}
	acc := te.GetAccepted()
	if acc == nil {
		t.Fatalf("expected Accepted payload, got %+v", te.Payload)
	}
	if acc.OrderId != 42 || acc.Symbol != "BTC-USDT" || acc.UserId != "u1" {
		t.Fatalf("accepted identity: %+v", acc)
	}
	if acc.Side != eventpb.Side_SIDE_SELL {
		t.Fatalf("accepted side: %v", acc.Side)
	}
	if acc.Price != "101.25" || acc.RemainingQty != "0.75" {
		t.Fatalf("accepted price/remaining: %+v", acc)
	}
}

func TestOutputToTradeEvent_Rejected(t *testing.T) {
	out := &sequencer.Output{
		Kind:         sequencer.OutputOrderRejected,
		SeqID:        5,
		Symbol:       "BTC-USDT",
		UserID:       "u1",
		OrderID:      1,
		RejectReason: orderbook.RejectPostOnlyWouldTake,
	}
	te, err := OutputToTradeEvent(out, "match-shard-0-main")
	if err != nil {
		t.Fatalf("%v", err)
	}
	rej := te.GetRejected()
	if rej == nil {
		t.Fatalf("expected Rejected payload, got %+v", te.Payload)
	}
	if rej.Reason != eventpb.RejectReason_REJECT_REASON_POST_ONLY_WOULD_TAKE {
		t.Fatalf("reject reason: %v", rej.Reason)
	}
}
