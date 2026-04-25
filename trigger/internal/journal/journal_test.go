package journal

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/trigger/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

func TestConvertMapsAllFields(t *testing.T) {
	stop, _ := dec.Parse("100")
	limit, _ := dec.Parse("99")
	qty, _ := dec.Parse("0.5")
	quote, _ := dec.Parse("0")
	act, _ := dec.Parse("110")
	wm, _ := dec.Parse("115")
	c := &engine.Trigger{
		ID:                1,
		ClientTriggerID:      "cli-1",
		UserID:            "u1",
		Symbol:            "BTC-USDT",
		Side:              eventpb.Side_SIDE_SELL,
		Type:              condrpc.TriggerType_TRIGGER_TYPE_TRAILING_STOP_LOSS,
		StopPrice:         stop,
		LimitPrice:        limit,
		Qty:               qty,
		QuoteQty:          quote,
		TIF:               eventpb.TimeInForce_TIME_IN_FORCE_GTC,
		Status:            condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED,
		CreatedAtMs:       111,
		TriggeredAtMs:     222,
		PlacedOrderID:     42,
		RejectReason:      "",
		ExpiresAtMs:       333,
		OCOGroupID:        "oco-1",
		TrailingDeltaBps:  100,
		ActivationPrice:   act,
		TrailingWatermark: wm,
		TrailingActive:    true,
	}
	u := ConvertForTest(c, 7, "trigger-inst")
	if u.Id != 1 || u.ClientTriggerId != "cli-1" || u.UserId != "u1" {
		t.Fatalf("basics wrong: %+v", u)
	}
	if u.Side != eventpb.Side_SIDE_SELL {
		t.Fatalf("side: %v", u.Side)
	}
	if u.Type != eventpb.TriggerEventType_TRIGGER_EVENT_TYPE_TRAILING_STOP_LOSS {
		t.Fatalf("type: %v", u.Type)
	}
	if u.Status != eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_TRIGGERED {
		t.Fatalf("status: %v", u.Status)
	}
	if u.PlacedOrderId != 42 {
		t.Fatalf("placed_order_id: %d", u.PlacedOrderId)
	}
	if u.OcoGroupId != "oco-1" {
		t.Fatalf("oco: %s", u.OcoGroupId)
	}
	if !u.TrailingActive || u.TrailingWatermark != "115" || u.ActivationPrice != "110" {
		t.Fatalf("trailing fields: %+v", u)
	}
	if u.Meta == nil || u.TriggerSeqId != 7 || u.Meta.ProducerId != "trigger-inst" {
		t.Fatalf("meta: %+v trigger_seq_id: %d", u.Meta, u.TriggerSeqId)
	}
	if u.Meta.TsUnixMs <= 0 {
		t.Fatalf("expected ts_unix_ms set")
	}
}

func TestMapTypeStatusFallbacks(t *testing.T) {
	if got := mapType(condrpc.TriggerType_TRIGGER_TYPE_UNSPECIFIED); got != eventpb.TriggerEventType_TRIGGER_EVENT_TYPE_UNSPECIFIED {
		t.Errorf("unspecified type: %v", got)
	}
	if got := mapStatus(condrpc.TriggerStatus_TRIGGER_STATUS_UNSPECIFIED); got != eventpb.TriggerEventStatus_TRIGGER_EVENT_STATUS_UNSPECIFIED {
		t.Errorf("unspecified status: %v", got)
	}
}
