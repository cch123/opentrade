package consumer

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestUserIDOf_AllPayloads(t *testing.T) {
	cases := []struct {
		name string
		evt  *eventpb.CounterJournalEvent
		want string
	}{
		{"freeze", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_Freeze{
			Freeze: &eventpb.FreezeEvent{UserId: "u1"}}}, "u1"},
		{"unfreeze", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_Unfreeze{
			Unfreeze: &eventpb.UnfreezeEvent{UserId: "u2"}}}, "u2"},
		{"settlement", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_Settlement{
			Settlement: &eventpb.SettlementEvent{UserId: "u3"}}}, "u3"},
		{"transfer", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_Transfer{
			Transfer: &eventpb.TransferEvent{UserId: "u4"}}}, "u4"},
		{"order_status", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_OrderStatus{
			OrderStatus: &eventpb.OrderStatusEvent{UserId: "u5"}}}, "u5"},
		{"cancel_req", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_CancelReq{
			CancelReq: &eventpb.CancelRequested{UserId: "u6"}}}, "u6"},
		{"nil", nil, ""},
		{"empty", &eventpb.CounterJournalEvent{}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := userIDOf(c.evt); got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}
