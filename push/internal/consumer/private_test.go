package consumer

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestUserIDOf_AllPayloads(t *testing.T) {
	cases := []struct {
		name string
		evt  *eventpb.CounterJournalEvent
		want uint64
	}{
		{"freeze", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_Freeze{
			Freeze: &eventpb.FreezeEvent{UserId: 1001}}}, 1001},
		{"unfreeze", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_Unfreeze{
			Unfreeze: &eventpb.UnfreezeEvent{UserId: 1002}}}, 1002},
		{"settlement", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_Settlement{
			Settlement: &eventpb.SettlementEvent{UserId: 1003}}}, 1003},
		{"transfer", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_Transfer{
			Transfer: &eventpb.TransferEvent{UserId: 1004}}}, 1004},
		{"order_status", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_OrderStatus{
			OrderStatus: &eventpb.OrderStatusEvent{UserId: 1005}}}, 1005},
		{"cancel_req", &eventpb.CounterJournalEvent{Payload: &eventpb.CounterJournalEvent_CancelReq{
			CancelReq: &eventpb.CancelRequested{UserId: 1006}}}, 1006},
		{"nil", nil, 0},
		{"empty", &eventpb.CounterJournalEvent{}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := userIDOf(c.evt); got != c.want {
				t.Errorf("got %d want %d", got, c.want)
			}
		})
	}
}
