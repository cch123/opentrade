package consumer

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"go.uber.org/zap"
)

func TestPerpUserIDOf_AllPayloads(t *testing.T) {
	cases := []struct {
		name string
		evt  *eventpb.PerpJournalEvent
		want uint64
	}{
		{"order_status", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_OrderStatus{
			OrderStatus: &eventpb.PerpOrderStatusEvent{UserId: 1001}}}, 1001},
		{"settlement", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Settlement{
			Settlement: &eventpb.PerpSettlementEvent{UserId: 1002}}}, 1002},
		{"margin", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Margin{
			Margin: &eventpb.PerpMarginEvent{UserId: 1003}}}, 1003},
		{"funding", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Funding{
			Funding: &eventpb.PerpFundingEvent{UserId: 1004}}}, 1004},
		{"liquidation", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Liquidation{
			Liquidation: &eventpb.PerpLiquidationEvent{UserId: 1005}}}, 1005},
		{"takeover", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Takeover{
			Takeover: &eventpb.PerpTakeoverEvent{UserId: 1006}}}, 1006},
		{"adl", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Adl{
			Adl: &eventpb.PerpAdlEvent{UserId: 1007}}}, 1007},
		{"nil", nil, 0},
		{"empty", &eventpb.PerpJournalEvent{}, 0},
	}
	for _, c := range cases {
		if got := perpUserIDOf(c.evt); got != c.want {
			t.Errorf("%s: perpUserIDOf = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestNewPerpPrivate_Validation(t *testing.T) {
	logger := zap.NewNop()
	if _, err := NewPerpPrivate(PerpPrivateConfig{GroupID: "g"}, nil, logger); err == nil {
		t.Error("expected error for empty brokers")
	}
	if _, err := NewPerpPrivate(PerpPrivateConfig{Brokers: []string{"localhost:9092"}}, nil, logger); err == nil {
		t.Error("expected error for empty group")
	}
}
