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
		want string
	}{
		{"order_status", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_OrderStatus{
			OrderStatus: &eventpb.PerpOrderStatusEvent{UserId: "u1"}}}, "u1"},
		{"settlement", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Settlement{
			Settlement: &eventpb.PerpSettlementEvent{UserId: "u2"}}}, "u2"},
		{"margin", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Margin{
			Margin: &eventpb.PerpMarginEvent{UserId: "u3"}}}, "u3"},
		{"funding", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Funding{
			Funding: &eventpb.PerpFundingEvent{UserId: "u4"}}}, "u4"},
		{"liquidation", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Liquidation{
			Liquidation: &eventpb.PerpLiquidationEvent{UserId: "u5"}}}, "u5"},
		{"takeover", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Takeover{
			Takeover: &eventpb.PerpTakeoverEvent{UserId: "u6"}}}, "u6"},
		{"adl", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Adl{
			Adl: &eventpb.PerpAdlEvent{UserId: "u7"}}}, "u7"},
		{"nil", nil, ""},
		{"empty", &eventpb.PerpJournalEvent{}, ""},
	}
	for _, c := range cases {
		if got := perpUserIDOf(c.evt); got != c.want {
			t.Errorf("%s: perpUserIDOf = %q, want %q", c.name, got, c.want)
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
