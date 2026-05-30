package journal

import (
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestOrderEventTopicFor(t *testing.T) {
	cases := []struct {
		prefix, symbol, want string
	}{
		{"order-event", "BTC-USDT-PERP", "order-event-BTC-USDT-PERP"},
		{"order-event", "ETH-USDT-PERP", "order-event-ETH-USDT-PERP"},
		{"perp-order", "BTC-USDT-PERP", "perp-order-BTC-USDT-PERP"},
		{"order-event", "", "order-event"}, // missing symbol → bare prefix
	}
	for _, c := range cases {
		if got := orderEventTopicFor(c.prefix, c.symbol); got != c.want {
			t.Errorf("orderEventTopicFor(%q,%q) = %q, want %q", c.prefix, c.symbol, got, c.want)
		}
	}
}

func TestOrderEventKey(t *testing.T) {
	if got := orderEventKey("BTC-USDT-PERP"); got != "BTC-USDT-PERP" {
		t.Errorf("orderEventKey = %q, want symbol", got)
	}
}

func TestJournalPartitionKey_AllPayloads(t *testing.T) {
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
		{"empty", &eventpb.PerpJournalEvent{}, ""},
	}
	for _, c := range cases {
		if got := journalPartitionKey(c.evt); got != c.want {
			t.Errorf("%s: journalPartitionKey = %q, want %q", c.name, got, c.want)
		}
	}
}
