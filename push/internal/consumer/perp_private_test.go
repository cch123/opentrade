package consumer

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

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
		{"position_config", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_PositionConfig{
			PositionConfig: &eventpb.PerpPositionConfigEvent{UserId: 1008}}}, 1008},
		{"margin_adjustment", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_MarginAdjustment{
			MarginAdjustment: &eventpb.PerpMarginAdjustmentEvent{UserId: 1009}}}, 1009},
		{"customer_risk_limit", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_CustomerRiskLimit{
			CustomerRiskLimit: &eventpb.PerpCustomerRiskLimitEvent{UserId: 1010}}}, 1010},
		{"invariant_breach", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_InvariantBreach{
			InvariantBreach: &eventpb.PerpInvariantBreachEvent{UserId: 1011}}}, 1011},
		{"customer_fee", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_CustomerFee{
			CustomerFee: &eventpb.PerpCustomerFeeEvent{UserId: 1012}}}, 1012},
		// System-level: no user attribution → never user-routed.
		{"risk_pool_settlement", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_RiskPoolSettlement{
			RiskPoolSettlement: &eventpb.RiskPoolSettlementEvent{LotId: "lot-1"}}}, 0},
		{"zero_user_id", &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_OrderStatus{
			OrderStatus: &eventpb.PerpOrderStatusEvent{}}}, 0},
		{"nil", nil, 0},
		{"empty", &eventpb.PerpJournalEvent{}, 0},
	}
	for _, c := range cases {
		if got := perpUserIDOf(c.evt); got != c.want {
			t.Errorf("%s: perpUserIDOf = %d, want %d", c.name, got, c.want)
		}
	}
}

// TestPerpUserIDOf_OneofExhaustive walks the PerpJournalEvent payload oneof by
// protobuf reflection: every payload carrying a user_id field must route to
// that user's private stream, and payloads without one must return 0 (not
// user-routed). A payload type added to the proto without a matching
// perpUserIDOf case falls into the switch default and fails here — the guard
// against repeating the omission that left InvariantBreach silently dropped
// by dispatch (mirrors TestJournalPartitionKey_OneofExhaustive in
// perp-counter/internal/journal).
func TestPerpUserIDOf_OneofExhaustive(t *testing.T) {
	const userID = uint64(7777)

	oneof := (&eventpb.PerpJournalEvent{}).ProtoReflect().Descriptor().Oneofs().ByName("payload")
	if oneof == nil {
		t.Fatal("PerpJournalEvent: payload oneof not found")
	}
	fields := oneof.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		evt := &eventpb.PerpJournalEvent{}
		payload := evt.ProtoReflect().Mutable(fd).Message()
		userField := payload.Descriptor().Fields().ByName("user_id")
		if userField == nil {
			if got := perpUserIDOf(evt); got != 0 {
				t.Errorf("%s: perpUserIDOf = %d, want 0 (payload has no user_id)", fd.Name(), got)
			}
			continue
		}
		if userField.Kind() != protoreflect.Uint64Kind {
			t.Fatalf("%s: user_id is %v, want uint64", fd.Name(), userField.Kind())
		}
		payload.Set(userField, protoreflect.ValueOfUint64(userID))
		if got := perpUserIDOf(evt); got != userID {
			t.Errorf("%s: perpUserIDOf = %d, want %d — user-attributed payloads must route to the user's private stream", fd.Name(), got, userID)
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
