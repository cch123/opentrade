package service

import (
	"context"
	"errors"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func assertAsyncRecordFailure(t *testing.T, svc *Service, evt *eventpb.TradeEvent, want error) {
	t.Helper()
	count := int32(-1)
	var callbackErr error
	svc.HandleTradeEventAsync(context.Background(), evt, func(n int32) {
		count = n
	}, func(err error) {
		callbackErr = err
	})
	if count != 1 {
		t.Fatalf("onCount = %d, want 1 failed work item", count)
	}
	if callbackErr == nil {
		t.Fatal("callback error = nil; record would be checkpointed")
	}
	if want != nil && !errors.Is(callbackErr, want) {
		t.Fatalf("callback error = %v, want %v", callbackErr, want)
	}
}

func TestHandleTradeEventAsync_UnknownPayloadFailsRecord(t *testing.T) {
	svc, _, _ := newFixture(t)
	assertAsyncRecordFailure(t, svc, &eventpb.TradeEvent{}, ErrUnknownPayload)
}

func TestHandleTradeEventAsync_ParseFailureFailsRecord(t *testing.T) {
	svc, _, _ := newFixture(t)
	evt := &eventpb.TradeEvent{
		Payload: &eventpb.TradeEvent_Trade{Trade: &eventpb.Trade{
			TradeId:             "bad-trade",
			MakerFilledQtyAfter: "not-a-decimal",
		}},
	}
	assertAsyncRecordFailure(t, svc, evt, nil)
}
