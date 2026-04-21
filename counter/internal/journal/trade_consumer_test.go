package journal

import (
	"context"
	"strings"
	"testing"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// NewTradePartitionConsumer is a thin wrapper around franz-go's
// ConsumePartitions; the value in testing it is catching bad config
// paths before they hit a real broker. End-to-end consumption is
// covered by the vshard worker integration test (ADR-0058 phase 3b-2).

func TestNewTradePartitionConsumer_RejectsEmptyBrokers(t *testing.T) {
	_, err := NewTradePartitionConsumer(TradePartitionConsumerConfig{
		Partitions: []int32{0},
	}, dummyTradeHandler{}, zap.NewNop())
	if err == nil || !strings.Contains(err.Error(), "brokers") {
		t.Fatalf("want brokers error, got %v", err)
	}
}

func TestNewTradePartitionConsumer_RejectsNoPartitions(t *testing.T) {
	_, err := NewTradePartitionConsumer(TradePartitionConsumerConfig{
		Brokers: []string{"localhost:9092"},
	}, dummyTradeHandler{}, zap.NewNop())
	if err == nil || !strings.Contains(err.Error(), "partitions") {
		t.Fatalf("want partitions error, got %v", err)
	}
}

func TestNewTradePartitionConsumer_DefaultsTopic(t *testing.T) {
	c, err := NewTradePartitionConsumer(TradePartitionConsumerConfig{
		Brokers:    []string{"localhost:9092"},
		Partitions: []int32{0, 1},
	}, dummyTradeHandler{}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	defer c.Close()
	if c.topic != "trade-event" {
		t.Fatalf("default topic = %q, want trade-event", c.topic)
	}
}

// dummyTradeHandler is a no-op TradeHandler for config-only tests.
type dummyTradeHandler struct{}

func (dummyTradeHandler) HandleTradeEvent(_ context.Context, _ *eventpb.TradeEvent) error {
	return nil
}
func (dummyTradeHandler) HandleTradeRecord(_ context.Context, _ *eventpb.TradeEvent, _ int32, _ int64) error {
	return nil
}
