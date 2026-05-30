package tradeevent

import (
	"context"
	"strings"
	"testing"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// NewPartitionConsumer is a thin wrapper around franz-go's ConsumePartitions;
// these tests pin the local config validation before a bad setup reaches a
// real broker. End-to-end consumption is covered by the vshard worker
// integration tests.
func TestNewPartitionConsumer_RejectsEmptyBrokers(t *testing.T) {
	_, err := NewPartitionConsumer(PartitionConsumerConfig{
		Partitions: []int32{0},
	}, dummyHandler{}, zap.NewNop())
	if err == nil || !strings.Contains(err.Error(), "brokers") {
		t.Fatalf("want brokers error, got %v", err)
	}
}

func TestNewPartitionConsumer_RejectsNoPartitions(t *testing.T) {
	_, err := NewPartitionConsumer(PartitionConsumerConfig{
		Brokers: []string{"localhost:9092"},
	}, dummyHandler{}, zap.NewNop())
	if err == nil || !strings.Contains(err.Error(), "partitions") {
		t.Fatalf("want partitions error, got %v", err)
	}
}

func TestNewPartitionConsumer_DefaultsTopic(t *testing.T) {
	c, err := NewPartitionConsumer(PartitionConsumerConfig{
		Brokers:    []string{"localhost:9092"},
		Partitions: []int32{0, 1},
	}, dummyHandler{}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	defer c.Close()
	if c.topic != "trade-event" {
		t.Fatalf("default topic = %q, want trade-event", c.topic)
	}
}

// dummyHandler is a no-op Handler for config-only tests.
type dummyHandler struct{}

func (dummyHandler) HandleTradeRecord(_ context.Context, _ *eventpb.TradeEvent, _ int32, _ int64) error {
	return nil
}
