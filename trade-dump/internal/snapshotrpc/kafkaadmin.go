package snapshotrpc

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// KafkaAdminClient adapts a *kadm.Client to the KafkaAdmin interface
// snapshotrpc.Server consumes. Separated from the gRPC handler so
// the server can be tested without a live Kafka cluster.
//
// ListEndOffset uses kadm.Client.ListEndOffsets under the hood,
// which issues a ListOffsets request with timestamp=-1 and
// isolation_level=READ_UNCOMMITTED (the kadm default). This is
// exactly the ADR-0064 §2.3 contract — we want the physical cursor
// (LEO) of counter-journal's partition, not the ReadCommitted LSO,
// because (a) trade-dump's shadow consumer is ReadCommitted and
// will skip aborted records as it applies past them, and (b) the
// sentinel commit marker Counter produces (ADR-0064 §3 step ③)
// stabilises LEO by construction.
type KafkaAdminClient struct {
	cli *kadm.Client
}

// NewKafkaAdminClient wraps an existing *kadm.Client. The caller
// owns the lifecycle of the underlying kgo.Client.
func NewKafkaAdminClient(cli *kadm.Client) *KafkaAdminClient {
	return &KafkaAdminClient{cli: cli}
}

// ListEndOffset returns the LEO for (topic, partition) or the first
// encountered error. Partition-level errors from the broker are
// bubbled up as-is so handler code can choose to map them to
// Unavailable (retryable) vs. other codes.
func (k *KafkaAdminClient) ListEndOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	if k.cli == nil {
		return 0, fmt.Errorf("kafka admin client nil")
	}
	offsets, err := k.cli.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list end offsets: %w", err)
	}
	topicOffsets, ok := offsets[topic]
	if !ok {
		return 0, fmt.Errorf("topic %q not found in ListEndOffsets response", topic)
	}
	p, ok := topicOffsets[partition]
	if !ok {
		return 0, fmt.Errorf("partition %d not found in topic %q response", partition, topic)
	}
	if p.Err != nil {
		return 0, fmt.Errorf("partition %d list offsets error: %w", partition, p.Err)
	}
	return p.Offset, nil
}
