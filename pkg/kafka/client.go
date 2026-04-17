// Package kafka wraps twmb/franz-go with OpenTrade defaults.
//
// MVP scope: plain producer + consumer. Transactional producer / EOS consumer
// will be added in a later MVP stage (needed by Counter and Match primaries).
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerConfig configures a plain idempotent producer (ADR-0005 events that
// are NOT part of a two-topic transactional write can use this).
type ProducerConfig struct {
	Brokers      []string
	ClientID     string
	RequireAcks  string // "all" (default) | "leader" | "none"
	MaxInflight  int    // default 5
}

// NewProducer constructs an idempotent producer.
func NewProducer(cfg ProducerConfig) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one broker is required")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(0), // latency-first; revisit for batch-heavy paths
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	switch cfg.RequireAcks {
	case "", "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "leader":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case "none":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	default:
		return nil, fmt.Errorf("kafka: unknown RequireAcks %q", cfg.RequireAcks)
	}
	if cfg.MaxInflight > 0 {
		opts = append(opts, kgo.MaxProduceRequestsInflightPerBroker(cfg.MaxInflight))
	}
	return kgo.NewClient(opts...)
}

// ConsumerConfig configures a consumer group consumer.
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	Group    string
	Topics   []string
}

// NewConsumer constructs a consumer group consumer reading committed events
// (ADR-0005 / 0017: we always set read_committed so we can consume transactional
// outputs without seeing aborted writes).
func NewConsumer(cfg ConsumerConfig) (*kgo.Client, error) {
	if cfg.Group == "" {
		return nil, fmt.Errorf("kafka: consumer group is required")
	}
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("kafka: at least one topic is required")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(), // we commit explicitly
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	return kgo.NewClient(opts...)
}

// Ping verifies the connection to the brokers is alive.
func Ping(ctx context.Context, cli *kgo.Client) error {
	return cli.Ping(ctx)
}
