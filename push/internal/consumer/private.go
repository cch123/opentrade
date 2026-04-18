package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/push/internal/hub"
	"github.com/xargin/opentrade/push/internal/ws"
)

// PrivateConfig configures the counter-journal consumer.
type PrivateConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "counter-journal"
}

// PrivateConsumer routes counter-journal events to the owning user's WS
// connections via hub.SendUser.
//
// MVP-7 consumes all partitions on a single instance. Multi-instance
// partition routing per ADR-0022 (partition_id % num_instances == instance_id)
// is deferred — the knobs are already in the consumer-group id.
type PrivateConsumer struct {
	cli    *kgo.Client
	hub    *hub.Hub
	logger *zap.Logger
}

// NewPrivate builds a counter-journal consumer starting at the topic tail.
func NewPrivate(cfg PrivateConfig, h *hub.Hub, logger *zap.Logger) (*PrivateConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("consumer: brokers required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: group id required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "counter-journal"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &PrivateConsumer{cli: cli, hub: h, logger: logger}, nil
}

// Close shuts down the Kafka client.
func (c *PrivateConsumer) Close() { c.cli.Close() }

// Run polls counter-journal and dispatches per-user sends until ctx is
// cancelled or the client is closed.
func (c *PrivateConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("counter-journal fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			c.dispatch(rec)
		})
	}
}

func (c *PrivateConsumer) dispatch(rec *kgo.Record) {
	var evt eventpb.CounterJournalEvent
	if err := proto.Unmarshal(rec.Value, &evt); err != nil {
		c.logger.Error("decode counter-journal",
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Error(err))
		return
	}
	userID := userIDOf(&evt)
	if userID == "" {
		return // unclassified event; skip
	}
	payload, err := protojson.Marshal(&evt)
	if err != nil {
		c.logger.Error("encode counter-journal json", zap.Error(err))
		return
	}
	frame, err := ws.EncodeData(ws.StreamUser, payload)
	if err != nil {
		c.logger.Error("encode ws frame", zap.Error(err))
		return
	}
	c.hub.SendUser(userID, frame)
}

func userIDOf(evt *eventpb.CounterJournalEvent) string {
	if evt == nil {
		return ""
	}
	switch p := evt.Payload.(type) {
	case *eventpb.CounterJournalEvent_Freeze:
		if p.Freeze != nil {
			return p.Freeze.UserId
		}
	case *eventpb.CounterJournalEvent_Unfreeze:
		if p.Unfreeze != nil {
			return p.Unfreeze.UserId
		}
	case *eventpb.CounterJournalEvent_Settlement:
		if p.Settlement != nil {
			return p.Settlement.UserId
		}
	case *eventpb.CounterJournalEvent_Transfer:
		if p.Transfer != nil {
			return p.Transfer.UserId
		}
	case *eventpb.CounterJournalEvent_OrderStatus:
		if p.OrderStatus != nil {
			return p.OrderStatus.UserId
		}
	case *eventpb.CounterJournalEvent_CancelReq:
		if p.CancelReq != nil {
			return p.CancelReq.UserId
		}
	}
	return ""
}
