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
	"github.com/xargin/opentrade/pkg/shard"
	"github.com/xargin/opentrade/push/internal/hub"
	"github.com/xargin/opentrade/push/internal/ws"
)

// PerpPrivateConfig configures the perp-journal consumer (ADR-0068 M7).
type PerpPrivateConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "perp-journal"

	// Sticky ownership, same as PrivateConsumer: events whose user_id hashes
	// to another instance are dropped (ADR-0033). TotalInstances <= 1 disables.
	InstanceOrdinal int
	TotalInstances  int
}

// PerpPrivateConsumer routes perp-journal events to the owning user's WS
// connections on the StreamPerpUser stream. Mirrors PrivateConsumer; the only
// differences are the topic, the event type, and the stream name.
type PerpPrivateConsumer struct {
	cli             *kgo.Client
	hub             *hub.Hub
	logger          *zap.Logger
	instanceOrdinal int
	totalInstances  int
}

// NewPerpPrivate builds a perp-journal consumer starting at the topic tail.
func NewPerpPrivate(cfg PerpPrivateConfig, h *hub.Hub, logger *zap.Logger) (*PerpPrivateConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("consumer: brokers required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: group id required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "perp-journal"
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
	return &PerpPrivateConsumer{
		cli: cli, hub: h, logger: logger,
		instanceOrdinal: cfg.InstanceOrdinal, totalInstances: cfg.TotalInstances,
	}, nil
}

// Close shuts down the Kafka client.
func (c *PerpPrivateConsumer) Close() { c.cli.Close() }

// Run polls perp-journal and dispatches per-user sends until ctx is cancelled.
func (c *PerpPrivateConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("perp-journal fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(c.dispatch)
	}
}

func (c *PerpPrivateConsumer) dispatch(rec *kgo.Record) {
	var evt eventpb.PerpJournalEvent
	if err := proto.Unmarshal(rec.Value, &evt); err != nil {
		c.logger.Error("decode perp-journal",
			zap.Int32("partition", rec.Partition), zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	userID := perpUserIDOf(&evt)
	if userID == "" {
		return
	}
	if !c.ownsUser(userID) {
		return
	}
	payload, err := protojson.Marshal(&evt)
	if err != nil {
		c.logger.Error("encode perp-journal json", zap.Error(err))
		return
	}
	frame, err := ws.EncodeData(ws.StreamPerpUser, payload)
	if err != nil {
		c.logger.Error("encode ws frame", zap.Error(err))
		return
	}
	c.hub.SendUser(userID, frame)
}

func (c *PerpPrivateConsumer) ownsUser(userID string) bool {
	if c.totalInstances <= 1 {
		return true
	}
	return shard.OwnsUser(c.instanceOrdinal, c.totalInstances, userID)
}

// perpUserIDOf extracts the owning user from any perp-journal payload.
func perpUserIDOf(evt *eventpb.PerpJournalEvent) string {
	if evt == nil {
		return ""
	}
	switch p := evt.Payload.(type) {
	case *eventpb.PerpJournalEvent_OrderStatus:
		if p.OrderStatus != nil {
			return p.OrderStatus.UserId
		}
	case *eventpb.PerpJournalEvent_Settlement:
		if p.Settlement != nil {
			return p.Settlement.UserId
		}
	case *eventpb.PerpJournalEvent_Margin:
		if p.Margin != nil {
			return p.Margin.UserId
		}
	case *eventpb.PerpJournalEvent_Funding:
		if p.Funding != nil {
			return p.Funding.UserId
		}
	case *eventpb.PerpJournalEvent_Liquidation:
		if p.Liquidation != nil {
			return p.Liquidation.UserId
		}
	case *eventpb.PerpJournalEvent_Takeover:
		if p.Takeover != nil {
			return p.Takeover.UserId
		}
	case *eventpb.PerpJournalEvent_Adl:
		if p.Adl != nil {
			return p.Adl.UserId
		}
	}
	return ""
}
