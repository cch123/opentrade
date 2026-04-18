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

// PrivateConfig configures the counter-journal consumer.
type PrivateConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "counter-journal"

	// InstanceOrdinal and TotalInstances enable sticky ownership: events
	// whose user_id hashes to a different instance are dropped (ADR-0033).
	// TotalInstances <= 1 disables the filter and every event is kept (the
	// legacy single-instance behavior).
	InstanceOrdinal int
	TotalInstances  int
}

// PrivateConsumer routes counter-journal events to the owning user's WS
// connections via hub.SendUser.
//
// MVP-13 drops events whose user_id doesn't belong to this instance per
// shard.Index. We still consume every partition (ADR-0033 explains the
// trade-off) — strict partition-level ownership alignment is deferred to
// MVP-13b when Counter's producer gets a custom partitioner.
type PrivateConsumer struct {
	cli             *kgo.Client
	hub             *hub.Hub
	logger          *zap.Logger
	instanceOrdinal int
	totalInstances  int
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
	return &PrivateConsumer{
		cli:             cli,
		hub:             h,
		logger:          logger,
		instanceOrdinal: cfg.InstanceOrdinal,
		totalInstances:  cfg.TotalInstances,
	}, nil
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
	if !c.ownsUser(userID) {
		return // not routed to this push instance; LB's sticky peer will deliver
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

// ownsUser reports whether userID is routed to this push instance. Returns
// true when TotalInstances <= 1 (feature disabled, single-instance mode).
func (c *PrivateConsumer) ownsUser(userID string) bool {
	if c.totalInstances <= 1 {
		return true
	}
	return shard.OwnsUser(c.instanceOrdinal, c.totalInstances, userID)
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
