package marketcache

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// ConsumerConfig wires a market-data consumer into the cache.
type ConsumerConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "market-data"
}

// Consumer feeds OrderBook Full + KlineClosed events from Kafka into a
// Cache. It intentionally ignores OrderBook Delta / PublicTrade /
// KlineUpdate: those streams are for live UI and don't belong in the
// reconnect snapshot.
type Consumer struct {
	cli    *kgo.Client
	cache  *Cache
	logger *zap.Logger
	topic  string
}

// NewConsumer constructs a consumer that starts at topic tail on a fresh
// group; offsets are never committed because the cache is derived from the
// stream and survives no restart.
func NewConsumer(cfg ConsumerConfig, cache *Cache, logger *zap.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("marketcache: brokers required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("marketcache: group id required")
	}
	if cache == nil {
		return nil, errors.New("marketcache: cache required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "market-data"
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
	return &Consumer{cli: cli, cache: cache, logger: logger, topic: cfg.Topic}, nil
}

// Close shuts down the Kafka client.
func (c *Consumer) Close() { c.cli.Close() }

// Run polls market-data and applies the relevant payloads to the cache
// until ctx is cancelled or the client is closed.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("marketcache fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			var evt eventpb.MarketDataEvent
			if err := proto.Unmarshal(rec.Value, &evt); err != nil {
				c.logger.Error("marketcache decode",
					zap.Int32("partition", rec.Partition),
					zap.Int64("offset", rec.Offset),
					zap.Error(err))
				return
			}
			c.apply(&evt)
		})
	}
}

func (c *Consumer) apply(evt *eventpb.MarketDataEvent) {
	switch p := evt.Payload.(type) {
	case *eventpb.MarketDataEvent_OrderBook:
		if p.OrderBook == nil {
			return
		}
		if full, ok := p.OrderBook.Data.(*eventpb.OrderBook_Full); ok && full.Full != nil {
			c.cache.PutOrderBookFull(evt.Symbol, evt.MatchSeqId, full.Full)
		}
	case *eventpb.MarketDataEvent_KlineClosed:
		c.cache.AppendKlineClosed(evt.Symbol, p.KlineClosed)
	}
}
