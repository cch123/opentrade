package journal

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// MarketDataConsumerConfig configures the market-data reader. markprice tails
// both the spot and perp OrderBook streams (separate topics, physically
// isolated per ADR-0068 §0) to learn the index (spot mid) and basis (perp mid).
type MarketDataConsumerConfig struct {
	Brokers  []string
	ClientID string
	Topics   []string // e.g. ["market-data", "perp-market-data"]
}

// MarketDataConsumer feeds a Book from OrderBook Full frames. It is a live
// price projection, not an event-sourced consumer: it reads every partition
// directly (no group) from the latest offset, since only the current book
// matters and restart re-tails to a fresh Full within one Full interval
// (ADR-0055 cold start).
type MarketDataConsumer struct {
	cli    *kgo.Client
	book   *Book
	logger *zap.Logger
}

// NewMarketDataConsumer builds a ReadCommitted reader of the given topics.
func NewMarketDataConsumer(cfg MarketDataConsumerConfig, book *Book, logger *zap.Logger) (*MarketDataConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if len(cfg.Topics) == 0 {
		return nil, errors.New("journal: no market-data topics")
	}
	if book == nil {
		return nil, errors.New("journal: nil book")
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// Live price feed: start at the newest data; the next Full (ADR-0055,
		// every Full interval) seeds each symbol's mid.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &MarketDataConsumer{cli: cli, book: book, logger: logger}, nil
}

// Run polls and applies Full frames until ctx is cancelled or the client closes.
func (c *MarketDataConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("market-data fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(c.handleRecord)
	}
}

// Close shuts down the client.
func (c *MarketDataConsumer) Close() { c.cli.Close() }

func (c *MarketDataConsumer) handleRecord(rec *kgo.Record) {
	var pb eventpb.MarketDataEvent
	if err := proto.Unmarshal(rec.Value, &pb); err != nil {
		c.logger.Error("decode market-data",
			zap.String("topic", rec.Topic), zap.Int64("offset", rec.Offset), zap.Error(err))
		return
	}
	ob := pb.GetOrderBook()
	if ob == nil {
		return // PublicTrade / Kline — not a price source for the mark
	}
	if full := ob.GetFull(); full != nil {
		c.book.ApplyFull(pb.GetSymbol(), full)
	}
	// Delta frames are intentionally ignored (see book.go).
}
