// Package consumer runs the two Kafka consumers that feed the WebSocket hub:
// market-data (public fan-out) and counter-journal (per-user private flow).
//
// Both consumers are at-least-once and do not commit offsets — Push keeps no
// state, and a restart resumes at topic tail. Missed events during a restart
// are recovered by the client via a BFF state snapshot on reconnect (ADR-0022).
package consumer

import (
	"context"
	"encoding/json"
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

// MarketDataConfig configures the market-data consumer.
type MarketDataConfig struct {
	Brokers  []string
	ClientID string
	GroupID  string
	Topic    string // default "market-data"
}

// MarketDataConsumer dispatches MarketDataEvents to per-stream subscribers.
type MarketDataConsumer struct {
	cli    *kgo.Client
	hub    *hub.Hub
	logger *zap.Logger
}

// NewMarketData builds a consumer starting at the topic tail (so new pushes
// start receiving live data; history stays in BFF snapshots / MySQL).
func NewMarketData(cfg MarketDataConfig, h *hub.Hub, logger *zap.Logger) (*MarketDataConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("consumer: brokers required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("consumer: group id required")
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
	return &MarketDataConsumer{cli: cli, hub: h, logger: logger}, nil
}

// Close shuts down the Kafka client.
func (c *MarketDataConsumer) Close() { c.cli.Close() }

// Run polls market-data and fans out to hub subscribers until ctx is
// cancelled or the client is closed.
func (c *MarketDataConsumer) Run(ctx context.Context) error {
	for {
		fetches := c.cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.Warn("market-data fetch error",
				zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			c.dispatch(rec)
		})
	}
}

func (c *MarketDataConsumer) dispatch(rec *kgo.Record) {
	var evt eventpb.MarketDataEvent
	if err := proto.Unmarshal(rec.Value, &evt); err != nil {
		c.logger.Error("decode market-data",
			zap.Int32("partition", rec.Partition),
			zap.Int64("offset", rec.Offset),
			zap.Error(err))
		return
	}
	streamKey, payload, coalescable := buildStreamFrame(&evt)
	if streamKey == "" || payload == nil {
		return
	}
	frame, err := ws.EncodeData(streamKey, payload)
	if err != nil {
		c.logger.Error("encode ws data", zap.Error(err))
		return
	}
	if coalescable {
		c.hub.BroadcastStreamCoalesce(streamKey, streamKey, frame)
		return
	}
	c.hub.BroadcastStream(streamKey, frame)
}

// buildStreamFrame derives the stream key and the marshalled payload JSON
// for a MarketDataEvent. Returns ("", nil, false) for unknown / unsupported
// events. The bool is true when the frame is safe to coalesce — i.e. the
// latest payload fully subsumes any in-flight older one. Today only
// KlineUpdate and OrderBook Full qualify: OrderBook Delta carries an
// incremental diff (losing one means a stale level), KlineClosed is a
// transition event, PublicTrade is per-fill.
func buildStreamFrame(evt *eventpb.MarketDataEvent) (string, json.RawMessage, bool) {
	symbol := evt.Symbol
	switch p := evt.Payload.(type) {
	case *eventpb.MarketDataEvent_PublicTrade:
		if p.PublicTrade == nil {
			return "", nil, false
		}
		b, err := protojson.Marshal(p.PublicTrade)
		if err != nil {
			return "", nil, false
		}
		return "trade@" + symbol, b, false
	case *eventpb.MarketDataEvent_OrderBook:
		if p.OrderBook == nil {
			return "", nil, false
		}
		return buildOrderBookFrame(symbol, evt.MatchSeqId, p.OrderBook)
	case *eventpb.MarketDataEvent_KlineUpdate:
		if p.KlineUpdate == nil || p.KlineUpdate.Kline == nil {
			return "", nil, false
		}
		key := "kline@" + symbol + ":" + klineIntervalLabel(p.KlineUpdate.Kline.Interval)
		b, err := protojson.Marshal(p.KlineUpdate)
		if err != nil {
			return "", nil, false
		}
		return key, b, true
	case *eventpb.MarketDataEvent_KlineClosed:
		if p.KlineClosed == nil || p.KlineClosed.Kline == nil {
			return "", nil, false
		}
		key := "kline@" + symbol + ":" + klineIntervalLabel(p.KlineClosed.Kline.Interval)
		b, err := protojson.Marshal(p.KlineClosed)
		if err != nil {
			return "", nil, false
		}
		return key, b, false
	default:
		return "", nil, false
	}
}

// buildOrderBookFrame marshals a Full frame to `depth.snapshot@{symbol}` and
// a Delta frame to `depth@{symbol}`, attaching match_seq_id for client-side
// chain validation (ADR-0055). Full is coalescable (a newer Full subsumes an
// older one); Delta is not.
func buildOrderBookFrame(symbol string, matchSeq uint64, ob *eventpb.OrderBook) (string, json.RawMessage, bool) {
	switch d := ob.Data.(type) {
	case *eventpb.OrderBook_Full:
		if d.Full == nil {
			return "", nil, false
		}
		b, err := json.Marshal(struct {
			Symbol     string                     `json:"symbol"`
			MatchSeqID uint64                     `json:"match_seq_id"`
			Bids       []*eventpb.OrderBookLevel  `json:"bids"`
			Asks       []*eventpb.OrderBookLevel  `json:"asks"`
		}{Symbol: symbol, MatchSeqID: matchSeq, Bids: d.Full.Bids, Asks: d.Full.Asks})
		if err != nil {
			return "", nil, false
		}
		return "depth.snapshot@" + symbol, b, true
	case *eventpb.OrderBook_Delta:
		if d.Delta == nil {
			return "", nil, false
		}
		b, err := json.Marshal(struct {
			Symbol     string                     `json:"symbol"`
			MatchSeqID uint64                     `json:"match_seq_id"`
			Bids       []*eventpb.OrderBookLevel  `json:"bids"`
			Asks       []*eventpb.OrderBookLevel  `json:"asks"`
		}{Symbol: symbol, MatchSeqID: matchSeq, Bids: d.Delta.Bids, Asks: d.Delta.Asks})
		if err != nil {
			return "", nil, false
		}
		return "depth@" + symbol, b, false
	}
	return "", nil, false
}

func klineIntervalLabel(i eventpb.KlineInterval) string {
	switch i {
	case eventpb.KlineInterval_KLINE_INTERVAL_1M:
		return "1m"
	case eventpb.KlineInterval_KLINE_INTERVAL_5M:
		return "5m"
	case eventpb.KlineInterval_KLINE_INTERVAL_15M:
		return "15m"
	case eventpb.KlineInterval_KLINE_INTERVAL_1H:
		return "1h"
	case eventpb.KlineInterval_KLINE_INTERVAL_1D:
		return "1d"
	default:
		return "unknown"
	}
}
