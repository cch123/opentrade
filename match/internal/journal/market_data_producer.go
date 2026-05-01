package journal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// MarketDataConfig configures the OrderBook-only market-data producer
// (ADR-0055). Market-data is a projection — missing a Delta is tolerable
// because the next Full frame recovers downstream state, so this producer
// stays in idempotent mode even when the trade-event producer runs
// transactional. If stronger guarantees are later required, upgrade to a
// shared transaction with TradeProducer.
type MarketDataConfig struct {
	Brokers       []string
	ClientID      string
	ProducerID    string        // stamped onto EventMeta.producer_id
	Topic         string        // default "market-data"
	BatchSize     int           // default 64
	FlushInterval time.Duration // default 10ms
}

// MarketDataProducer publishes MarketDataOutput frames to the market-data
// topic as MarketDataEvent{OrderBook{Full|Delta}}. Partition key is symbol.
type MarketDataProducer struct {
	client *kgo.Client
	cfg    MarketDataConfig
	logger *zap.Logger
}

// NewMarketDataProducer constructs a producer in idempotent mode.
func NewMarketDataProducer(cfg MarketDataConfig, logger *zap.Logger) (*MarketDataProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.ProducerID == "" {
		return nil, errors.New("journal: ProducerID required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "market-data"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Millisecond
	}
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ProducerLinger(0),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &MarketDataProducer{client: cli, cfg: cfg, logger: logger}, nil
}

// PublishBatch sends a batch of MarketDataOutput frames. In idempotent mode
// franz-go dedupes / retries internally; the call returns after every record
// has been acked (or the first error surfaces).
func (p *MarketDataProducer) PublishBatch(ctx context.Context, batch []*sequencer.MarketDataOutput) error {
	if len(batch) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(batch))
	for _, md := range batch {
		evt, err := MarketDataOutputToEvent(md, p.cfg.ProducerID)
		if err != nil {
			return fmt.Errorf("convert md output: %w", err)
		}
		payload, err := proto.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal MarketDataEvent: %w", err)
		}
		recs = append(recs, &kgo.Record{
			Topic: p.cfg.Topic,
			Key:   []byte(md.Symbol),
			Value: payload,
		})
	}
	if err := p.client.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("produce market-data: %w", err)
	}
	return nil
}

// Pump drains mdOutbox and publishes frames in small batches until ctx is
// cancelled or the channel is closed.
func (p *MarketDataProducer) Pump(ctx context.Context, mdOutbox <-chan *sequencer.MarketDataOutput) {
	batch := make([]*sequencer.MarketDataOutput, 0, p.cfg.BatchSize)
	timer := time.NewTimer(p.cfg.FlushInterval)
	if !timer.Stop() {
		<-timer.C
	}
	timerArmed := false

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := p.PublishBatch(ctx, batch); err != nil {
			p.logger.Error("publish market-data batch",
				zap.Int("batch", len(batch)),
				zap.String("first_symbol", batch[0].Symbol),
				zap.Uint64("first_book_seq", batch[0].BookSeq),
				zap.Error(err))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case md, ok := <-mdOutbox:
			if !ok {
				flush()
				return
			}
			batch = append(batch, md)
			if !timerArmed {
				timer.Reset(p.cfg.FlushInterval)
				timerArmed = true
			}
			if len(batch) >= p.cfg.BatchSize {
				if timerArmed && !timer.Stop() {
					<-timer.C
				}
				timerArmed = false
				flush()
			}
		case <-timer.C:
			timerArmed = false
			flush()
		}
	}
}

// Close shuts down the underlying Kafka client.
func (p *MarketDataProducer) Close() { p.client.Close() }

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

// MarketDataOutputToEvent converts a sequencer MarketDataOutput to a wire
// MarketDataEvent carrying the OrderBook payload. ProducerID stamps the
// EventMeta so downstream correlation is possible.
func MarketDataOutputToEvent(md *sequencer.MarketDataOutput, producerID string) (*eventpb.MarketDataEvent, error) {
	if md == nil {
		return nil, errors.New("nil MarketDataOutput")
	}
	ob := &eventpb.OrderBook{}
	switch md.Kind {
	case sequencer.MDKindFull:
		ob.Data = &eventpb.OrderBook_Full{Full: &eventpb.OrderBookFull{
			Bids: levelsToProto(md.Bids, false),
			Asks: levelsToProto(md.Asks, false),
		}}
	case sequencer.MDKindDelta:
		ob.Data = &eventpb.OrderBook_Delta{Delta: &eventpb.OrderBookDelta{
			Bids: levelsToProto(md.Bids, true),
			Asks: levelsToProto(md.Asks, true),
		}}
	default:
		return nil, fmt.Errorf("unknown MarketDataKind %d", md.Kind)
	}
	return &eventpb.MarketDataEvent{
		Meta: &eventpb.EventMeta{
			TsUnixMs:   time.Now().UnixMilli(),
			ProducerId: producerID,
		},
		Symbol:     md.Symbol,
		MatchSeqId: md.BookSeq,
		Payload:    &eventpb.MarketDataEvent_OrderBook{OrderBook: ob},
	}, nil
}

// levelsToProto converts orderbook.Level slices to proto OrderBookLevel.
// allowZero=true keeps Qty=0 entries (Delta: "level deleted"); false skips
// them (Full never carries empty levels).
func levelsToProto(in []orderbook.Level, allowZero bool) []*eventpb.OrderBookLevel {
	if len(in) == 0 {
		return nil
	}
	out := make([]*eventpb.OrderBookLevel, 0, len(in))
	for _, lv := range in {
		if !allowZero && dec.IsZero(lv.Qty) {
			continue
		}
		out = append(out, &eventpb.OrderBookLevel{
			Price: lv.Price.String(),
			Qty:   lv.Qty.String(),
		})
	}
	return out
}
