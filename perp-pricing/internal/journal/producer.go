package journal

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
)

// MarkProducerConfig configures the mark-price producer.
type MarkProducerConfig struct {
	Brokers    []string
	ClientID   string
	ProducerID string // stamped onto EventMeta.producer_id
	Topic      string // default "mark-price"
}

// MarkProducer publishes MarkPriceEvent (MarkTick / FundingTick) keyed by
// symbol. Idempotent mode: the mark-price stream is a high-frequency estimate,
// so a dropped tick is recovered by the next one (ADR-0068 §5) — no transaction
// needed. The funding watermark (funding_round_seen) on the perp-counter side
// makes a redelivered FundingTick safe too.
type MarkProducer struct {
	cli    *kgo.Client
	cfg    MarkProducerConfig
	logger *zap.Logger
}

// NewMarkProducer constructs an idempotent producer.
func NewMarkProducer(cfg MarkProducerConfig, logger *zap.Logger) (*MarkProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.ProducerID == "" {
		return nil, errors.New("journal: ProducerID required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "mark-price"
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
	return &MarkProducer{cli: cli, cfg: cfg, logger: logger}, nil
}

// PublishMarkTick emits a high-frequency mark/index/funding-estimate tick.
// The stale/degraded bits are part of ADR-0069's safety contract: consumers
// may keep displaying the frozen mark when stale, but must not liquidate.
func (p *MarkProducer) PublishMarkTick(ctx context.Context, symbol string, mark, index, fundingEst dec.Decimal, tsMs int64, indexStale, indexDegraded bool) error {
	return p.publish(ctx, symbol, &eventpb.MarkPriceEvent{
		Meta:   &eventpb.EventMeta{TsUnixMs: tsMs, ProducerId: p.cfg.ProducerID},
		Symbol: symbol,
		Payload: &eventpb.MarkPriceEvent_Tick{Tick: &eventpb.MarkTick{
			MarkPrice: mark.String(), IndexPrice: index.String(),
			FundingRate: fundingEst.String(), TsUnixMs: tsMs,
			IndexStale: indexStale, IndexDegraded: indexDegraded,
		}},
	})
}

// PublishFundingTick emits a settled funding round at an interval boundary
// (ADR-0068 §7). roundID is the boundary's unix seconds; funding_round_id is the
// idempotency key perp-counter settles on.
func (p *MarkProducer) PublishFundingTick(ctx context.Context, symbol string, roundID int64, rate, mark dec.Decimal, tsMs int64) error {
	return p.publish(ctx, symbol, &eventpb.MarkPriceEvent{
		Meta:   &eventpb.EventMeta{TsUnixMs: tsMs, ProducerId: p.cfg.ProducerID},
		Symbol: symbol,
		Payload: &eventpb.MarkPriceEvent_Funding{Funding: &eventpb.FundingTick{
			FundingRoundId: FundingRoundID(symbol, roundID),
			FundingRate:    rate.String(), MarkPrice: mark.String(), TsUnixMs: tsMs,
		}},
	})
}

func (p *MarkProducer) publish(ctx context.Context, symbol string, evt *eventpb.MarkPriceEvent) error {
	payload, err := proto.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal mark-price: %w", err)
	}
	return p.cli.ProduceSync(ctx, &kgo.Record{
		Topic: p.cfg.Topic, Key: []byte(symbol), Value: payload,
	}).FirstErr()
}

// Close flushes and closes the client.
func (p *MarkProducer) Close() { p.cli.Close() }

// FundingRoundID builds the funding round idempotency key
// "<symbol>:<unix-seconds>" (ADR-0068 §7 / mark_price.proto).
func FundingRoundID(symbol string, roundUnixSec int64) string {
	return fmt.Sprintf("%s:%d", symbol, roundUnixSec)
}
