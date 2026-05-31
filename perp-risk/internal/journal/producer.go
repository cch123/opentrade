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
)

type ProducerConfig struct {
	Brokers        []string
	ClientID       string
	Topic          string
	ProduceTimeout time.Duration
}

// Producer publishes RiskPool settlement events back to perp-journal so
// trade-dump/history can audit the same fund movement that perp-risk folded.
type Producer struct {
	cli    *kgo.Client
	topic  string
	logger *zap.Logger
	to     time.Duration
}

func NewProducer(cfg ProducerConfig, logger *zap.Logger) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("perp-risk journal producer: brokers required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "perp-journal"
	}
	if cfg.ProduceTimeout <= 0 {
		cfg.ProduceTimeout = 5 * time.Second
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
	return &Producer{cli: cli, topic: cfg.Topic, logger: logger, to: cfg.ProduceTimeout}, nil
}

func (p *Producer) EmitRiskPoolSettlement(evt *eventpb.PerpJournalEvent) {
	payload, err := proto.Marshal(evt)
	if err != nil {
		p.logger.Error("marshal risk-pool settlement", zap.Error(err))
		return
	}
	key := ""
	if s := evt.GetRiskPoolSettlement(); s != nil {
		key = s.GetLotId()
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.to)
	defer cancel()
	if err := p.cli.ProduceSync(ctx, &kgo.Record{Topic: p.topic, Key: []byte(key), Value: payload}).FirstErr(); err != nil {
		p.logger.Error("emit risk-pool settlement", zap.String("lot_id", key), zap.Error(err))
	}
}

func (p *Producer) Close() { p.cli.Close() }
