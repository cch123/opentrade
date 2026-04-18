package journal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/xargin/opentrade/match/internal/sequencer"
)

// ProducerConfig configures the trade-event Kafka producer.
//
// When TransactionalID is set, the producer runs in transactional mode so the
// shard's stable id fences stale primaries (ADR-0017, MVP-12b):
//   - Every flushed batch is wrapped in BeginTransaction / EndTransaction.
//   - A new primary with the same TransactionalID calling InitProducerID
//     (implicit on first BeginTransaction) rotates the producer epoch and
//     makes any older primary's in-flight transactions abort on commit.
//
// When TransactionalID is empty the producer stays in legacy idempotent
// mode (ADR-0005 pre-MVP-12b) — used by tests and by --ha-mode=disabled
// single-node dev.
type ProducerConfig struct {
	Brokers         []string
	ClientID        string
	ProducerID      string        // stamped onto EventMeta.producer_id
	Topic           string        // default "trade-event"
	TransactionalID string        // empty = idempotent mode
	BatchSize       int           // flush after this many outputs; default 32
	FlushInterval   time.Duration // flush if idle longer than this; default 10ms
}

// TradeProducer publishes sequencer.Output emissions to the trade-event topic.
type TradeProducer struct {
	client *kgo.Client
	cfg    ProducerConfig
	logger *zap.Logger
	// transactional is cached from cfg.TransactionalID for hot-path checks.
	transactional bool
}

// NewTradeProducer constructs a TradeProducer. If cfg.TransactionalID is
// non-empty the client opens in transactional mode; franz-go will issue
// InitProducerID on the first BeginTransaction, at which point any earlier
// primary using the same id loses its producer epoch.
func NewTradeProducer(cfg ProducerConfig, logger *zap.Logger) (*TradeProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	if cfg.ProducerID == "" {
		return nil, errors.New("journal: ProducerID required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "trade-event"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 32
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 10 * time.Millisecond
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ProducerLinger(0),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	transactional := cfg.TransactionalID != ""
	if transactional {
		opts = append(opts,
			kgo.TransactionalID(cfg.TransactionalID),
			kgo.TransactionTimeout(10*time.Second),
		)
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kgo.NewClient: %w", err)
	}
	return &TradeProducer{
		client:        cli,
		cfg:           cfg,
		logger:        logger,
		transactional: transactional,
	}, nil
}

// Publish sends a single Output. In transactional mode the call is wrapped
// in its own Begin/End cycle. Kept for tests and single-shot callers; the
// Pump loop below uses batched PublishBatch for throughput.
func (p *TradeProducer) Publish(ctx context.Context, out *sequencer.Output) error {
	return p.PublishBatch(ctx, []*sequencer.Output{out})
}

// PublishBatch wraps an entire batch of Outputs in a single Kafka transaction
// (transactional mode) or just fires them sequentially (idempotent mode).
// On any produce / flush / commit error the transaction is aborted.
func (p *TradeProducer) PublishBatch(ctx context.Context, batch []*sequencer.Output) error {
	if len(batch) == 0 {
		return nil
	}
	records, err := p.encodeBatch(batch)
	if err != nil {
		return err
	}
	if !p.transactional {
		for _, rec := range records {
			if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
				return fmt.Errorf("produce trade-event: %w", err)
			}
		}
		return nil
	}
	if err := p.client.BeginTransaction(); err != nil {
		return fmt.Errorf("begin txn: %w", err)
	}
	for _, rec := range records {
		if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
			_ = p.client.EndTransaction(ctx, kgo.TryAbort)
			return fmt.Errorf("produce trade-event: %w", err)
		}
	}
	if err := p.client.Flush(ctx); err != nil {
		_ = p.client.EndTransaction(ctx, kgo.TryAbort)
		return fmt.Errorf("flush txn: %w", err)
	}
	if err := p.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("commit txn: %w", err)
	}
	return nil
}

func (p *TradeProducer) encodeBatch(batch []*sequencer.Output) ([]*kgo.Record, error) {
	recs := make([]*kgo.Record, 0, len(batch))
	for _, out := range batch {
		te, err := OutputToTradeEvent(out, p.cfg.ProducerID)
		if err != nil {
			return nil, fmt.Errorf("convert output: %w", err)
		}
		payload, err := proto.Marshal(te)
		if err != nil {
			return nil, fmt.Errorf("marshal TradeEvent: %w", err)
		}
		recs = append(recs, &kgo.Record{
			Topic: p.cfg.Topic,
			Key:   []byte(out.Symbol),
			Value: payload,
		})
	}
	return recs, nil
}

// Pump drains outbox and publishes Outputs in batches until ctx is cancelled
// or outbox is closed. Batching is bounded by BatchSize and FlushInterval so
// the transactional mode doesn't commit one txn per event (which would melt
// the broker's transaction state machine at match throughput).
//
// Publish errors are logged; franz-go internally retries for transient
// broker issues. Fatal errors (fenced producer on lease loss) will surface
// repeatedly — callers should notice and exit runPrimary so the election
// loop can demote this process.
func (p *TradeProducer) Pump(ctx context.Context, outbox <-chan *sequencer.Output) {
	batch := make([]*sequencer.Output, 0, p.cfg.BatchSize)
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
			// Log one line per batch (not per event); include the first
			// symbol so we can find the flow in other services.
			p.logger.Error("publish trade-event batch",
				zap.Int("batch", len(batch)),
				zap.String("first_symbol", batch[0].Symbol),
				zap.Uint64("first_seq_id", batch[0].SeqID),
				zap.Error(err))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case out, ok := <-outbox:
			if !ok {
				flush()
				return
			}
			batch = append(batch, out)
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

// Close flushes and closes the underlying client.
func (p *TradeProducer) Close() { p.client.Close() }
