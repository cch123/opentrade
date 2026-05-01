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
	"github.com/xargin/opentrade/pkg/shard"
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

	// VShardCount is the Counter-side vshard count (ADR-0058 §2). Match
	// uses it to compute `partition = shard.Index(user_id, VShardCount)`
	// for every trade-event record so the mapping exactly matches what
	// Counter workers consume. Required (>0) — mismatch with Counter
	// equals routing breakage.
	VShardCount int
}

// flushReq asks the Pump to drain outbox and flush the current batch. The
// reply chan is unbuffered; Pump sends the final publish error (possibly nil)
// after the flush completes.
type flushReq struct {
	ctx   context.Context
	reply chan error
}

// TradeProducer publishes sequencer.Output emissions to the trade-event topic.
type TradeProducer struct {
	client *kgo.Client
	cfg    ProducerConfig
	logger *zap.Logger
	// transactional is cached from cfg.TransactionalID for hot-path checks.
	transactional bool
	// flushCh is served by the Pump goroutine; FlushAndWait sends a request
	// and waits for reply. Buffer of 1 is enough — callers are expected to
	// serialise FlushAndWait calls per producer.
	flushCh chan flushReq
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
	if cfg.VShardCount <= 0 {
		return nil, errors.New("journal: VShardCount required (ADR-0058)")
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
		// ADR-0058 §2a: every record carries an explicit Partition
		// computed from user_id. ManualPartitioner honours that field
		// instead of hashing the key itself.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
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
		flushCh:       make(chan flushReq, 1),
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
		if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
			return fmt.Errorf("produce trade-event: %w", err)
		}
		return nil
	}
	if err := p.client.BeginTransaction(); err != nil {
		return fmt.Errorf("begin txn: %w", err)
	}
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		_ = p.client.EndTransaction(ctx, kgo.TryAbort)
		return fmt.Errorf("produce trade-event: %w", err)
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
		for _, target := range outputTargets(out, p.cfg.VShardCount) {
			recs = append(recs, &kgo.Record{
				Topic:     p.cfg.Topic,
				Key:       []byte(target.userID),
				Value:     payload,
				Partition: int32(target.partition),
			})
		}
	}
	return recs, nil
}

// routeTarget is one (user_id → vshard partition) tuple. Trade
// Outputs expand to two when maker != taker; everything else is one.
type routeTarget struct {
	userID    string
	partition int
}

// outputTargets enumerates the vshards a single Output must land on
// under the ADR-0058 §2 dual-emit rule. Trade carries two users so it
// emits one record per side (collapsing to a single record on
// self-trade). Every other OutputKind is tied to one user (out.UserID).
func outputTargets(out *sequencer.Output, vshardCount int) []routeTarget {
	if out.Kind == sequencer.OutputTrade && out.MakerUserID != "" && out.MakerUserID != out.UserID {
		return []routeTarget{
			{out.MakerUserID, shard.Index(out.MakerUserID, vshardCount)},
			{out.UserID, shard.Index(out.UserID, vshardCount)}, // taker side (out.UserID == taker)
		}
	}
	return []routeTarget{{out.UserID, shard.Index(out.UserID, vshardCount)}}
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

	// flush publishes the current batch and returns the first error (if any).
	// Background tick / shutdown paths ignore the return value (errors are
	// already logged); FlushAndWait surfaces it to its caller.
	flush := func(useCtx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		err := p.PublishBatch(useCtx, batch)
		if err != nil {
			// Log one line per batch (not per event); include the first
			// symbol so we can find the flow in other services.
			p.logger.Error("publish trade-event batch",
				zap.Int("batch", len(batch)),
				zap.String("first_symbol", batch[0].Symbol),
				zap.Uint64("first_match_seq_id", batch[0].MatchSeq),
				zap.Error(err))
		}
		batch = batch[:0]
		return err
	}

	disarmTimer := func() {
		if timerArmed && !timer.Stop() {
			<-timer.C
		}
		timerArmed = false
	}

	for {
		select {
		case <-ctx.Done():
			_ = flush(ctx)
			return
		case out, ok := <-outbox:
			if !ok {
				_ = flush(ctx)
				return
			}
			batch = append(batch, out)
			if !timerArmed {
				timer.Reset(p.cfg.FlushInterval)
				timerArmed = true
			}
			if len(batch) >= p.cfg.BatchSize {
				disarmTimer()
				_ = flush(ctx)
			}
		case <-timer.C:
			timerArmed = false
			_ = flush(ctx)
		case req := <-p.flushCh:
			disarmTimer()
			// Drain any events already sitting in outbox (non-blocking) so
			// the flush captures everything emitted up to now. Callers
			// invoke FlushAndWait while the worker's state lock is held,
			// so no new outputs can be appended after this drain returns.
		drain:
			for {
				select {
				case out, ok := <-outbox:
					if !ok {
						err := flush(req.ctx)
						req.reply <- err
						return
					}
					batch = append(batch, out)
				default:
					break drain
				}
			}
			err := flush(req.ctx)
			req.reply <- err
		}
	}
}

// FlushAndWait blocks until the Pump has drained every output already in the
// shared outbox and confirmed commit with Kafka (ADR-0048 output flush
// barrier). Callers MUST hold any worker-side lock that prevents new
// emissions; otherwise outputs appended after the drain point are missed.
//
// Returns nil on success. Returns ctx.Err() if ctx expires before the Pump
// receives / replies to the request. Returns the Publish error if the Pump
// tried to commit but Kafka refused (log already emitted inside Pump).
func (p *TradeProducer) FlushAndWait(ctx context.Context) error {
	reply := make(chan error, 1)
	req := flushReq{ctx: ctx, reply: reply}
	select {
	case p.flushCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes and closes the underlying client.
func (p *TradeProducer) Close() { p.client.Close() }
