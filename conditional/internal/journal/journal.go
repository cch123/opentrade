// Package journal publishes conditional-event records to Kafka for the
// audit stream consumed by trade-dump (ADR-0047). The service emits a full
// post-change snapshot on every state transition; the consumer upserts by
// conditional id with a seq-guarded last-write-wins rule so HA handover
// duplicates converge.
//
// The producer is intentionally non-transactional — MVP accepts duplicate
// emissions under HA handover because each event carries `meta.ts_unix_ms`
// which trade-dump uses as a monotonic guard. Adding an EOS-style
// transactional producer here is future work (see ADR-0047 §Future Work).
package journal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	"github.com/xargin/opentrade/conditional/internal/engine"
)

// DefaultTopic matches the topic name referenced in ADR-0047. Config
// surfaces a knob so operators can prefix/suffix in staging.
const DefaultTopic = "conditional-event"

// Sink is the narrow interface the engine uses. Kafka-backed implementations
// may block to apply backpressure rather than dropping history.
type Sink = engine.JournalSink

// Config wires the Kafka client.
type Config struct {
	Brokers      []string
	Topic        string
	ProducerID   string        // set on EventMeta.producer_id
	QueueSize    int           // async queue capacity; 0 → 4096
	RetryBackoff time.Duration // produce retry backoff; 0 → 100ms
	Logger       *zap.Logger
}

// Producer is the Kafka-backed Sink. Emit enqueues onto an unbounded
// channel (bounded by QueueSize) and a single goroutine drains to Kafka.
type Producer struct {
	kc           *kgo.Client
	topic        string
	producerID   string
	logger       *zap.Logger
	retryBackoff time.Duration

	seq atomic.Uint64

	queue     chan *eventpb.ConditionalUpdate
	done      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// New opens the Kafka client and starts the drain goroutine. Close() on
// shutdown drains in-flight records and releases the client.
func New(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("journal: no brokers")
	}
	topic := cfg.Topic
	if topic == "" {
		topic = DefaultTopic
	}
	qs := cfg.QueueSize
	if qs <= 0 {
		qs = 4096
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	retryBackoff := cfg.RetryBackoff
	if retryBackoff <= 0 {
		retryBackoff = 100 * time.Millisecond
	}

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		kc:           kc,
		topic:        topic,
		producerID:   cfg.ProducerID,
		logger:       logger,
		retryBackoff: retryBackoff,
		queue:        make(chan *eventpb.ConditionalUpdate, qs),
		done:         make(chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
	}
	go p.drain()
	return p, nil
}

// Emit queues c for asynchronous publication. When the queue is full, Emit
// blocks and applies backpressure instead of dropping terminal history.
func (p *Producer) Emit(c *engine.Conditional) {
	u := convert(c, p.seq.Add(1), p.producerID)
	select {
	case p.queue <- u:
	case <-p.ctx.Done():
	}
}

// Close drains remaining records and shuts the Kafka client.
func (p *Producer) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		close(p.queue)
	})
	select {
	case <-p.done:
	case <-ctx.Done():
		p.cancel()
	}
	p.kc.Close()
	return nil
}

// drain pulls events off the queue and produces to Kafka. Produce errors are
// retried with backoff until the record is acked or Close's context cancels
// the producer.
func (p *Producer) drain() {
	defer close(p.done)
	for u := range p.queue {
		data, err := proto.Marshal(u)
		if err != nil {
			p.logger.Warn("marshal conditional update", zap.Error(err))
			continue
		}
		rec := &kgo.Record{
			Topic: p.topic,
			Key:   []byte(u.UserId),
			Value: data,
		}
		p.produceWithRetry(u, rec)
	}
}

func (p *Producer) produceWithRetry(u *eventpb.ConditionalUpdate, rec *kgo.Record) {
	for {
		if err := p.kc.ProduceSync(p.ctx, rec).FirstErr(); err != nil {
			if p.ctx.Err() != nil {
				return
			}
			p.logger.Warn("conditional journal produce failed; retrying",
				zap.Uint64("id", u.Id),
				zap.Uint64("conditional_seq_id", u.ConditionalSeqId),
				zap.Error(err))
			timer := time.NewTimer(p.retryBackoff)
			select {
			case <-timer.C:
			case <-p.ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
			continue
		}
		return
	}
}

// convert builds the proto event from the engine's in-memory twin. Kept
// exported via ConvertForTest so unit tests can exercise the mapping
// without spinning up a Kafka client.
func convert(c *engine.Conditional, conditionalSeq uint64, producerID string) *eventpb.ConditionalUpdate {
	nowMs := time.Now().UnixMilli()
	u := &eventpb.ConditionalUpdate{
		Meta: &eventpb.EventMeta{
			TsUnixMs:   nowMs,
			ProducerId: producerID,
		},
		ConditionalSeqId:    conditionalSeq,
		Id:                  c.ID,
		ClientConditionalId: c.ClientCondID,
		UserId:              c.UserID,
		Symbol:              c.Symbol,
		Side:                c.Side,
		Type:                mapType(c.Type),
		StopPrice:           decString(c.StopPrice),
		LimitPrice:          decString(c.LimitPrice),
		Qty:                 decString(c.Qty),
		QuoteQty:            decString(c.QuoteQty),
		Tif:                 c.TIF,
		Status:              mapStatus(c.Status),
		CreatedAtUnixMs:     c.CreatedAtMs,
		TriggeredAtUnixMs:   c.TriggeredAtMs,
		PlacedOrderId:       c.PlacedOrderID,
		RejectReason:        c.RejectReason,
		ExpiresAtUnixMs:     c.ExpiresAtMs,
		OcoGroupId:          c.OCOGroupID,
		TrailingDeltaBps:    c.TrailingDeltaBps,
		ActivationPrice:     decString(c.ActivationPrice),
		TrailingWatermark:   decString(c.TrailingWatermark),
		TrailingActive:      c.TrailingActive,
	}
	return u
}

// ConvertForTest is the test hook for the private `convert` helper.
func ConvertForTest(c *engine.Conditional, seq uint64, producerID string) *eventpb.ConditionalUpdate {
	return convert(c, seq, producerID)
}

type decStringer interface{ String() string }

func decString(d decStringer) string {
	s := d.String()
	if s == "" {
		return "0"
	}
	return s
}

func mapType(t condrpc.ConditionalType) eventpb.ConditionalEventType {
	switch t {
	case condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS:
		return eventpb.ConditionalEventType_CONDITIONAL_EVENT_TYPE_STOP_LOSS
	case condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT:
		return eventpb.ConditionalEventType_CONDITIONAL_EVENT_TYPE_STOP_LOSS_LIMIT
	case condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT:
		return eventpb.ConditionalEventType_CONDITIONAL_EVENT_TYPE_TAKE_PROFIT
	case condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT:
		return eventpb.ConditionalEventType_CONDITIONAL_EVENT_TYPE_TAKE_PROFIT_LIMIT
	case condrpc.ConditionalType_CONDITIONAL_TYPE_TRAILING_STOP_LOSS:
		return eventpb.ConditionalEventType_CONDITIONAL_EVENT_TYPE_TRAILING_STOP_LOSS
	}
	return eventpb.ConditionalEventType_CONDITIONAL_EVENT_TYPE_UNSPECIFIED
}

func mapStatus(s condrpc.ConditionalStatus) eventpb.ConditionalEventStatus {
	switch s {
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING:
		return eventpb.ConditionalEventStatus_CONDITIONAL_EVENT_STATUS_PENDING
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_TRIGGERED:
		return eventpb.ConditionalEventStatus_CONDITIONAL_EVENT_STATUS_TRIGGERED
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED:
		return eventpb.ConditionalEventStatus_CONDITIONAL_EVENT_STATUS_CANCELED
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_REJECTED:
		return eventpb.ConditionalEventStatus_CONDITIONAL_EVENT_STATUS_REJECTED
	case condrpc.ConditionalStatus_CONDITIONAL_STATUS_EXPIRED:
		return eventpb.ConditionalEventStatus_CONDITIONAL_EVENT_STATUS_EXPIRED
	}
	return eventpb.ConditionalEventStatus_CONDITIONAL_EVENT_STATUS_UNSPECIFIED
}
