// Package recovery replays trigger-event after restoring a trigger snapshot.
// The replay is a startup-only, single-threaded pass: the service does not
// expose RPC or start either price consumer until the journal reaches a stable
// log end offset on every partition.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/trigger/engine"
)

const (
	defaultCatchUpTimeout = 2 * time.Minute
	catchUpPollTimeout    = 500 * time.Millisecond
)

// Config describes the trigger-event recovery pass. InitialOffsets is the
// snapshot's partition -> next-to-consume map; an absent partition starts at
// offset zero so a cold start rebuilds the complete trigger set.
type Config struct {
	Brokers        []string
	ClientID       string
	Topic          string
	InitialOffsets map[int32]int64
	Timeout        time.Duration
	Logger         *zap.Logger
}

// CatchUp replays TriggerEvent envelopes from the snapshot cursor to a stable
// LEO. Stability means: after applying through one observed end-offset map, a
// second metadata/ListOffsets pass returns the same map. If an old primary is
// still draining its journal queue, the loop follows the advancing LEO rather
// than opening RPC with an update gap.
func CatchUp(ctx context.Context, cfg Config, eng *engine.Engine) error {
	if eng == nil {
		return errors.New("trigger recovery: engine required")
	}
	if len(cfg.Brokers) == 0 {
		return errors.New("trigger recovery: brokers required")
	}
	if cfg.Topic == "" {
		return errors.New("trigger recovery: topic required")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "trigger-recovery"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultCatchUpTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}

	catchCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	source, err := newKafkaSource(cfg)
	if err != nil {
		return err
	}
	defer source.Close()
	if err := catchUp(catchCtx, source, cfg.InitialOffsets, eng, cfg.Logger); err != nil {
		return fmt.Errorf("trigger recovery: %w", err)
	}
	return nil
}

// ApplyRecord decodes and applies one trigger-event record. It is exported as
// a narrow test hook and keeps the Kafka loop separate from engine semantics.
func ApplyRecord(eng *engine.Engine, rec *kgo.Record) error {
	if eng == nil {
		return errors.New("apply trigger-event: nil engine")
	}
	if rec == nil {
		return errors.New("apply trigger-event: nil record")
	}
	var envelope eventpb.TriggerEvent
	if err := proto.Unmarshal(rec.Value, &envelope); err != nil {
		return fmt.Errorf("decode partition %d offset %d: %w", rec.Partition, rec.Offset, err)
	}
	if err := eng.ApplyJournalEvent(&envelope); err != nil {
		return fmt.Errorf("apply partition %d offset %d: %w", rec.Partition, rec.Offset, err)
	}
	return nil
}

type partitionBounds struct {
	start int64
	end   int64
}

type recordSource interface {
	Bounds(context.Context) (map[int32]partitionBounds, error)
	AddPartitions(map[int32]int64)
	Poll(context.Context) ([]*kgo.Record, error)
	Close()
}

func catchUp(ctx context.Context, source recordSource, initial map[int32]int64, eng *engine.Engine, logger *zap.Logger) error {
	bounds, err := source.Bounds(ctx)
	if err != nil {
		return fmt.Errorf("query initial offsets: %w", err)
	}
	if len(bounds) == 0 {
		return errors.New("trigger-event topic has no partitions")
	}

	cursors := make(map[int32]int64, len(bounds))
	assignments := make(map[int32]int64, len(bounds))
	for partition, bound := range bounds {
		start := int64(0)
		if saved, ok := initial[partition]; ok {
			start = saved
		}
		if err := validateCursor(partition, start, bound); err != nil {
			return err
		}
		cursors[partition] = start
		assignments[partition] = start
	}
	for partition := range initial {
		if _, ok := bounds[partition]; !ok {
			return fmt.Errorf("snapshot references missing trigger-event partition %d", partition)
		}
	}
	source.AddPartitions(assignments)
	targets := endOffsets(bounds)
	applied := 0

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("catch-up timeout/cancel after %d records: %w", applied, err)
		}
		if reached(cursors, targets) {
			latest, err := source.Bounds(ctx)
			if err != nil {
				return fmt.Errorf("re-query stable offsets: %w", err)
			}
			newAssignments, err := mergeBounds(cursors, latest)
			if err != nil {
				return err
			}
			if len(newAssignments) > 0 {
				source.AddPartitions(newAssignments)
			}
			latestTargets := endOffsets(latest)
			if equalOffsets(targets, latestTargets) {
				logger.Info("trigger-event catch-up complete",
					zap.Int("applied", applied),
					zap.Int("partitions", len(targets)))
				return nil
			}
			targets = latestTargets
			continue
		}

		pollCtx, cancel := context.WithTimeout(ctx, catchUpPollTimeout)
		records, pollErr := source.Poll(pollCtx)
		cancel()
		if pollErr != nil {
			if errors.Is(pollErr, context.DeadlineExceeded) && ctx.Err() == nil {
				continue
			}
			return fmt.Errorf("poll: %w", pollErr)
		}
		for _, rec := range records {
			cursor, ok := cursors[rec.Partition]
			if !ok {
				return fmt.Errorf("record from unassigned partition %d", rec.Partition)
			}
			if rec.Offset < cursor {
				// A retried fetch may overlap a previously applied batch. Skipping
				// the prefix keeps replay idempotent without reordering the tail.
				continue
			}
			if rec.Offset > cursor {
				return fmt.Errorf("offset gap on partition %d: got %d want %d", rec.Partition, rec.Offset, cursor)
			}
			if err := ApplyRecord(eng, rec); err != nil {
				return err
			}
			cursors[rec.Partition] = rec.Offset + 1
			applied++
		}
	}
}

func validateCursor(partition int32, cursor int64, bound partitionBounds) error {
	if cursor < bound.start {
		return fmt.Errorf("trigger-event partition %d retention gap: snapshot offset %d before log start %d", partition, cursor, bound.start)
	}
	if cursor > bound.end {
		return fmt.Errorf("trigger-event partition %d snapshot offset %d beyond LEO %d", partition, cursor, bound.end)
	}
	return nil
}

func mergeBounds(cursors map[int32]int64, latest map[int32]partitionBounds) (map[int32]int64, error) {
	for partition := range cursors {
		if _, ok := latest[partition]; !ok {
			return nil, fmt.Errorf("trigger-event partition %d disappeared during catch-up", partition)
		}
	}
	additions := make(map[int32]int64)
	for partition, bound := range latest {
		cursor, exists := cursors[partition]
		if !exists {
			cursor = 0
			cursors[partition] = cursor
			additions[partition] = cursor
		}
		if err := validateCursor(partition, cursor, bound); err != nil {
			return nil, err
		}
	}
	return additions, nil
}

func reached(cursors, targets map[int32]int64) bool {
	for partition, target := range targets {
		if cursors[partition] < target {
			return false
		}
	}
	return true
}

func endOffsets(bounds map[int32]partitionBounds) map[int32]int64 {
	ends := make(map[int32]int64, len(bounds))
	for partition, bound := range bounds {
		ends[partition] = bound.end
	}
	return ends
}

func equalOffsets(a, b map[int32]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for partition, offset := range a {
		if b[partition] != offset {
			return false
		}
	}
	return true
}

type kafkaSource struct {
	cli   *kgo.Client
	topic string
}

func newKafkaSource(cfg Config) (*kafkaSource, error) {
	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		// The current trigger producer is non-transactional, but recovery is
		// state reconstruction rather than audit inspection. ReadCommitted
		// keeps the path correct if producer fencing/EOS is added later.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, fmt.Errorf("trigger recovery: kgo.NewClient: %w", err)
	}
	return &kafkaSource{cli: cli, topic: cfg.Topic}, nil
}

func (s *kafkaSource) Close() { s.cli.Close() }

func (s *kafkaSource) AddPartitions(starts map[int32]int64) {
	parts := make(map[int32]kgo.Offset, len(starts))
	for partition, offset := range starts {
		parts[partition] = kgo.NewOffset().At(offset)
	}
	s.cli.AddConsumePartitions(map[string]map[int32]kgo.Offset{s.topic: parts})
}

func (s *kafkaSource) Poll(ctx context.Context) ([]*kgo.Record, error) {
	fetches := s.cli.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil, errors.New("Kafka client closed unexpectedly")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		first := errs[0]
		return nil, fmt.Errorf("partition %d: %w", first.Partition, first.Err)
	}
	var records []*kgo.Record
	fetches.EachRecord(func(rec *kgo.Record) {
		records = append(records, rec)
	})
	return records, nil
}

func (s *kafkaSource) Bounds(ctx context.Context) (map[int32]partitionBounds, error) {
	partitions, err := s.partitions(ctx)
	if err != nil {
		return nil, err
	}
	starts, err := s.listOffsets(ctx, partitions, -2)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	ends, err := s.listOffsets(ctx, partitions, -1)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	bounds := make(map[int32]partitionBounds, len(partitions))
	for _, partition := range partitions {
		bounds[partition] = partitionBounds{start: starts[partition], end: ends[partition]}
	}
	return bounds, nil
}

func (s *kafkaSource) partitions(ctx context.Context) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(s.topic)}}
	raw, err := s.cli.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	resp, ok := raw.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("metadata: unexpected response %T", raw)
	}
	for _, topic := range resp.Topics {
		if topic.Topic == nil || *topic.Topic != s.topic {
			continue
		}
		if topic.ErrorCode != 0 {
			return nil, fmt.Errorf("metadata topic error code %d", topic.ErrorCode)
		}
		partitions := make([]int32, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				return nil, fmt.Errorf("metadata partition %d error code %d", partition.Partition, partition.ErrorCode)
			}
			partitions = append(partitions, partition.Partition)
		}
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		return partitions, nil
	}
	return nil, fmt.Errorf("topic %q missing from metadata", s.topic)
}

func (s *kafkaSource) listOffsets(ctx context.Context, partitions []int32, timestamp int64) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = s.topic
	for _, partition := range partitions {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = partition
		part.Timestamp = timestamp
		topic.Partitions = append(topic.Partitions, part)
	}
	req.Topics = append(req.Topics, topic)
	raw, err := s.cli.Request(ctx, req)
	if err != nil {
		return nil, err
	}
	resp, ok := raw.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response %T", raw)
	}
	offsets := make(map[int32]int64, len(partitions))
	for _, responseTopic := range resp.Topics {
		if responseTopic.Topic != s.topic {
			continue
		}
		for _, partition := range responseTopic.Partitions {
			if partition.ErrorCode != 0 {
				return nil, fmt.Errorf("partition %d error code %d", partition.Partition, partition.ErrorCode)
			}
			offsets[partition.Partition] = partition.Offset
		}
	}
	for _, partition := range partitions {
		if _, ok := offsets[partition]; !ok {
			return nil, fmt.Errorf("partition %d missing from ListOffsets response", partition)
		}
	}
	return offsets, nil
}
