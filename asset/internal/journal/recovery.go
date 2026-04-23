package journal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// RecoveryConfig configures startup replay from asset-journal.
type RecoveryConfig struct {
	Brokers  []string
	Topic    string
	ClientID string
	Logger   *zap.Logger
}

// RecoveryResult summarizes the replayed journal range.
type RecoveryResult struct {
	Applied       int
	MaxAssetSeqID uint64
	Partitions    int
}

// ReplayFundingState rebuilds the funding wallet by replaying asset-journal
// from Kafka log start to the topic end captured at startup. It fails if Kafka
// has truncated any partition (start offset > 0), because without a durable
// snapshot there is no safe way to reconstruct balances from a partial log.
func ReplayFundingState(ctx context.Context, cfg RecoveryConfig, state *engine.State) (RecoveryResult, error) {
	if len(cfg.Brokers) == 0 {
		return RecoveryResult{}, errors.New("journal recovery: no brokers")
	}
	if state == nil {
		return RecoveryResult{}, errors.New("journal recovery: nil state")
	}
	if cfg.Topic == "" {
		cfg.Topic = DefaultTopic
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "asset-recovery"
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	starts, ends, err := listRecoveryOffsets(ctx, cfg)
	if err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			logger.Warn("asset-journal topic not found during recovery; starting with empty funding state",
				zap.String("topic", cfg.Topic))
			return RecoveryResult{}, nil
		}
		return RecoveryResult{}, err
	}

	endParts := ends[cfg.Topic]
	if len(endParts) == 0 {
		return RecoveryResult{}, nil
	}
	startParts := starts[cfg.Topic]
	partitions := make(map[int32]kgo.Offset, len(endParts))
	targets := make(map[int32]int64, len(endParts))
	current := make(map[int32]int64, len(endParts))
	done := make(map[int32]bool, len(endParts))
	remaining := 0
	for part, end := range endParts {
		if end.Err != nil {
			return RecoveryResult{}, fmt.Errorf("journal recovery: end offset partition %d: %w", part, end.Err)
		}
		start, ok := startParts[part]
		if !ok {
			return RecoveryResult{}, fmt.Errorf("journal recovery: missing start offset for partition %d", part)
		}
		if start.Err != nil {
			return RecoveryResult{}, fmt.Errorf("journal recovery: start offset partition %d: %w", part, start.Err)
		}
		if start.Offset > 0 {
			return RecoveryResult{}, fmt.Errorf("%w: asset-journal partition %d starts at offset %d without a funding snapshot",
				os.ErrInvalid, part, start.Offset)
		}
		partitions[part] = kgo.NewOffset().At(start.Offset)
		targets[part] = end.Offset
		current[part] = start.Offset
		if start.Offset >= end.Offset {
			done[part] = true
			continue
		}
		remaining++
	}
	if remaining == 0 {
		return RecoveryResult{Partitions: len(endParts)}, nil
	}

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: partitions}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("journal recovery: kgo.NewClient: %w", err)
	}
	defer cli.Close()

	var res RecoveryResult
	res.Partitions = len(endParts)
	markDone := func(part int32) {
		if done[part] {
			return
		}
		if current[part] >= targets[part] {
			done[part] = true
			remaining--
		}
	}

	for remaining > 0 {
		fetches := cli.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return res, nil
		}
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if err := fetches.Err(); err != nil {
			return res, fmt.Errorf("journal recovery fetch: %w", err)
		}
		var replayErr error
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if replayErr != nil {
				return
			}
			if p.Topic != cfg.Topic {
				return
			}
			target, ok := targets[p.Partition]
			if !ok || done[p.Partition] {
				return
			}
			for _, rec := range p.Records {
				if rec.Offset >= target {
					current[p.Partition] = target
					break
				}
				var evt eventpb.AssetJournalEvent
				if err := proto.Unmarshal(rec.Value, &evt); err != nil {
					replayErr = fmt.Errorf("decode asset-journal partition %d offset %d: %w",
						rec.Partition, rec.Offset, err)
					return
				}
				if evt.AssetSeqId > res.MaxAssetSeqID {
					res.MaxAssetSeqID = evt.AssetSeqId
				}
				if err := ApplyFundingEvent(state, &evt); err != nil {
					replayErr = fmt.Errorf("apply asset-journal partition %d offset %d asset_seq_id %d: %w",
						rec.Partition, rec.Offset, evt.AssetSeqId, err)
					return
				}
				res.Applied++
				next := rec.Offset + 1
				if next > current[p.Partition] {
					current[p.Partition] = next
				}
			}
			markDone(p.Partition)
		})
		if replayErr != nil {
			return res, replayErr
		}
	}
	return res, nil
}

func listRecoveryOffsets(ctx context.Context, cfg RecoveryConfig) (kadm.ListedOffsets, kadm.ListedOffsets, error) {
	adminClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID+"-admin"),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("journal recovery admin client: %w", err)
	}
	defer adminClient.Close()

	admin := kadm.NewClient(adminClient)
	starts, err := admin.ListStartOffsets(ctx, cfg.Topic)
	if err != nil {
		return nil, nil, fmt.Errorf("journal recovery list start offsets: %w", err)
	}
	if err := starts.Error(); err != nil {
		return nil, nil, fmt.Errorf("journal recovery list start offsets: %w", err)
	}
	ends, err := admin.ListEndOffsets(ctx, cfg.Topic)
	if err != nil {
		return nil, nil, fmt.Errorf("journal recovery list end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return nil, nil, fmt.Errorf("journal recovery list end offsets: %w", err)
	}
	return starts, ends, nil
}

// ApplyFundingEvent applies one funding payload to state and verifies the
// recomputed balance against the journal's BalanceAfter snapshot. Saga-only
// events are intentionally ignored by the funding wallet.
func ApplyFundingEvent(state *engine.State, evt *eventpb.AssetJournalEvent) error {
	if state == nil {
		return errors.New("nil state")
	}
	if evt == nil {
		return nil
	}
	switch p := evt.Payload.(type) {
	case *eventpb.AssetJournalEvent_XferOut:
		if p.XferOut == nil {
			return nil
		}
		req, err := transferRequest(p.XferOut.UserId, p.XferOut.TransferId, p.XferOut.Asset, p.XferOut.Amount)
		if err != nil {
			return err
		}
		res, err := state.ApplyTransferOut(req)
		if err != nil {
			return err
		}
		return verifyFundingSnapshot("xfer_out", evt.FundingVersion, res, p.XferOut.BalanceAfter)
	case *eventpb.AssetJournalEvent_XferIn:
		if p.XferIn == nil {
			return nil
		}
		req, err := transferRequest(p.XferIn.UserId, p.XferIn.TransferId, p.XferIn.Asset, p.XferIn.Amount)
		if err != nil {
			return err
		}
		res, err := state.ApplyTransferIn(req)
		if err != nil {
			return err
		}
		return verifyFundingSnapshot("xfer_in", evt.FundingVersion, res, p.XferIn.BalanceAfter)
	case *eventpb.AssetJournalEvent_Compensate:
		if p.Compensate == nil {
			return nil
		}
		req, err := transferRequest(p.Compensate.UserId, p.Compensate.TransferId, p.Compensate.Asset, p.Compensate.Amount)
		if err != nil {
			return err
		}
		res, err := state.ApplyCompensate(req)
		if err != nil {
			return err
		}
		return verifyFundingSnapshot("compensate", evt.FundingVersion, res, p.Compensate.BalanceAfter)
	case *eventpb.AssetJournalEvent_SagaState:
		return nil
	default:
		return nil
	}
}

func transferRequest(userID, transferID, asset, amount string) (engine.TransferRequest, error) {
	amt, err := dec.Parse(amount)
	if err != nil {
		return engine.TransferRequest{}, fmt.Errorf("parse amount: %w", err)
	}
	return engine.TransferRequest{
		UserID:     userID,
		TransferID: transferID,
		Asset:      asset,
		Amount:     amt,
	}, nil
}

func verifyFundingSnapshot(kind string, fundingVersion uint64, res engine.Result, snap *eventpb.FundingBalanceSnapshot) error {
	if snap == nil {
		return fmt.Errorf("%s: missing balance_after", kind)
	}
	avail, err := dec.Parse(snap.Available)
	if err != nil {
		return fmt.Errorf("%s: parse balance_after.available: %w", kind, err)
	}
	frozen, err := dec.Parse(snap.Frozen)
	if err != nil {
		return fmt.Errorf("%s: parse balance_after.frozen: %w", kind, err)
	}
	if !dec.Equal(res.BalanceAfter.Available, avail) ||
		!dec.Equal(res.BalanceAfter.Frozen, frozen) ||
		res.BalanceAfter.Version != snap.Version ||
		res.FundingVersion != fundingVersion {
		return fmt.Errorf("%s: replay mismatch balance_after=(%s,%s,v%d,fv%d) journal=(%s,%s,v%d,fv%d)",
			kind,
			res.BalanceAfter.Available.String(),
			res.BalanceAfter.Frozen.String(),
			res.BalanceAfter.Version,
			res.FundingVersion,
			snap.Available,
			snap.Frozen,
			snap.Version,
			fundingVersion)
	}
	return nil
}

// DefaultRecoveryTimeout is the startup replay budget used by the asset
// binary. The helper lives here so tests and future binaries can share it.
const DefaultRecoveryTimeout = 60 * time.Second
