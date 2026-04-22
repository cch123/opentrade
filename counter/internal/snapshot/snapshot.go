// Package snapshot serializes Counter's ShardState + Dedup + UserSequencer
// counter-shard-scoped seq to a BlobStore (local filesystem or shared
// object storage) and restores it on startup.
//
// ADR-0049: default on-disk format is protobuf (.pb); JSON (.json) stays
// available as a debug fallback selected by --snapshot-format=json. Load
// probes .pb first, then .json, so in-place upgrades just work.
//
// ADR-0058 phase 1: I/O is abstracted behind BlobStore so snapshots can
// live on shared object storage and be read by a different node after
// vshard migration.
package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"

	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// Format names the on-disk encoding. ADR-0049.
type Format int

const (
	// FormatProto is the default binary encoding (file extension .pb).
	FormatProto Format = iota
	// FormatJSON is the debug-friendly text encoding (file extension .json).
	FormatJSON
)

// String returns the canonical CLI token ("proto" / "json").
func (f Format) String() string {
	switch f {
	case FormatProto:
		return "proto"
	case FormatJSON:
		return "json"
	default:
		return "unknown"
	}
}

// ext returns the file extension (including the leading dot).
func (f Format) ext() string {
	switch f {
	case FormatProto:
		return ".pb"
	case FormatJSON:
		return ".json"
	default:
		return ""
	}
}

// ParseFormat parses a CLI token back to Format. Empty / unrecognised input
// returns an error; the caller is expected to surface it as an invalid-flag
// error at startup.
func ParseFormat(s string) (Format, error) {
	switch s {
	case "proto", "pb", "protobuf":
		return FormatProto, nil
	case "json":
		return FormatJSON, nil
	default:
		return 0, fmt.Errorf("snapshot: unknown format %q (want proto|json)", s)
	}
}

// Version of the snapshot format.
//
// v1 (MVP-2 … MVP-14): shard state + dedup + reservations, no Kafka
//     offsets. Restore falls back to Kafka consumer group committed offset.
//
// v2 (ADR-0048): same payload plus per-partition `Offsets`. Restore uses
//     those to seek the trade-event consumer exactly, eliminating the
//     commit-ahead-of-snapshot loss window. Loading a v1 file is supported
//     as a migration bridge: offsets come back empty, consumer falls back
//     to AtStart one time, then the next tick writes v2.
const (
	versionV1 = 1
	versionV2 = 2
	Version   = versionV2
)

// KafkaOffset is one partition's next-to-consume offset, persisted with the
// snapshot so Restore can seek the consumer without depending on Kafka
// consumer-group state (ADR-0048).
type KafkaOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// BalanceSnapshot is the serialized form of engine.Balance. Version is the
// per-(user, asset) monotonic counter (ADR-0048 backlog: 双层 version 方案
// B). Persisted and restored verbatim so clients using Balance.Version as
// a cache-invalidation handle / optimistic lock don't see it reset across
// process restarts.
type BalanceSnapshot struct {
	Asset     string `json:"asset"`
	Available string `json:"available"`
	Frozen    string `json:"frozen"`
	Version   uint64 `json:"version,omitempty"`
}

// AccountSnapshot captures one user's balances, per-symbol match_seq guard
// (ADR-0048 backlog item 2), the user-level version counter (ADR-0048
// backlog item 1 / 方案 B), and the recent-transfer ring for dedup
// (ADR-0048 backlog item 4 / 方案 A). RecentTransferIDs is stored in
// insertion order (oldest → newest) so Restore can rebuild the ring.
//
// RecentTerminatedOrders is the per-user ring of recently evicted terminal
// orders (ADR-0062); same oldest → newest ordering contract as
// RecentTransferIDs. Missing / empty in older snapshots — the ring is
// dormant until the ADR-0062 evictor comes online in later milestones.
type AccountSnapshot struct {
	UserID                  string                          `json:"user_id"`
	Version                 uint64                          `json:"version,omitempty"`
	Balances                []BalanceSnapshot               `json:"balances"`
	LastMatchSeq            map[string]uint64               `json:"last_match_seq,omitempty"`
	RecentTransferIDs       []string                        `json:"recent_transfer_ids,omitempty"`
	RecentTerminatedOrders  []TerminatedOrderEntrySnapshot  `json:"recent_terminated_orders,omitempty"`
}

// TerminatedOrderEntrySnapshot is the serialized form of
// engine.TerminatedOrderEntry (ADR-0062). Kept small: just enough to let
// CancelOrder produce an idempotent terminal response after the live
// order has been evicted from OrderStore.byID.
type TerminatedOrderEntrySnapshot struct {
	OrderID       uint64 `json:"order_id"`
	FinalStatus   uint8  `json:"final_status"`   // engine.OrderStatus
	TerminatedAt  int64  `json:"terminated_at"`  // ms timestamp
	ClientOrderID string `json:"client_order_id,omitempty"`
	Symbol        string `json:"symbol,omitempty"`
}

// DedupEntrySnapshot captured one transfer_id dedup record when Counter
// still used the shard-wide dedup.Table for idempotency. ADR-0048 backlog
// item 4 方案 A replaced that path with the per-user AccountSnapshot
// .RecentTransferIDs ring, so new snapshots no longer write here. Kept
// for loading pre-ring snapshots: entries in this slice are ignored at
// Restore (the authoritative source is now AccountSnapshot).
type DedupEntrySnapshot struct {
	Key         string `json:"key"`
	ExpiresUnix int64  `json:"expires_unix_ms"`
}

// OrderSnapshot is the serialized form of engine.Order.
type OrderSnapshot struct {
	ID              uint64 `json:"id"`
	ClientOrderID   string `json:"client_order_id,omitempty"`
	UserID          string `json:"user_id"`
	Symbol          string `json:"symbol"`
	Side            uint8  `json:"side"`
	OrderType       uint8  `json:"type"`
	TIF             uint8  `json:"tif"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	QuoteQty        string `json:"quote_qty,omitempty"` // ADR-0035 market buy budget
	FilledQty       string `json:"filled_qty"`
	FrozenAsset     string `json:"frozen_asset,omitempty"`
	FrozenAmount    string `json:"frozen_amount"`
	FrozenSpent     string `json:"frozen_spent,omitempty"` // ADR-0035 consumed freeze
	Status          uint8  `json:"status"`
	PreCancelStatus uint8  `json:"pre_cancel_status,omitempty"`
	CreatedAt       int64  `json:"created_at"`
	UpdatedAt       int64  `json:"updated_at"`
	TerminatedAt    int64  `json:"terminated_at,omitempty"` // ADR-0062; 0 when non-terminal
}

// ReservationSnapshot captures a conditional-order reservation
// (ADR-0041). Reservations are not Kafka-journaled — they only persist
// across graceful restarts via this snapshot. See ADR-0041 §Durability.
type ReservationSnapshot struct {
	UserID      string `json:"user_id"`
	RefID       string `json:"ref_id"`
	Asset       string `json:"asset"`
	Amount      string `json:"amount"`
	CreatedAtMs int64  `json:"created_at_ms,omitempty"`
}

// ShardSnapshot is the on-disk representation of one Counter shard.
type ShardSnapshot struct {
	Version      int                   `json:"version"`
	ShardID      int                   `json:"shard_id"`
	CounterSeq   uint64                `json:"counter_seq"`
	TimestampMS  int64                 `json:"ts_unix_ms"`
	Accounts     []AccountSnapshot     `json:"accounts"`
	Orders       []OrderSnapshot       `json:"orders,omitempty"`
	Dedup        []DedupEntrySnapshot  `json:"dedup,omitempty"`
	Reservations []ReservationSnapshot `json:"reservations,omitempty"`
	// Offsets records per-partition next-to-consume offsets for the
	// trade-event topic (ADR-0048). Written at v2+; v1 files leave this nil
	// and Restore tolerates that.
	Offsets []KafkaOffset `json:"offsets,omitempty"`
	// JournalOffset records the next-to-consume position on this vshard's
	// counter-journal partition at snapshot moment (ADR-0060 §4.1).
	// Recovery uses it to drive a catch-up journal consumer that
	// re-applies events published after the state in this snapshot was
	// last synced — covers the crash window between "state mutated +
	// journal published" and "snapshot captured". Zero means either
	// pre-0060 snapshot (no catch-up) or steady-state cold start (no
	// prior publishes).
	JournalOffset int64 `json:"journal_offset,omitempty"`
}

// -----------------------------------------------------------------------------
// Capture / Restore
// -----------------------------------------------------------------------------

// Capture builds a snapshot from the current state. Callers are responsible
// for ensuring no writes are in-flight while Capture runs (MVP-2 performs
// Capture during graceful shutdown only; live snapshots from a backup node
// arrive in MVP-8).
//
// offsets records the trade-event consumer's next-to-consume position per
// partition (ADR-0048). Pass nil (or empty) on cold paths; the callers in
// MVP code read it from service.Service.Offsets() after having flushed the
// Kafka transactional producer (output flush barrier).
//
// journalOffset is the next-to-consume position on this vshard's
// counter-journal partition at the moment Capture runs (ADR-0060 §4.1).
// Callers MUST Flush the TxnProducer before reading it and pass the
// result here; zero is acceptable for pre-0060 callers / cold start
// paths — catch-up on Restore becomes a no-op.
func Capture(shardID int, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, offsets map[int32]int64, journalOffset int64, tsMS int64) *ShardSnapshot {
	snap := &ShardSnapshot{
		Version:       Version,
		ShardID:       shardID,
		CounterSeq:    seq.CounterSeq(),
		TimestampMS:   tsMS,
		Offsets:       OffsetsMapToSlice(offsets),
		JournalOffset: journalOffset,
	}
	for _, userID := range state.Users() {
		acc := state.Account(userID)
		balances := acc.Copy()
		as := AccountSnapshot{
			UserID:            userID,
			Version:           acc.Version(),
			LastMatchSeq:      acc.MatchSeqSnapshot(),
			RecentTransferIDs: acc.RecentTransferIDsSnapshot(),
		}
		for asset, bal := range balances {
			as.Balances = append(as.Balances, BalanceSnapshot{
				Asset:     asset,
				Available: bal.Available.String(),
				Frozen:    bal.Frozen.String(),
				Version:   bal.Version,
			})
		}
		if terminated := acc.RecentTerminatedSnapshot(); len(terminated) > 0 {
			as.RecentTerminatedOrders = make([]TerminatedOrderEntrySnapshot, 0, len(terminated))
			for _, t := range terminated {
				as.RecentTerminatedOrders = append(as.RecentTerminatedOrders, TerminatedOrderEntrySnapshot{
					OrderID:       t.OrderID,
					FinalStatus:   uint8(t.FinalStatus),
					TerminatedAt:  t.TerminatedAt,
					ClientOrderID: t.ClientOrderID,
					Symbol:        t.Symbol,
				})
			}
		}
		snap.Accounts = append(snap.Accounts, as)
	}
	// Legacy dedup.Table is no longer the source of truth for Transfer
	// idempotency (ADR-0048 backlog item 4 方案 A moved to per-user ring
	// stored in AccountSnapshot). We still drain whatever entries happen
	// to be in dt — Service.Transfer no longer writes to it, so on steady
	// state this slice is empty, but older snapshots / tests may seed it.
	// Writing is kept so operators can mix pre-/post-ring builds without
	// losing audit entries mid-migration.
	for _, e := range dt.Snapshot() {
		snap.Dedup = append(snap.Dedup, DedupEntrySnapshot{
			Key:         e.Key,
			ExpiresUnix: e.ExpiresAt.UnixMilli(),
		})
	}
	for _, o := range state.Orders().All() {
		snap.Orders = append(snap.Orders, OrderSnapshot{
			ID:              o.ID,
			ClientOrderID:   o.ClientOrderID,
			UserID:          o.UserID,
			Symbol:          o.Symbol,
			Side:            uint8(o.Side),
			OrderType:       uint8(o.Type),
			TIF:             uint8(o.TIF),
			Price:           o.Price.String(),
			Qty:             o.Qty.String(),
			QuoteQty:        o.QuoteQty.String(),
			FilledQty:       o.FilledQty.String(),
			FrozenAsset:     o.FrozenAsset,
			FrozenAmount:    o.FrozenAmount.String(),
			FrozenSpent:     o.FrozenSpent.String(),
			Status:          uint8(o.Status),
			PreCancelStatus: uint8(o.PreCancelStatus),
			CreatedAt:       o.CreatedAt,
			UpdatedAt:       o.UpdatedAt,
			TerminatedAt:    o.TerminatedAt,
		})
	}
	for _, r := range state.AllReservations() {
		snap.Reservations = append(snap.Reservations, ReservationSnapshot{
			UserID:      r.UserID,
			RefID:       r.RefID,
			Asset:       r.Asset,
			Amount:      r.Amount.String(),
			CreatedAtMs: r.CreatedAtMs,
		})
	}
	return snap
}

// Restore loads state / seq id / dedup keys from snap. Caller MUST pass fresh
// (empty) state + sequencer + dedup table; Restore panics on misuse.
//
// Accepts v1 files (no offsets) as a one-time migration bridge: business
// state restores normally, snap.Offsets stays nil, main is expected to
// start the trade consumer at AtStart which equates to one full rescan
// guarded by existing dedup / shard_seq idempotency.
func Restore(shardID int, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, snap *ShardSnapshot) error {
	if snap == nil {
		return fmt.Errorf("snapshot is nil")
	}
	if snap.Version != versionV1 && snap.Version != versionV2 {
		return fmt.Errorf("snapshot version unsupported: got %d want %d or %d", snap.Version, versionV1, versionV2)
	}
	if snap.ShardID != shardID {
		return fmt.Errorf("snapshot shard mismatch: got %d want %d", snap.ShardID, shardID)
	}
	if len(state.Users()) != 0 {
		return fmt.Errorf("state is not empty")
	}
	for _, as := range snap.Accounts {
		acc := state.Account(as.UserID)
		for _, bs := range as.Balances {
			available, err := dec.Parse(bs.Available)
			if err != nil {
				return fmt.Errorf("account %s %s available: %w", as.UserID, bs.Asset, err)
			}
			frozen, err := dec.Parse(bs.Frozen)
			if err != nil {
				return fmt.Errorf("account %s %s frozen: %w", as.UserID, bs.Asset, err)
			}
			// Apply a synthetic deposit-like write directly through Account.
			// We intentionally bypass ApplyTransfer since this is restore, not
			// a normal state transition. Pass through Version so the
			// per-asset counter survives restart (ADR-0048 backlog: 双层
			// version 方案 B). Missing Version in an older snapshot file
			// decodes as 0, which is the correct cold-start value.
			putBalance(acc, bs.Asset, engine.Balance{
				Available: available,
				Frozen:    frozen,
				Version:   bs.Version,
			})
		}
		// Restore the per-symbol match_seq guard. Empty map for v1 snapshots
		// and for brand-new accounts — handlers treat missing entries as
		// zero, which lets the first event through and primes the guard.
		acc.RestoreMatchSeq(as.LastMatchSeq)
		// Restore the user-level version counter. Same semantics as above:
		// missing field decodes as 0.
		acc.RestoreVersion(as.Version)
		// Restore the recent-transfer ring (ADR-0048 backlog item 4).
		// Missing / empty slice in older snapshots yields an empty ring,
		// at which point any never-seen transfer_id flows through.
		acc.RestoreRecentTransferIDs(as.RecentTransferIDs)
		// Restore the recent-terminated-orders ring (ADR-0062). Missing
		// / empty in pre-0062 snapshots yields an empty ring, which is
		// semantically correct — on rollout the first wave of evictions
		// will rebuild it naturally.
		if len(as.RecentTerminatedOrders) > 0 {
			entries := make([]engine.TerminatedOrderEntry, 0, len(as.RecentTerminatedOrders))
			for _, t := range as.RecentTerminatedOrders {
				entries = append(entries, engine.TerminatedOrderEntry{
					OrderID:       t.OrderID,
					FinalStatus:   engine.OrderStatus(t.FinalStatus),
					TerminatedAt:  t.TerminatedAt,
					ClientOrderID: t.ClientOrderID,
					Symbol:        t.Symbol,
				})
			}
			acc.RestoreRecentTerminated(entries)
		}
	}
	seq.SetCounterSeq(snap.CounterSeq)

	// Restore the legacy dedup.Table only so pre-ring snapshots still load
	// without losing their entries in case operators re-export. Service is
	// no longer reading from dt for Transfer idempotency (ADR-0048 backlog
	// item 4 方案 A).
	if len(snap.Dedup) > 0 {
		entries := make([]dedup.Entry, 0, len(snap.Dedup))
		for _, e := range snap.Dedup {
			entries = append(entries, dedup.Entry{
				Key:       e.Key,
				Value:     nil, // tombstone
				ExpiresAt: time.UnixMilli(e.ExpiresUnix),
			})
		}
		dt.Restore(entries)
	}
	for _, os := range snap.Orders {
		price, err := dec.Parse(os.Price)
		if err != nil {
			return fmt.Errorf("order %d price: %w", os.ID, err)
		}
		qty, err := dec.Parse(os.Qty)
		if err != nil {
			return fmt.Errorf("order %d qty: %w", os.ID, err)
		}
		quoteQty, err := dec.Parse(os.QuoteQty)
		if err != nil {
			return fmt.Errorf("order %d quote_qty: %w", os.ID, err)
		}
		filled, err := dec.Parse(os.FilledQty)
		if err != nil {
			return fmt.Errorf("order %d filled_qty: %w", os.ID, err)
		}
		frozen, err := dec.Parse(os.FrozenAmount)
		if err != nil {
			return fmt.Errorf("order %d frozen_amount: %w", os.ID, err)
		}
		frozenSpent, err := dec.Parse(os.FrozenSpent)
		if err != nil {
			return fmt.Errorf("order %d frozen_spent: %w", os.ID, err)
		}
		state.Orders().RestoreInsert(&engine.Order{
			ID:              os.ID,
			ClientOrderID:   os.ClientOrderID,
			UserID:          os.UserID,
			Symbol:          os.Symbol,
			Side:            engine.Side(os.Side),
			Type:            engine.OrderType(os.OrderType),
			TIF:             engine.TIF(os.TIF),
			Price:           price,
			Qty:             qty,
			QuoteQty:        quoteQty,
			FilledQty:       filled,
			FrozenAsset:     os.FrozenAsset,
			FrozenAmount:    frozen,
			FrozenSpent:     frozenSpent,
			Status:          engine.OrderStatus(os.Status),
			PreCancelStatus: engine.OrderStatus(os.PreCancelStatus),
			CreatedAt:       os.CreatedAt,
			UpdatedAt:       os.UpdatedAt,
			TerminatedAt:    os.TerminatedAt,
		})
	}
	for _, rs := range snap.Reservations {
		amount, err := dec.Parse(rs.Amount)
		if err != nil {
			return fmt.Errorf("reservation %s amount: %w", rs.RefID, err)
		}
		state.RestoreReservation(&engine.Reservation{
			UserID:      rs.UserID,
			RefID:       rs.RefID,
			Asset:       rs.Asset,
			Amount:      amount,
			CreatedAtMs: rs.CreatedAtMs,
		})
	}
	return nil
}

// putBalance is a restore-only helper that writes a full balance onto an
// Account. It lives here rather than on engine.Account to keep engine's
// exported API transfer-only.
func putBalance(acc *engine.Account, asset string, b engine.Balance) {
	acc.PutForRestore(asset, b)
}

// -----------------------------------------------------------------------------
// Offset helpers (ADR-0048)
// -----------------------------------------------------------------------------

// OffsetsMapToSlice serialises the service's per-partition offsets map to
// the persisted KafkaOffset slice. The Topic field records the canonical
// trade-event topic so v2 files are self-describing.
func OffsetsMapToSlice(m map[int32]int64) []KafkaOffset {
	if len(m) == 0 {
		return nil
	}
	out := make([]KafkaOffset, 0, len(m))
	for p, o := range m {
		out = append(out, KafkaOffset{Topic: "trade-event", Partition: p, Offset: o})
	}
	return out
}

// OffsetsSliceToMap is the inverse. Duplicate partitions are reduced to the
// max — recovery must not go backwards.
func OffsetsSliceToMap(s []KafkaOffset) map[int32]int64 {
	if len(s) == 0 {
		return nil
	}
	out := make(map[int32]int64, len(s))
	for _, ko := range s {
		if ko.Offset > out[ko.Partition] {
			out[ko.Partition] = ko.Offset
		}
	}
	return out
}

// -----------------------------------------------------------------------------
// BlobStore I/O (ADR-0058)
// -----------------------------------------------------------------------------

// Save encodes snap and hands the blob to store under `baseKey + format.ext()`.
// baseKey is the filename stem without extension (e.g. "shard-0"); the
// format's extension is appended here so the store layer stays unaware of
// encoding.
func Save(ctx context.Context, store BlobStore, baseKey string, snap *ShardSnapshot, format Format) error {
	data, err := encode(snap, format)
	if err != nil {
		return fmt.Errorf("encode %s: %w", format, err)
	}
	if err := store.Put(ctx, baseKey+format.ext(), data); err != nil {
		return fmt.Errorf("put %s: %w", baseKey+format.ext(), err)
	}
	return nil
}

// Load fetches a snapshot by probing proto then json under baseKey.
// Returns os.ErrNotExist only when neither variant exists (callers treat
// that as cold start).
func Load(ctx context.Context, store BlobStore, baseKey string) (*ShardSnapshot, error) {
	// Probe in fallback order (ADR-0049): proto → json.
	for _, format := range []Format{FormatProto, FormatJSON} {
		key := baseKey + format.ext()
		data, err := store.Get(ctx, key)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		snap, err := decode(data, format)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", key, err)
		}
		return snap, nil
	}
	return nil, os.ErrNotExist
}

// encode dispatches to the format-specific encoder.
func encode(snap *ShardSnapshot, format Format) ([]byte, error) {
	switch format {
	case FormatProto:
		return proto.Marshal(toProto(snap))
	case FormatJSON:
		return json.MarshalIndent(snap, "", "  ")
	default:
		return nil, fmt.Errorf("snapshot: unknown format %d", format)
	}
}

// decode dispatches to the format-specific decoder.
func decode(data []byte, format Format) (*ShardSnapshot, error) {
	switch format {
	case FormatProto:
		var pb snapshotpb.CounterShardSnapshot
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return fromProto(&pb), nil
	case FormatJSON:
		var snap ShardSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, err
		}
		return &snap, nil
	default:
		return nil, fmt.Errorf("snapshot: unknown format %d", format)
	}
}

// -----------------------------------------------------------------------------
// Proto <-> ShardSnapshot mapping (ADR-0049)
// -----------------------------------------------------------------------------

func toProto(s *ShardSnapshot) *snapshotpb.CounterShardSnapshot {
	if s == nil {
		return nil
	}
	pb := &snapshotpb.CounterShardSnapshot{
		Version:       uint32(s.Version),
		ShardId:       int32(s.ShardID),
		CounterSeq:    s.CounterSeq,
		TimestampMs:   s.TimestampMS,
		JournalOffset: s.JournalOffset,
	}
	if len(s.Accounts) > 0 {
		pb.Accounts = make([]*snapshotpb.CounterAccount, 0, len(s.Accounts))
		for i := range s.Accounts {
			pb.Accounts = append(pb.Accounts, accountToProto(&s.Accounts[i]))
		}
	}
	if len(s.Orders) > 0 {
		pb.Orders = make([]*snapshotpb.CounterOrder, 0, len(s.Orders))
		for i := range s.Orders {
			pb.Orders = append(pb.Orders, orderToProto(&s.Orders[i]))
		}
	}
	if len(s.Dedup) > 0 {
		pb.Dedup = make([]*snapshotpb.CounterDedupEntry, 0, len(s.Dedup))
		for _, d := range s.Dedup {
			pb.Dedup = append(pb.Dedup, &snapshotpb.CounterDedupEntry{
				Key: d.Key, ExpiresUnixMs: d.ExpiresUnix,
			})
		}
	}
	if len(s.Reservations) > 0 {
		pb.Reservations = make([]*snapshotpb.CounterReservation, 0, len(s.Reservations))
		for _, r := range s.Reservations {
			pb.Reservations = append(pb.Reservations, &snapshotpb.CounterReservation{
				UserId: r.UserID, RefId: r.RefID, Asset: r.Asset,
				Amount: r.Amount, CreatedAtMs: r.CreatedAtMs,
			})
		}
	}
	if len(s.Offsets) > 0 {
		pb.Offsets = make([]*snapshotpb.CounterKafkaOffset, 0, len(s.Offsets))
		for _, o := range s.Offsets {
			pb.Offsets = append(pb.Offsets, &snapshotpb.CounterKafkaOffset{
				Topic: o.Topic, Partition: o.Partition, Offset: o.Offset,
			})
		}
	}
	return pb
}

func accountToProto(a *AccountSnapshot) *snapshotpb.CounterAccount {
	out := &snapshotpb.CounterAccount{
		UserId:            a.UserID,
		Version:           a.Version,
		LastMatchSeq:      a.LastMatchSeq,
		RecentTransferIds: a.RecentTransferIDs,
	}
	if len(a.Balances) > 0 {
		out.Balances = make([]*snapshotpb.CounterBalance, 0, len(a.Balances))
		for _, b := range a.Balances {
			out.Balances = append(out.Balances, &snapshotpb.CounterBalance{
				Asset:     b.Asset,
				Available: b.Available,
				Frozen:    b.Frozen,
				Version:   b.Version,
			})
		}
	}
	if len(a.RecentTerminatedOrders) > 0 {
		out.RecentTerminatedOrders = make([]*snapshotpb.CounterTerminatedOrder, 0, len(a.RecentTerminatedOrders))
		for _, t := range a.RecentTerminatedOrders {
			out.RecentTerminatedOrders = append(out.RecentTerminatedOrders, &snapshotpb.CounterTerminatedOrder{
				OrderId:       t.OrderID,
				FinalStatus:   uint32(t.FinalStatus),
				TerminatedAt:  t.TerminatedAt,
				ClientOrderId: t.ClientOrderID,
				Symbol:        t.Symbol,
			})
		}
	}
	return out
}

func orderToProto(o *OrderSnapshot) *snapshotpb.CounterOrder {
	return &snapshotpb.CounterOrder{
		Id:              o.ID,
		ClientOrderId:   o.ClientOrderID,
		UserId:          o.UserID,
		Symbol:          o.Symbol,
		Side:            uint32(o.Side),
		Type:            uint32(o.OrderType),
		Tif:             uint32(o.TIF),
		Price:           o.Price,
		Qty:             o.Qty,
		QuoteQty:        o.QuoteQty,
		FilledQty:       o.FilledQty,
		FrozenAsset:     o.FrozenAsset,
		FrozenAmount:    o.FrozenAmount,
		FrozenSpent:     o.FrozenSpent,
		Status:          uint32(o.Status),
		PreCancelStatus: uint32(o.PreCancelStatus),
		CreatedAt:       o.CreatedAt,
		UpdatedAt:       o.UpdatedAt,
		TerminatedAt:    o.TerminatedAt,
	}
}

func fromProto(pb *snapshotpb.CounterShardSnapshot) *ShardSnapshot {
	if pb == nil {
		return nil
	}
	s := &ShardSnapshot{
		Version:       int(pb.Version),
		ShardID:       int(pb.ShardId),
		CounterSeq:    pb.CounterSeq,
		TimestampMS:   pb.TimestampMs,
		JournalOffset: pb.JournalOffset,
	}
	if n := len(pb.Accounts); n > 0 {
		s.Accounts = make([]AccountSnapshot, 0, n)
		for _, a := range pb.Accounts {
			s.Accounts = append(s.Accounts, accountFromProto(a))
		}
	}
	if n := len(pb.Orders); n > 0 {
		s.Orders = make([]OrderSnapshot, 0, n)
		for _, o := range pb.Orders {
			s.Orders = append(s.Orders, orderFromProto(o))
		}
	}
	if n := len(pb.Dedup); n > 0 {
		s.Dedup = make([]DedupEntrySnapshot, 0, n)
		for _, d := range pb.Dedup {
			s.Dedup = append(s.Dedup, DedupEntrySnapshot{
				Key:         d.Key,
				ExpiresUnix: d.ExpiresUnixMs,
			})
		}
	}
	if n := len(pb.Reservations); n > 0 {
		s.Reservations = make([]ReservationSnapshot, 0, n)
		for _, r := range pb.Reservations {
			s.Reservations = append(s.Reservations, ReservationSnapshot{
				UserID:      r.UserId,
				RefID:       r.RefId,
				Asset:       r.Asset,
				Amount:      r.Amount,
				CreatedAtMs: r.CreatedAtMs,
			})
		}
	}
	if n := len(pb.Offsets); n > 0 {
		s.Offsets = make([]KafkaOffset, 0, n)
		for _, o := range pb.Offsets {
			s.Offsets = append(s.Offsets, KafkaOffset{
				Topic: o.Topic, Partition: o.Partition, Offset: o.Offset,
			})
		}
	}
	return s
}

func accountFromProto(a *snapshotpb.CounterAccount) AccountSnapshot {
	out := AccountSnapshot{
		UserID:            a.UserId,
		Version:           a.Version,
		LastMatchSeq:      a.LastMatchSeq,
		RecentTransferIDs: a.RecentTransferIds,
	}
	if n := len(a.Balances); n > 0 {
		out.Balances = make([]BalanceSnapshot, 0, n)
		for _, b := range a.Balances {
			out.Balances = append(out.Balances, BalanceSnapshot{
				Asset:     b.Asset,
				Available: b.Available,
				Frozen:    b.Frozen,
				Version:   b.Version,
			})
		}
	}
	if n := len(a.RecentTerminatedOrders); n > 0 {
		out.RecentTerminatedOrders = make([]TerminatedOrderEntrySnapshot, 0, n)
		for _, t := range a.RecentTerminatedOrders {
			out.RecentTerminatedOrders = append(out.RecentTerminatedOrders, TerminatedOrderEntrySnapshot{
				OrderID:       t.OrderId,
				FinalStatus:   uint8(t.FinalStatus),
				TerminatedAt:  t.TerminatedAt,
				ClientOrderID: t.ClientOrderId,
				Symbol:        t.Symbol,
			})
		}
	}
	return out
}

func orderFromProto(o *snapshotpb.CounterOrder) OrderSnapshot {
	return OrderSnapshot{
		ID:              o.Id,
		ClientOrderID:   o.ClientOrderId,
		UserID:          o.UserId,
		Symbol:          o.Symbol,
		Side:            uint8(o.Side),
		OrderType:       uint8(o.Type),
		TIF:             uint8(o.Tif),
		Price:           o.Price,
		Qty:             o.Qty,
		QuoteQty:        o.QuoteQty,
		FilledQty:       o.FilledQty,
		FrozenAsset:     o.FrozenAsset,
		FrozenAmount:    o.FrozenAmount,
		FrozenSpent:     o.FrozenSpent,
		Status:          uint8(o.Status),
		PreCancelStatus: uint8(o.PreCancelStatus),
		CreatedAt:       o.CreatedAt,
		UpdatedAt:       o.UpdatedAt,
		TerminatedAt:    o.TerminatedAt,
	}
}

// LoadPath preserves the original single-key semantics for tools that
// already pass an explicit key with extension. If key ends in .pb or
// .json, only that format is attempted; otherwise fall through to Load
// which probes both.
func LoadPath(ctx context.Context, store BlobStore, key string) (*ShardSnapshot, error) {
	switch filepath.Ext(key) {
	case ".pb":
		data, err := store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return decode(data, FormatProto)
	case ".json":
		data, err := store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		return decode(data, FormatJSON)
	default:
		return Load(ctx, store, key)
	}
}
