// Package snapshot serializes Counter's ShardState + Dedup + UserSequencer
// shard seq id to local disk and restores it on startup.
//
// MVP-2 scope: single-file JSON per shard, atomic write (tmp + rename). S3
// upload + periodic capture from the backup node land in MVP-8 (ADR-0006).
package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// Version of the snapshot format.
const Version = 1

// BalanceSnapshot is the serialized form of engine.Balance.
type BalanceSnapshot struct {
	Asset     string `json:"asset"`
	Available string `json:"available"`
	Frozen    string `json:"frozen"`
}

// AccountSnapshot captures one user's balances.
type AccountSnapshot struct {
	UserID   string            `json:"user_id"`
	Balances []BalanceSnapshot `json:"balances"`
}

// DedupEntrySnapshot captures a single transfer_id dedup record. For MVP-2
// we persist only the key + expiry; the cached response payload is rebuilt
// as a tombstone result on restore (CONFIRMED with empty balance) since the
// canonical truth is Kafka journal. If the caller re-hits the same
// transfer_id after restart and we had previously answered it, a tombstone
// reply is acceptable; the alternative would be encoding the result into the
// snapshot blob.
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
	ShardSeq     uint64                `json:"shard_seq"`
	TimestampMS  int64                 `json:"ts_unix_ms"`
	Accounts     []AccountSnapshot     `json:"accounts"`
	Orders       []OrderSnapshot       `json:"orders,omitempty"`
	Dedup        []DedupEntrySnapshot  `json:"dedup,omitempty"`
	Reservations []ReservationSnapshot `json:"reservations,omitempty"`
}

// -----------------------------------------------------------------------------
// Capture / Restore
// -----------------------------------------------------------------------------

// Capture builds a snapshot from the current state. Callers are responsible
// for ensuring no writes are in-flight while Capture runs (MVP-2 performs
// Capture during graceful shutdown only; live snapshots from a backup node
// arrive in MVP-8).
func Capture(shardID int, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, tsMS int64) *ShardSnapshot {
	snap := &ShardSnapshot{
		Version:     Version,
		ShardID:     shardID,
		ShardSeq:    seq.ShardSeq(),
		TimestampMS: tsMS,
	}
	for _, userID := range state.Users() {
		acc := state.Account(userID)
		balances := acc.Copy()
		as := AccountSnapshot{UserID: userID}
		for asset, bal := range balances {
			as.Balances = append(as.Balances, BalanceSnapshot{
				Asset:     asset,
				Available: bal.Available.String(),
				Frozen:    bal.Frozen.String(),
			})
		}
		snap.Accounts = append(snap.Accounts, as)
	}
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
func Restore(shardID int, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, snap *ShardSnapshot) error {
	if snap == nil {
		return fmt.Errorf("snapshot is nil")
	}
	if snap.Version != Version {
		return fmt.Errorf("snapshot version mismatch: got %d want %d", snap.Version, Version)
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
			// a normal state transition.
			putBalance(acc, bs.Asset, engine.Balance{Available: available, Frozen: frozen})
		}
	}
	seq.SetShardSeq(snap.ShardSeq)

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
// Disk I/O
// -----------------------------------------------------------------------------

// Save writes snap to path atomically (tmp + rename + fsync).
func Save(path string, snap *ShardSnapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(snap); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("encode: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// Load reads a snapshot from path.
func Load(path string) (*ShardSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var snap ShardSnapshot
	if err := json.NewDecoder(f).Decode(&snap); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &snap, nil
}
