// Package snapshot serializes a SymbolWorker's orderbook state to local disk
// and restores it on startup.
//
// ADR-0049 made the on-disk format selectable: default protobuf (.pb) with
// JSON (.json) as a debug fallback selected via --snapshot-format=json.
// Load probes .pb first, then .json, so in-place upgrades just work.
package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// Format names the on-disk encoding (ADR-0049).
type Format int

const (
	FormatProto Format = iota
	FormatJSON
)

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

// ParseFormat maps a CLI token to Format.
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

// Version marks the snapshot format version. Bump when the schema changes.
const Version = 1

// KafkaOffset records the consumer position for a (topic, partition).
// Included in the snapshot so that recovery can resume from the exact point
// the snapshot was taken.
type KafkaOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// OrderSnapshot is the serialized form of an orderbook.Order.
type OrderSnapshot struct {
	ID        uint64 `json:"id"`
	UserID    string `json:"user_id"`
	ClientID  string `json:"client_id,omitempty"`
	Side      uint8  `json:"side"`
	Type      uint8  `json:"type"`
	TIF       uint8  `json:"tif"`
	Price     string `json:"price"`
	Qty       string `json:"qty"`
	Remaining string `json:"remaining"`
	CreatedAt int64  `json:"created_at"`
}

// SymbolSnapshot is the full on-disk representation of one SymbolWorker.
type SymbolSnapshot struct {
	Version     int             `json:"version"`
	Symbol      string          `json:"symbol"`
	MatchSeqID  uint64          `json:"match_seq_id"`
	Offsets     []KafkaOffset   `json:"offsets"`
	Orders      []OrderSnapshot `json:"orders"`
	TimestampMS int64           `json:"ts_unix_ms"`
}

// -----------------------------------------------------------------------------
// Capture / restore between Worker <-> Snapshot
// -----------------------------------------------------------------------------

// Capture extracts the current state of w into a SymbolSnapshot. Safe to
// call while the worker goroutine is running — it takes the worker's state
// lock (WithStateLocked) so book / matchSeq / offsets are read as a
// consistent triple. timestampMS is the wall-clock time at which the
// snapshot is taken (informational).
//
// Per-partition offsets are sourced from the worker itself (ADR-0048). The
// snapshot file thus becomes the authoritative consumer position on restart;
// callers do NOT pass offsets explicitly anymore.
func Capture(w *sequencer.SymbolWorker, timestampMS int64) *SymbolSnapshot {
	snap := &SymbolSnapshot{
		Version:     Version,
		Symbol:      w.Symbol(),
		TimestampMS: timestampMS,
	}
	w.WithStateLocked(func(book *orderbook.Book, matchSeq uint64, offsets map[int32]int64) {
		snap.MatchSeqID = matchSeq
		snap.Offsets = offsetsMapToSlice(offsets)
		book.Walk(orderbook.Bid, func(o *orderbook.Order) bool {
			snap.Orders = append(snap.Orders, toSnapshot(o))
			return true
		})
		book.Walk(orderbook.Ask, func(o *orderbook.Order) bool {
			snap.Orders = append(snap.Orders, toSnapshot(o))
			return true
		})
	})
	return snap
}

// Restore rebuilds the worker's state from snap. The worker MUST be freshly
// constructed (empty book, default match_seq_id); calling Restore on a
// worker that has already processed events panics.
func Restore(w *sequencer.SymbolWorker, snap *SymbolSnapshot) error {
	if w.Book().Len() != 0 || w.MatchSeq() != 0 {
		return fmt.Errorf("snapshot.Restore: worker is not empty")
	}
	if snap.Symbol != w.Symbol() {
		return fmt.Errorf("snapshot.Restore: symbol mismatch: %q vs %q", snap.Symbol, w.Symbol())
	}
	if snap.Version != Version {
		return fmt.Errorf("snapshot.Restore: version mismatch: got %d want %d", snap.Version, Version)
	}
	for i := range snap.Orders {
		o, err := fromSnapshot(snap.Symbol, snap.Orders[i])
		if err != nil {
			return fmt.Errorf("snapshot.Restore: order %d: %w", snap.Orders[i].ID, err)
		}
		if err := w.Book().Insert(o); err != nil {
			return fmt.Errorf("snapshot.Restore: insert order %d: %w", o.ID, err)
		}
	}
	w.SetMatchSeq(snap.MatchSeqID)
	w.SetOffsets(offsetsSliceToMap(snap.Offsets))
	return nil
}

// offsetsMapToSlice serialises the worker's offsets map to the on-disk
// KafkaOffset slice. The Topic field is populated with the canonical
// order-event topic — in the current deployment all events share one topic,
// but embedding it keeps the schema robust against future multi-topic setups.
func offsetsMapToSlice(m map[int32]int64) []KafkaOffset {
	if len(m) == 0 {
		return nil
	}
	out := make([]KafkaOffset, 0, len(m))
	for p, o := range m {
		out = append(out, KafkaOffset{Topic: "order-event", Partition: p, Offset: o})
	}
	return out
}

// offsetsSliceToMap reverses offsetsMapToSlice. Multiple entries for the same
// partition (shouldn't happen in well-formed snapshots but guard anyway) are
// reduced to the max — recovery must not go backwards.
func offsetsSliceToMap(s []KafkaOffset) map[int32]int64 {
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
// Disk I/O
// -----------------------------------------------------------------------------

// Save writes snap to disk atomically (ADR-0049). basePath is the filename
// without extension; Save appends `.pb` / `.json` per format.
func Save(basePath string, snap *SymbolSnapshot, format Format) error {
	if err := os.MkdirAll(filepath.Dir(basePath), 0o755); err != nil {
		return fmt.Errorf("snapshot.Save: mkdir: %w", err)
	}
	data, err := encode(snap, format)
	if err != nil {
		return fmt.Errorf("snapshot.Save: encode %s: %w", format, err)
	}
	path := basePath + format.ext()
	tmp := path + ".tmp"
	if err := writeAtomic(tmp, path, data); err != nil {
		return fmt.Errorf("snapshot.Save: %w", err)
	}
	return nil
}

// Load reads a snapshot from disk. Probes .pb first, then .json; returns
// os.ErrNotExist only when neither exists.
func Load(basePath string) (*SymbolSnapshot, error) {
	for _, format := range []Format{FormatProto, FormatJSON} {
		path := basePath + format.ext()
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("snapshot.Load: read %s: %w", path, err)
		}
		snap, err := decode(data, format)
		if err != nil {
			return nil, fmt.Errorf("snapshot.Load: decode %s: %w", path, err)
		}
		return snap, nil
	}
	return nil, os.ErrNotExist
}

func writeAtomic(tmp, path string, data []byte) error {
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write: %w", err)
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

func encode(snap *SymbolSnapshot, format Format) ([]byte, error) {
	switch format {
	case FormatProto:
		return proto.Marshal(toProto(snap))
	case FormatJSON:
		return json.MarshalIndent(snap, "", "  ")
	default:
		return nil, fmt.Errorf("snapshot: unknown format %d", format)
	}
}

func decode(data []byte, format Format) (*SymbolSnapshot, error) {
	switch format {
	case FormatProto:
		var pb snapshotpb.MatchSymbolSnapshot
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return fromProto(&pb), nil
	case FormatJSON:
		var snap SymbolSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, err
		}
		return &snap, nil
	default:
		return nil, fmt.Errorf("snapshot: unknown format %d", format)
	}
}

// -----------------------------------------------------------------------------
// Proto <-> SymbolSnapshot mapping (ADR-0049)
// -----------------------------------------------------------------------------

func toProto(s *SymbolSnapshot) *snapshotpb.MatchSymbolSnapshot {
	if s == nil {
		return nil
	}
	pb := &snapshotpb.MatchSymbolSnapshot{
		Version:     uint32(s.Version),
		Symbol:      s.Symbol,
		MatchSeqId:  s.MatchSeqID,
		TimestampMs: s.TimestampMS,
	}
	if n := len(s.Offsets); n > 0 {
		pb.Offsets = make([]*snapshotpb.MatchKafkaOffset, 0, n)
		for _, o := range s.Offsets {
			pb.Offsets = append(pb.Offsets, &snapshotpb.MatchKafkaOffset{
				Topic: o.Topic, Partition: o.Partition, Offset: o.Offset,
			})
		}
	}
	if n := len(s.Orders); n > 0 {
		pb.Orders = make([]*snapshotpb.MatchOrder, 0, n)
		for i := range s.Orders {
			o := &s.Orders[i]
			pb.Orders = append(pb.Orders, &snapshotpb.MatchOrder{
				Id:        o.ID,
				UserId:    o.UserID,
				ClientId:  o.ClientID,
				Side:      uint32(o.Side),
				Type:      uint32(o.Type),
				Tif:       uint32(o.TIF),
				Price:     o.Price,
				Qty:       o.Qty,
				Remaining: o.Remaining,
				CreatedAt: o.CreatedAt,
			})
		}
	}
	return pb
}

func fromProto(pb *snapshotpb.MatchSymbolSnapshot) *SymbolSnapshot {
	if pb == nil {
		return nil
	}
	s := &SymbolSnapshot{
		Version:     int(pb.Version),
		Symbol:      pb.Symbol,
		MatchSeqID:  pb.MatchSeqId,
		TimestampMS: pb.TimestampMs,
	}
	if n := len(pb.Offsets); n > 0 {
		s.Offsets = make([]KafkaOffset, 0, n)
		for _, o := range pb.Offsets {
			s.Offsets = append(s.Offsets, KafkaOffset{
				Topic: o.Topic, Partition: o.Partition, Offset: o.Offset,
			})
		}
	}
	if n := len(pb.Orders); n > 0 {
		s.Orders = make([]OrderSnapshot, 0, n)
		for _, o := range pb.Orders {
			s.Orders = append(s.Orders, OrderSnapshot{
				ID:        o.Id,
				UserID:    o.UserId,
				ClientID:  o.ClientId,
				Side:      uint8(o.Side),
				Type:      uint8(o.Type),
				TIF:       uint8(o.Tif),
				Price:     o.Price,
				Qty:       o.Qty,
				Remaining: o.Remaining,
				CreatedAt: o.CreatedAt,
			})
		}
	}
	return s
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func toSnapshot(o *orderbook.Order) OrderSnapshot {
	return OrderSnapshot{
		ID:        o.ID,
		UserID:    o.UserID,
		ClientID:  o.ClientID,
		Side:      uint8(o.Side),
		Type:      uint8(o.Type),
		TIF:       uint8(o.TIF),
		Price:     o.Price.String(),
		Qty:       o.Qty.String(),
		Remaining: o.Remaining.String(),
		CreatedAt: o.CreatedAt,
	}
}

func fromSnapshot(symbol string, s OrderSnapshot) (*orderbook.Order, error) {
	price, err := dec.Parse(s.Price)
	if err != nil {
		return nil, fmt.Errorf("price: %w", err)
	}
	qty, err := dec.Parse(s.Qty)
	if err != nil {
		return nil, fmt.Errorf("qty: %w", err)
	}
	rem, err := dec.Parse(s.Remaining)
	if err != nil {
		return nil, fmt.Errorf("remaining: %w", err)
	}
	return &orderbook.Order{
		ID:        s.ID,
		UserID:    s.UserID,
		ClientID:  s.ClientID,
		Symbol:    symbol,
		Side:      orderbook.Side(s.Side),
		Type:      orderbook.OrderType(s.Type),
		TIF:       orderbook.TIF(s.TIF),
		Price:     price,
		Qty:       qty,
		Remaining: rem,
		CreatedAt: s.CreatedAt,
	}, nil
}
