// Package snapshot serializes Quote's in-memory engine state (per-symbol
// depth book + kline aggregators + emit sequence + per-partition
// trade-event offsets) to local disk, so a restart can resume from the
// saved offsets instead of rescanning the entire topic (ADR-0025 §未来
// 工作, ADR-0036).
//
// ADR-0049: default on-disk format is protobuf (.pb); JSON (.json) stays
// available as a debug fallback via --snapshot-format=json. Load probes
// .pb first, then .json.
package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	snapshotpb "github.com/xargin/opentrade/api/gen/snapshot"
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

// Version of the on-disk format.
const Version = 1

// Snapshot is the full on-disk representation of the engine.
type Snapshot struct {
	Version   int                        `json:"version"`
	TakenAtMs int64                      `json:"taken_at_ms"`
	Seq       uint64                     `json:"seq"`
	Offsets   map[int32]int64            `json:"offsets"`           // partition → next-to-consume
	Symbols   map[string]*SymbolSnapshot `json:"symbols,omitempty"` // symbol → state
}

// SymbolSnapshot captures everything engine tracks for one symbol.
type SymbolSnapshot struct {
	Depth *DepthSnapshot `json:"depth,omitempty"`
	Kline *KlineSnapshot `json:"kline,omitempty"`
}

// DepthSnapshot captures depth.Book state.
type DepthSnapshot struct {
	Symbol string           `json:"symbol"`
	Bids   map[string]string `json:"bids,omitempty"`   // priceKey → qty
	Asks   map[string]string `json:"asks,omitempty"`   // priceKey → qty
	Prices map[string]string `json:"prices,omitempty"` // priceKey → price canonical string
	Orders []OrderRefSnap    `json:"orders,omitempty"`
}

// OrderRefSnap is a resting order as tracked by depth.Book.
type OrderRefSnap struct {
	OrderID   uint64 `json:"order_id"`
	Side      uint8  `json:"side"` // 1=buy/bid, 2=sell/ask
	PriceKey  string `json:"price_key"`
	Remaining string `json:"remaining"`
}

// KlineSnapshot captures kline.Aggregator state.
type KlineSnapshot struct {
	Symbol string            `json:"symbol"`
	Bars   map[int32]BarSnap `json:"bars,omitempty"` // KlineInterval enum int → bar
}

// BarSnap is an open candlestick.
type BarSnap struct {
	OpenTimeMs  int64  `json:"open_time_ms"`
	CloseTimeMs int64  `json:"close_time_ms"`
	Open        string `json:"open"`
	High        string `json:"high"`
	Low         string `json:"low"`
	Close       string `json:"close"`
	Volume      string `json:"volume"`
	QuoteVolume string `json:"quote_volume"`
	Count       uint64 `json:"count"`
}

// -----------------------------------------------------------------------------
// Disk I/O
// -----------------------------------------------------------------------------

// Save writes snap to disk atomically (ADR-0049). basePath is without
// extension; Save appends `.pb` / `.json` per format.
func Save(basePath string, snap *Snapshot, format Format) error {
	if err := os.MkdirAll(filepath.Dir(basePath), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	data, err := encode(snap, format)
	if err != nil {
		return fmt.Errorf("encode %s: %w", format, err)
	}
	path := basePath + format.ext()
	tmp := path + ".tmp"
	return writeAtomic(tmp, path, data)
}

// Load reads a snapshot from disk. Probes .pb first, then .json. Missing
// both → (nil, nil) so callers treat absence as cold start.
func Load(basePath string) (*Snapshot, error) {
	for _, format := range []Format{FormatProto, FormatJSON} {
		path := basePath + format.ext()
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		snap, err := decode(data, format)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", path, err)
		}
		if snap.Version != Version {
			return nil, fmt.Errorf("snapshot version mismatch: got %d want %d", snap.Version, Version)
		}
		return snap, nil
	}
	return nil, nil
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

func encode(snap *Snapshot, format Format) ([]byte, error) {
	switch format {
	case FormatProto:
		return proto.Marshal(toProto(snap))
	case FormatJSON:
		return json.MarshalIndent(snap, "", "  ")
	default:
		return nil, fmt.Errorf("snapshot: unknown format %d", format)
	}
}

func decode(data []byte, format Format) (*Snapshot, error) {
	switch format {
	case FormatProto:
		var pb snapshotpb.QuoteSnapshot
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, err
		}
		return fromProto(&pb), nil
	case FormatJSON:
		var snap Snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, err
		}
		return &snap, nil
	default:
		return nil, fmt.Errorf("snapshot: unknown format %d", format)
	}
}

// -----------------------------------------------------------------------------
// Proto <-> Snapshot mapping (ADR-0049)
// -----------------------------------------------------------------------------

func toProto(s *Snapshot) *snapshotpb.QuoteSnapshot {
	if s == nil {
		return nil
	}
	pb := &snapshotpb.QuoteSnapshot{
		Version:   uint32(s.Version),
		TakenAtMs: s.TakenAtMs,
		Seq:       s.Seq,
		Offsets:   s.Offsets,
	}
	if len(s.Symbols) > 0 {
		pb.Symbols = make(map[string]*snapshotpb.QuoteSymbolState, len(s.Symbols))
		for sym, st := range s.Symbols {
			pb.Symbols[sym] = symbolToProto(st)
		}
	}
	return pb
}

func symbolToProto(s *SymbolSnapshot) *snapshotpb.QuoteSymbolState {
	if s == nil {
		return nil
	}
	out := &snapshotpb.QuoteSymbolState{}
	if s.Depth != nil {
		d := s.Depth
		out.Depth = &snapshotpb.QuoteDepth{
			Symbol: d.Symbol,
			Bids:   d.Bids,
			Asks:   d.Asks,
			Prices: d.Prices,
		}
		if n := len(d.Orders); n > 0 {
			out.Depth.Orders = make([]*snapshotpb.QuoteOrderRef, 0, n)
			for _, o := range d.Orders {
				out.Depth.Orders = append(out.Depth.Orders, &snapshotpb.QuoteOrderRef{
					OrderId:   o.OrderID,
					Side:      uint32(o.Side),
					PriceKey:  o.PriceKey,
					Remaining: o.Remaining,
				})
			}
		}
	}
	if s.Kline != nil {
		k := s.Kline
		out.Kline = &snapshotpb.QuoteKline{Symbol: k.Symbol}
		if len(k.Bars) > 0 {
			out.Kline.Bars = make(map[int32]*snapshotpb.QuoteBar, len(k.Bars))
			for iv, b := range k.Bars {
				out.Kline.Bars[iv] = &snapshotpb.QuoteBar{
					OpenTimeMs:  b.OpenTimeMs,
					CloseTimeMs: b.CloseTimeMs,
					Open:        b.Open,
					High:        b.High,
					Low:         b.Low,
					Close:       b.Close,
					Volume:      b.Volume,
					QuoteVolume: b.QuoteVolume,
					Count:       b.Count,
				}
			}
		}
	}
	return out
}

func fromProto(pb *snapshotpb.QuoteSnapshot) *Snapshot {
	if pb == nil {
		return nil
	}
	s := &Snapshot{
		Version:   int(pb.Version),
		TakenAtMs: pb.TakenAtMs,
		Seq:       pb.Seq,
		Offsets:   pb.Offsets,
	}
	if len(pb.Symbols) > 0 {
		s.Symbols = make(map[string]*SymbolSnapshot, len(pb.Symbols))
		for sym, st := range pb.Symbols {
			s.Symbols[sym] = symbolFromProto(st)
		}
	}
	return s
}

func symbolFromProto(pb *snapshotpb.QuoteSymbolState) *SymbolSnapshot {
	if pb == nil {
		return nil
	}
	out := &SymbolSnapshot{}
	if pb.Depth != nil {
		d := pb.Depth
		out.Depth = &DepthSnapshot{
			Symbol: d.Symbol,
			Bids:   d.Bids,
			Asks:   d.Asks,
			Prices: d.Prices,
		}
		if n := len(d.Orders); n > 0 {
			out.Depth.Orders = make([]OrderRefSnap, 0, n)
			for _, o := range d.Orders {
				out.Depth.Orders = append(out.Depth.Orders, OrderRefSnap{
					OrderID:   o.OrderId,
					Side:      uint8(o.Side),
					PriceKey:  o.PriceKey,
					Remaining: o.Remaining,
				})
			}
		}
	}
	if pb.Kline != nil {
		k := pb.Kline
		out.Kline = &KlineSnapshot{Symbol: k.Symbol}
		if len(k.Bars) > 0 {
			out.Kline.Bars = make(map[int32]BarSnap, len(k.Bars))
			for iv, b := range k.Bars {
				out.Kline.Bars[iv] = BarSnap{
					OpenTimeMs:  b.OpenTimeMs,
					CloseTimeMs: b.CloseTimeMs,
					Open:        b.Open,
					High:        b.High,
					Low:         b.Low,
					Close:       b.Close,
					Volume:      b.Volume,
					QuoteVolume: b.QuoteVolume,
					Count:       b.Count,
				}
			}
		}
	}
	return out
}
