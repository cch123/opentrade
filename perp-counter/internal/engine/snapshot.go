package engine

import (
	"sort"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// Snapshot is the serializable image of engine state. Decimals are strings
// (ADR-0049 convention) so the same shape serializes cleanly to JSON now and
// to the perp snapshot proto later. The real recovery path will bind this to
// per-partition Kafka offsets (ADR-0048 invariant #5); this struct is the
// state half.
type Snapshot struct {
	Wallets   []WalletSnap      `json:"wallets"`
	Positions []PositionSnap    `json:"positions"`
	Marks     map[string]string `json:"marks"`
	Insurance map[string]string `json:"insurance"`
}

// WalletSnap is one user's margin balance.
type WalletSnap struct {
	UserID    string `json:"user_id"`
	Available string `json:"available"`
	Reserved  string `json:"reserved"`
}

// PositionSnap is one (user, symbol) position with its recovery watermarks.
type PositionSnap struct {
	UserID           string `json:"user_id"`
	Symbol           string `json:"symbol"`
	Side             uint8  `json:"side"`
	Size             string `json:"size"`
	Entry            string `json:"entry"`
	Margin           string `json:"margin"`
	Leverage         string `json:"leverage"`
	Realized         string `json:"realized"`
	Mode             uint8  `json:"mode"`
	LastMatchSeq     uint64 `json:"last_match_seq"`
	FundingRoundSeen int64  `json:"funding_round_seen"`
	Version          uint64 `json:"version"`
}

// Snapshot captures current state. Output is deterministic (sorted) so
// round-trips and golden comparisons are stable.
func (e *Engine) Snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s := Snapshot{Marks: map[string]string{}, Insurance: map[string]string{}}

	users := make([]string, 0, len(e.wallets))
	for u := range e.wallets {
		users = append(users, u)
	}
	sort.Strings(users)
	for _, u := range users {
		w := e.wallets[u]
		s.Wallets = append(s.Wallets, WalletSnap{
			UserID: u, Available: w.Available.String(), Reserved: w.Reserved.String(),
		})
	}

	posUsers := make([]string, 0, len(e.positions))
	for u := range e.positions {
		posUsers = append(posUsers, u)
	}
	sort.Strings(posUsers)
	for _, u := range posUsers {
		bySym := e.positions[u]
		syms := make([]string, 0, len(bySym))
		for sym := range bySym {
			syms = append(syms, sym)
		}
		sort.Strings(syms)
		for _, sym := range syms {
			p := bySym[sym]
			s.Positions = append(s.Positions, PositionSnap{
				UserID: p.UserID, Symbol: p.Symbol, Side: uint8(p.Side),
				Size: p.Size.String(), Entry: p.Entry.String(), Margin: p.Margin.String(),
				Leverage: p.Leverage.String(), Realized: p.Realized.String(), Mode: uint8(p.Mode),
				LastMatchSeq: p.LastMatchSeq, FundingRoundSeen: p.FundingRoundSeen, Version: p.Version,
			})
		}
	}

	for sym, m := range e.marks {
		s.Marks[sym] = m.String()
	}
	for sym, f := range e.insurance {
		s.Insurance[sym] = f.String()
	}
	return s
}

// Restore replaces engine state with s. Used at startup after loading the
// snapshot blob (and, in the full pipeline, before seeking Kafka to the
// bound offsets).
func (e *Engine) Restore(s Snapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.wallets = map[string]*Wallet{}
	e.positions = map[string]map[string]*perpstate.Position{}
	e.marks = map[string]dec.Decimal{}
	e.insurance = map[string]dec.Decimal{}

	for _, w := range s.Wallets {
		e.wallets[w.UserID] = &Wallet{Available: dec.New(w.Available), Reserved: dec.New(w.Reserved)}
	}
	for _, ps := range s.Positions {
		bySym := e.positions[ps.UserID]
		if bySym == nil {
			bySym = map[string]*perpstate.Position{}
			e.positions[ps.UserID] = bySym
		}
		bySym[ps.Symbol] = &perpstate.Position{
			UserID: ps.UserID, Symbol: ps.Symbol, Side: perpstate.Side(ps.Side),
			Size: dec.New(ps.Size), Entry: dec.New(ps.Entry), Margin: dec.New(ps.Margin),
			Leverage: dec.New(ps.Leverage), Realized: dec.New(ps.Realized),
			Mode: perpstate.MarginMode(ps.Mode), LastMatchSeq: ps.LastMatchSeq,
			FundingRoundSeen: ps.FundingRoundSeen, Version: ps.Version,
		}
	}
	for sym, v := range s.Marks {
		e.marks[sym] = dec.New(v)
	}
	for sym, v := range s.Insurance {
		e.insurance[sym] = dec.New(v)
	}
}
