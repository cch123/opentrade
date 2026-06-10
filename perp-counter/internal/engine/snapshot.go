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
	Wallets   []WalletSnap            `json:"wallets"`
	Positions []PositionSnap          `json:"positions"`
	Marks     map[string]string       `json:"marks"`
	Insurance map[string]string       `json:"insurance"`
	Transfers map[string]TransferSnap `json:"transfers"` // transfer_id → cached outcome (AssetHolder idempotency)

	// ADR-0074 state. Limits are the admin leverage caps; Ops is the config
	// op idempotency cache (client_op_id → first outcome) — both must
	// survive restart or a replayed op could double-apply (same rule as
	// Transfers / ADR-0048 "all recovery watermarks persist").
	Limits []LimitSnap       `json:"limits,omitempty"`
	Ops    map[string]OpSnap `json:"ops,omitempty"`
}

// LimitSnap is one customer leverage cap row (ADR-0074 §10).
type LimitSnap struct {
	UserID      uint64 `json:"user_id"`
	Symbol      string `json:"symbol"` // "" = user-global
	MaxLeverage string `json:"max_leverage"`
	Reason      string `json:"reason,omitempty"`
	UpdatedBy   string `json:"updated_by,omitempty"`
	UpdatedMs   int64  `json:"updated_ms,omitempty"`
}

// OpSnap is one cached config-op outcome (ADR-0074 client_op_id idempotency).
type OpSnap struct {
	Accepted    bool   `json:"accepted"`
	Reason      string `json:"reason,omitempty"`
	Mode        uint8  `json:"mode,omitempty"`
	Leverage    string `json:"leverage"`
	RiskID      uint32 `json:"risk_id,omitempty"`
	MarginAfter string `json:"margin_after"`
	FreeAfter   string `json:"free_after"`
	Moved       string `json:"moved"`
	Version     uint64 `json:"version,omitempty"`
}

// TransferSnap is one cached transfer outcome (AssetHolder dedup, ADR-0057).
type TransferSnap struct {
	Status         uint8  `json:"status"`
	AvailableAfter string `json:"available_after"`
	ReservedAfter  string `json:"reserved_after"`
	RejectReason   string `json:"reject_reason"`
}

// WalletSnap is one user's margin ledger buckets (ADR-0074 §2).
type WalletSnap struct {
	UserID        uint64 `json:"user_id"`
	Available     string `json:"available"`
	Reserved      string `json:"reserved"`
	CrossReserved string `json:"cross_reserved"`
}

// PositionSnap is one (user, symbol) position with its recovery watermarks.
type PositionSnap struct {
	UserID           uint64 `json:"user_id"`
	Symbol           string `json:"symbol"`
	Side             uint8  `json:"side"`
	Size             string `json:"size"`
	Entry            string `json:"entry"`
	Margin           string `json:"margin"`
	Leverage         string `json:"leverage"`
	Realized         string `json:"realized"`
	Mode             uint8  `json:"mode"`
	RiskID           uint32 `json:"risk_id,omitempty"`
	AutoAddMargin    bool   `json:"auto_add_margin,omitempty"`
	AutoAddMax       string `json:"auto_add_max,omitempty"`
	LastMatchSeq     uint64 `json:"last_match_seq"`
	LastAdlRound     uint64 `json:"last_adl_round"`
	FundingRoundSeen int64  `json:"funding_round_seen"`
	Version          uint64 `json:"version"`
}

// Snapshot captures current state. Output is deterministic (sorted) so
// round-trips and golden comparisons are stable.
func (e *Engine) Snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s := Snapshot{Marks: map[string]string{}, Insurance: map[string]string{}, Transfers: map[string]TransferSnap{}}

	users := make([]uint64, 0, len(e.wallets))
	for u := range e.wallets {
		users = append(users, u)
	}
	sort.Slice(users, func(i, j int) bool { return users[i] < users[j] })
	for _, u := range users {
		w := e.wallets[u]
		s.Wallets = append(s.Wallets, WalletSnap{
			UserID: u, Available: w.Available.String(), Reserved: w.Reserved.String(),
			CrossReserved: w.CrossReserved.String(),
		})
	}

	posUsers := make([]uint64, 0, len(e.positions))
	for u := range e.positions {
		posUsers = append(posUsers, u)
	}
	sort.Slice(posUsers, func(i, j int) bool { return posUsers[i] < posUsers[j] })
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
				RiskID: p.RiskID, AutoAddMargin: p.AutoAddMargin, AutoAddMax: p.AutoAddMax.String(),
				LastMatchSeq: p.LastMatchSeq, LastAdlRound: p.LastAdlRound,
				FundingRoundSeen: p.FundingRoundSeen, Version: p.Version,
			})
		}
	}

	for sym, m := range e.marks {
		s.Marks[sym] = m.String()
	}
	for sym, f := range e.insurance {
		s.Insurance[sym] = f.String()
	}
	for id, o := range e.transfers {
		s.Transfers[id] = TransferSnap{
			Status: uint8(o.Status), AvailableAfter: o.AvailableAfter.String(),
			ReservedAfter: o.ReservedAfter.String(), RejectReason: o.RejectReason,
		}
	}
	for _, row := range e.customerLeverageLimitsLocked() {
		s.Limits = append(s.Limits, LimitSnap{
			UserID: row.UserID, Symbol: row.Symbol, MaxLeverage: row.MaxLeverage.String(),
			Reason: row.Reason, UpdatedBy: row.UpdatedBy, UpdatedMs: row.UpdatedMs,
		})
	}
	if len(e.ops) > 0 {
		s.Ops = make(map[string]OpSnap, len(e.ops))
		for id, o := range e.ops {
			s.Ops[id] = OpSnap{
				Accepted: o.Accepted, Reason: o.Reason, Mode: uint8(o.Mode),
				Leverage: o.Leverage.String(), RiskID: o.RiskID,
				MarginAfter: o.MarginAfter.String(), FreeAfter: o.FreeAfter.String(),
				Moved: o.Moved.String(), Version: o.Version,
			}
		}
	}
	return s
}

// snapDec parses a snapshot decimal, treating "" (a field absent from an
// older snapshot) as zero.
func snapDec(v string) dec.Decimal {
	if v == "" {
		return zero
	}
	return dec.New(v)
}

// Restore replaces engine state with s. Used at startup after loading the
// snapshot blob (and, in the full pipeline, before seeking Kafka to the
// bound offsets).
func (e *Engine) Restore(s Snapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.wallets = map[uint64]*Wallet{}
	e.positions = map[uint64]map[string]*perpstate.Position{}
	e.marks = map[string]dec.Decimal{}
	e.insurance = map[string]dec.Decimal{}
	e.transfers = map[string]TransferOutcome{}

	for _, w := range s.Wallets {
		e.wallets[w.UserID] = &Wallet{Available: dec.New(w.Available), Reserved: dec.New(w.Reserved),
			CrossReserved: snapDec(w.CrossReserved)}
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
			Mode:   perpstate.MarginMode(ps.Mode),
			RiskID: ps.RiskID, AutoAddMargin: ps.AutoAddMargin, AutoAddMax: snapDec(ps.AutoAddMax),
			LastMatchSeq: ps.LastMatchSeq,
			LastAdlRound: ps.LastAdlRound, FundingRoundSeen: ps.FundingRoundSeen, Version: ps.Version,
		}
	}
	for sym, v := range s.Marks {
		e.marks[sym] = dec.New(v)
	}
	for sym, v := range s.Insurance {
		e.insurance[sym] = dec.New(v)
	}
	for id, ts := range s.Transfers {
		e.transfers[id] = TransferOutcome{
			Status: TransferStatus(ts.Status), AvailableAfter: dec.New(ts.AvailableAfter),
			ReservedAfter: dec.New(ts.ReservedAfter), RejectReason: ts.RejectReason,
		}
	}
	e.levLimits = map[uint64]map[string]CustomerLimit{}
	for _, row := range s.Limits {
		bySym := e.levLimits[row.UserID]
		if bySym == nil {
			bySym = map[string]CustomerLimit{}
			e.levLimits[row.UserID] = bySym
		}
		bySym[row.Symbol] = CustomerLimit{MaxLeverage: dec.New(row.MaxLeverage),
			Reason: row.Reason, UpdatedBy: row.UpdatedBy, UpdatedMs: row.UpdatedMs}
	}
	e.ops = map[string]OpOutcome{}
	for id, o := range s.Ops {
		e.ops[id] = OpOutcome{
			Accepted: o.Accepted, Reason: o.Reason, Mode: perpstate.MarginMode(o.Mode),
			Leverage: snapDec(o.Leverage), RiskID: o.RiskID,
			MarginAfter: snapDec(o.MarginAfter), FreeAfter: snapDec(o.FreeAfter),
			Moved: snapDec(o.Moved), Version: o.Version,
		}
	}
	// ADR-0072 keeps the liq-price index out of snapshots because it is a
	// materialized view over positions. Rebuilding here keeps restore atomic:
	// once the lock is released, readers see positions and their index together.
	e.rebuildLiquidationIndexLocked()
}
