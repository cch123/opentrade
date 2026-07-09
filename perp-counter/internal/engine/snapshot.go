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
	// Keys are the operation-qualified strings produced by
	// transferOperationKey. Restore also accepts legacy raw transfer_id keys,
	// so the on-disk JSON shape stays backward compatible.
	Transfers map[string]TransferSnap `json:"transfers"`

	// ADR-0074 state. Limits are the admin leverage caps; Ops is the config
	// op idempotency cache (client_op_id → first outcome) — both must
	// survive restart or a replayed op could double-apply (same rule as
	// Transfers / ADR-0048 "all recovery watermarks persist").
	Limits []LimitSnap       `json:"limits,omitempty"`
	Ops    map[string]OpSnap `json:"ops,omitempty"`

	// ADR-0077 per-(user, symbol) position modes. Only HEDGE rows are
	// emitted — absent means ONE_WAY, so pre-hedge snapshots restore
	// unchanged.
	PosModes []PosModeSnap `json:"pos_modes,omitempty"`

	// ADR-0079 fee state: the platform fee account's net balance per settle
	// asset and the per-user fee override rows. Both must survive restart —
	// the platform balance is the system-side counter-account, and a lost
	// override would re-pin future orders at symbol rates.
	PlatformFee  map[string]string `json:"platform_fee,omitempty"`
	FeeOverrides []FeeOverrideSnap `json:"fee_overrides,omitempty"`
}

// FeeOverrideSnap is one (user, symbol) fee override row (ADR-0079 §1).
type FeeOverrideSnap struct {
	UserID    uint64 `json:"user_id"`
	Symbol    string `json:"symbol"` // "" = user-global
	RuleID    string `json:"rule_id"`
	MakerRate string `json:"maker_rate"`
	TakerRate string `json:"taker_rate"`
	Reason    string `json:"reason,omitempty"`
	UpdatedBy string `json:"updated_by,omitempty"`
	UpdatedMs int64  `json:"updated_ms,omitempty"`
}

// PosModeSnap is one (user, symbol) position-mode row (ADR-0077 §1).
type PosModeSnap struct {
	UserID uint64 `json:"user_id"`
	Symbol string `json:"symbol"`
	Mode   uint8  `json:"mode"`
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
	Accepted    bool          `json:"accepted"`
	Reason      string        `json:"reason,omitempty"`
	Mode        uint8         `json:"mode,omitempty"`
	PosMode     uint8         `json:"pos_mode,omitempty"` // ADR-0077 position mode
	Leverage    string        `json:"leverage"`
	RiskID      uint32        `json:"risk_id,omitempty"`
	MarginAfter string        `json:"margin_after"`
	FreeAfter   string        `json:"free_after"`
	Moved       string        `json:"moved"`
	Version     uint64        `json:"version,omitempty"`
	LegMoves    []LegMoveSnap `json:"leg_moves,omitempty"` // ADR-0077 multi-leg op detail
}

// LegMoveSnap is one leg's cash movement inside a cached multi-leg op.
type LegMoveSnap struct {
	PositionIdx  uint8  `json:"position_idx"`
	Moved        string `json:"moved"`
	MarginBefore string `json:"margin_before"`
	MarginAfter  string `json:"margin_after"`
	Version      uint64 `json:"version,omitempty"`
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

// PositionSnap is one (user, symbol, idx) leg with its recovery watermarks.
// PositionIdx omitted = 0 (the one-way net record), so pre-hedge snapshots
// restore unchanged (ADR-0077).
type PositionSnap struct {
	UserID            uint64 `json:"user_id"`
	Symbol            string `json:"symbol"`
	PositionIdx       uint8  `json:"position_idx,omitempty"`
	Side              uint8  `json:"side"`
	Size              string `json:"size"`
	Entry             string `json:"entry"`
	Margin            string `json:"margin"`
	Leverage          string `json:"leverage"`
	Realized          string `json:"realized"`
	Mode              uint8  `json:"mode"`
	RiskID            uint32 `json:"risk_id,omitempty"`
	RiskConfigVersion uint64 `json:"risk_config_version,omitempty"` // ADR-0075 §3 staged pin
	AutoAddMargin     bool   `json:"auto_add_margin,omitempty"`
	AutoAddMax        string `json:"auto_add_max,omitempty"`
	LastMatchSeq      uint64 `json:"last_match_seq"`
	LastAdlRound      uint64 `json:"last_adl_round"`
	FundingRoundSeen  int64  `json:"funding_round_seen"`
	Version           uint64 `json:"version"`
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
			sp := bySym[sym]
			if sp.mode != perpstate.PositionOneWay {
				s.PosModes = append(s.PosModes, PosModeSnap{UserID: u, Symbol: sym, Mode: uint8(sp.mode)})
			}
			for _, p := range sp.liveLegs(nil) {
				s.Positions = append(s.Positions, PositionSnap{
					UserID: p.UserID, Symbol: p.Symbol, PositionIdx: p.PositionIdx, Side: uint8(p.Side),
					Size: p.Size.String(), Entry: p.Entry.String(), Margin: p.Margin.String(),
					Leverage: p.Leverage.String(), Realized: p.Realized.String(), Mode: uint8(p.Mode),
					RiskID: p.RiskID, RiskConfigVersion: p.RiskConfigVersion,
					AutoAddMargin: p.AutoAddMargin, AutoAddMax: p.AutoAddMax.String(),
					LastMatchSeq: p.LastMatchSeq, LastAdlRound: p.LastAdlRound,
					FundingRoundSeen: p.FundingRoundSeen, Version: p.Version,
				})
			}
		}
	}

	for sym, m := range e.marks {
		s.Marks[sym] = m.String()
	}
	for sym, f := range e.insurance {
		s.Insurance[sym] = f.String()
	}
	if len(e.platformFee) > 0 {
		s.PlatformFee = make(map[string]string, len(e.platformFee))
		for asset, v := range e.platformFee {
			s.PlatformFee[asset] = v.String()
		}
	}
	for _, row := range e.customerFeeOverridesLocked() {
		s.FeeOverrides = append(s.FeeOverrides, FeeOverrideSnap{
			UserID: row.UserID, Symbol: row.Symbol, RuleID: row.RuleID,
			MakerRate: row.MakerRate.String(), TakerRate: row.TakerRate.String(),
			Reason: row.Reason, UpdatedBy: row.UpdatedBy, UpdatedMs: row.UpdatedMs,
		})
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
			snap := OpSnap{
				Accepted: o.Accepted, Reason: o.Reason, Mode: uint8(o.Mode), PosMode: uint8(o.PosMode),
				Leverage: o.Leverage.String(), RiskID: o.RiskID,
				MarginAfter: o.MarginAfter.String(), FreeAfter: o.FreeAfter.String(),
				Moved: o.Moved.String(), Version: o.Version,
			}
			for _, lm := range o.LegMoves {
				snap.LegMoves = append(snap.LegMoves, LegMoveSnap{
					PositionIdx: lm.PositionIdx, Moved: lm.Moved.String(),
					MarginBefore: lm.MarginBefore.String(), MarginAfter: lm.MarginAfter.String(),
					Version: lm.Version,
				})
			}
			s.Ops[id] = snap
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
	e.positions = map[uint64]map[string]*symbolPositions{}
	e.marks = map[string]dec.Decimal{}
	e.insurance = map[string]dec.Decimal{}
	e.transfers = map[string]TransferOutcome{}
	e.platformFee = map[string]dec.Decimal{}
	e.feeOverrides = map[uint64]map[string]FeeOverride{}

	for _, w := range s.Wallets {
		e.wallets[w.UserID] = &Wallet{Available: dec.New(w.Available), Reserved: dec.New(w.Reserved),
			CrossReserved: snapDec(w.CrossReserved)}
	}
	for _, ps := range s.Positions {
		sp := e.symLocked(ps.UserID, ps.Symbol)
		sp.legs[ps.PositionIdx] = &perpstate.Position{
			UserID: ps.UserID, Symbol: ps.Symbol, PositionIdx: ps.PositionIdx,
			Side: perpstate.Side(ps.Side),
			Size: dec.New(ps.Size), Entry: dec.New(ps.Entry), Margin: dec.New(ps.Margin),
			Leverage: dec.New(ps.Leverage), Realized: dec.New(ps.Realized),
			Mode:   perpstate.MarginMode(ps.Mode),
			RiskID: ps.RiskID, RiskConfigVersion: ps.RiskConfigVersion,
			AutoAddMargin: ps.AutoAddMargin, AutoAddMax: snapDec(ps.AutoAddMax),
			LastMatchSeq: ps.LastMatchSeq,
			LastAdlRound: ps.LastAdlRound, FundingRoundSeen: ps.FundingRoundSeen, Version: ps.Version,
		}
	}
	for _, pm := range s.PosModes {
		e.symLocked(pm.UserID, pm.Symbol).mode = perpstate.PositionMode(pm.Mode)
	}
	for sym, v := range s.Marks {
		e.marks[sym] = dec.New(v)
	}
	for sym, v := range s.Insurance {
		e.insurance[sym] = dec.New(v)
	}
	for asset, v := range s.PlatformFee {
		e.platformFee[asset] = dec.New(v)
	}
	for _, row := range s.FeeOverrides {
		bySym := e.feeOverrides[row.UserID]
		if bySym == nil {
			bySym = map[string]FeeOverride{}
			e.feeOverrides[row.UserID] = bySym
		}
		bySym[row.Symbol] = FeeOverride{
			RuleID: row.RuleID, MakerRate: dec.New(row.MakerRate), TakerRate: dec.New(row.TakerRate),
			Reason: row.Reason, UpdatedBy: row.UpdatedBy, UpdatedMs: row.UpdatedMs,
		}
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
		out := OpOutcome{
			Accepted: o.Accepted, Reason: o.Reason, Mode: perpstate.MarginMode(o.Mode),
			PosMode:  perpstate.PositionMode(o.PosMode),
			Leverage: snapDec(o.Leverage), RiskID: o.RiskID,
			MarginAfter: snapDec(o.MarginAfter), FreeAfter: snapDec(o.FreeAfter),
			Moved: snapDec(o.Moved), Version: o.Version,
		}
		for _, lm := range o.LegMoves {
			out.LegMoves = append(out.LegMoves, LegMove{
				PositionIdx: lm.PositionIdx, Moved: snapDec(lm.Moved),
				MarginBefore: snapDec(lm.MarginBefore), MarginAfter: snapDec(lm.MarginAfter),
				Version: lm.Version,
			})
		}
		e.ops[id] = out
	}
	// ADR-0072 keeps the liq-price index out of snapshots because it is a
	// materialized view over positions. Rebuilding here keeps restore atomic:
	// once the lock is released, readers see positions and their index together.
	e.rebuildLiquidationIndexLocked()
}
