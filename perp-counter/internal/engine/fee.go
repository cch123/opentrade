package engine

// fee.go is the ADR-0079 trade-fee accounting state and the per-fill fee
// application step. Three pieces live here:
//
//   - per-user fee overrides (admin plane; mirrors the ADR-0074 §10 customer
//     leverage caps: symbol "" is the user-global row, a symbol row overrides
//     it),
//   - the platform fee account: ONE net balance per settle asset (collected
//     fees minus paid rebates), same in-memory + snapshot pattern as the
//     insurance cache. The gross split is reconstructed downstream from the
//     signed fee on each settlement row; the audit invariant is
//     platform_fee[asset] == SUM(fee - fee_deficit) over perp_settlements,
//   - applyFeeLocked: moves a fill's fee between the user wallet, the order's
//     fee reservation, and the platform account inside the SAME critical
//     section as the position mutation and cash routing (no TOCTOU between
//     settling a fill and charging its fee).
//
// Determinism contract (ADR-0079 §1/§5): everything here is a pure function
// of (wallet state, FeeCharge) — rates were pinned at order admission, and a
// rebate is paid unconditionally (no shared-budget gate whose outcome would
// depend on cross-user processing order during replay).

import (
	"sort"

	"github.com/xargin/opentrade/pkg/dec"
)

// FeeOverride is one per-user fee rate row (ADR-0079 §1 admin plane).
type FeeOverride struct {
	RuleID    string
	MakerRate dec.Decimal // signed; < 0 = rebate
	TakerRate dec.Decimal // >= 0
	Reason    string
	UpdatedBy string
	UpdatedMs int64
}

// FeeOverrideRow is a listed override with its scope key (RPC / snapshot).
type FeeOverrideRow struct {
	UserID uint64
	Symbol string // "" = user-global
	FeeOverride
}

// FeeCharge is the fee the service computed for one fill from the order's
// pinned rates (ADR-0079 §1). Zero value = no fee (legacy mode / replays).
type FeeCharge struct {
	Amount dec.Decimal // signed: > 0 the user pays, < 0 rebate to the user
	Asset  string      // settle asset; "" only when Amount is zero
	// FromReserve caps how much of this charge may be drawn from the order's
	// remaining fee reservation (Order.ReservedFee). The buffer sits in the
	// wallet bucket selected by Cross.
	FromReserve dec.Decimal
	Cross       bool // order margin mode: reserve bucket + deficit clamp policy
}

// FeeOutcome reports how a FeeCharge actually routed (journal input).
type FeeOutcome struct {
	Collected   dec.Decimal // credited to platform_fee (= Amount - Deficit for a positive charge)
	Rebated     dec.Decimal // paid to the user (= -Amount for a negative charge)
	ReserveUsed dec.Decimal // portion drawn from the order's fee reservation
	Deficit     dec.Decimal // uncollectable portion (isolated wallet clamped at zero)
	WalletAfter dec.Decimal // Available after the fee movement
}

// applyFeeLocked routes one fill's fee. Caller holds e.mu and runs inside the
// owning user's sequencer.
func (e *Engine) applyFeeLocked(user uint64, fee FeeCharge) FeeOutcome {
	w := e.walletLocked(user)
	out := FeeOutcome{Collected: zero, Rebated: zero, ReserveUsed: zero, Deficit: zero}
	switch {
	case fee.Amount.Sign() > 0:
		remaining := fee.Amount
		// 1) Consume the order's fee reservation first: the buffer already
		// left Available at PlaceOrder, so drawing it moves money straight
		// from the hold bucket to the platform account.
		bucket := &w.Reserved
		if fee.Cross {
			bucket = &w.CrossReserved
		}
		take := dec.Min(dec.Min(remaining, fee.FromReserve), *bucket)
		if take.Sign() > 0 {
			*bucket = bucket.Sub(take)
			out.ReserveUsed = take
			remaining = remaining.Sub(take)
		}
		// 2) Remainder from Available. Isolated clamps at zero — the fee must
		// not dig the wallet negative; the shortfall is fee_deficit (ADR-0079
		// §4, gap scenarios only). Cross settles like realized PnL against
		// the pool's cash component (pool liquidation guards solvency), so no
		// clamp.
		if remaining.Sign() > 0 {
			pay := remaining
			if !fee.Cross {
				pay = dec.Min(remaining, dec.Max(w.Available, zero))
			}
			w.Available = w.Available.Sub(pay)
			out.Deficit = remaining.Sub(pay)
		}
		out.Collected = fee.Amount.Sub(out.Deficit)
		e.platformFee[fee.Asset] = e.platformFee[fee.Asset].Add(out.Collected)
	case fee.Amount.Sign() < 0:
		// Rebate: paid unconditionally (ADR-0079 §5). platform_fee may go
		// negative — it is a liability record, not a payment gate; quota
		// enforcement is a config/monitoring-plane action.
		r := fee.Amount.Neg()
		w.Available = w.Available.Add(r)
		out.Rebated = r
		e.platformFee[fee.Asset] = e.platformFee[fee.Asset].Sub(r)
	}
	out.WalletAfter = w.Available
	return out
}

// PlatformFee returns the platform fee account's net balance for asset.
func (e *Engine) PlatformFee(asset string) dec.Decimal {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.platformFee[asset]
}

// SetCustomerFeeOverride installs (or, with an empty RuleID, removes) the
// (user, symbol) fee override. symbol "" is the user-global row. Validation
// is the service's job; the engine stores what it is given.
func (e *Engine) SetCustomerFeeOverride(user uint64, symbol string, ov FeeOverride) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySym := e.feeOverrides[user]
	if ov.RuleID == "" {
		if bySym != nil {
			delete(bySym, symbol)
			if len(bySym) == 0 {
				delete(e.feeOverrides, user)
			}
		}
		return
	}
	if bySym == nil {
		bySym = map[string]FeeOverride{}
		e.feeOverrides[user] = bySym
	}
	bySym[symbol] = ov
}

// CustomerFeeOverride resolves the override governing (user, symbol): the
// symbol row wins over the user-global row. ok=false → no override, the
// caller falls back to SymbolConfig.FeeParams (ADR-0079 §1 precedence).
func (e *Engine) CustomerFeeOverride(user uint64, symbol string) (FeeOverride, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	bySym := e.feeOverrides[user]
	if bySym == nil {
		return FeeOverride{}, false
	}
	if ov, ok := bySym[symbol]; ok {
		return ov, true
	}
	ov, ok := bySym[""]
	return ov, ok
}

// CustomerFeeOverrides lists override rows — user 0 = all users. Sorted for
// stable RPC/snapshot output.
func (e *Engine) CustomerFeeOverrides(user uint64) []FeeOverrideRow {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if user != 0 {
		return sortFeeRows(e.feeRowsForLocked(user, nil))
	}
	return sortFeeRows(e.customerFeeOverridesLocked())
}

// customerFeeOverridesLocked collects every override row. Caller holds e.mu
// (any mode) — the locked entry point Snapshot() composes with (the
// self-locking method is the convenience wrapper, not the only door).
func (e *Engine) customerFeeOverridesLocked() []FeeOverrideRow {
	var out []FeeOverrideRow
	for u := range e.feeOverrides {
		out = e.feeRowsForLocked(u, out)
	}
	return sortFeeRows(out)
}

func (e *Engine) feeRowsForLocked(user uint64, out []FeeOverrideRow) []FeeOverrideRow {
	for sym, ov := range e.feeOverrides[user] {
		out = append(out, FeeOverrideRow{UserID: user, Symbol: sym, FeeOverride: ov})
	}
	return out
}

func sortFeeRows(rows []FeeOverrideRow) []FeeOverrideRow {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].UserID != rows[j].UserID {
			return rows[i].UserID < rows[j].UserID
		}
		return rows[i].Symbol < rows[j].Symbol
	})
	return rows
}
