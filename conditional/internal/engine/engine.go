// Package engine is the core of the conditional-order service. It holds
// pending conditionals in memory, scans them against incoming PublicTrade
// prices, and asks an OrderPlacer (Counter gRPC wrapper) to emit the real
// order once a trigger fires.
//
// Safety model:
//
//   - All state is guarded by a single mutex. Per-symbol partitioning would
//     be a later optimization; MVP is single-threaded per engine.
//   - External calls (Counter.PlaceOrder) happen OUTSIDE the mutex. The
//     engine snapshots a conditional under the lock, releases it, makes the
//     RPC, then re-acquires to commit the outcome — so a slow Counter
//     can't block cancels or other triggers.
//   - Counter dedup keeps us safe against duplicate fires: we derive
//     client_order_id = "cond-<id>" deterministically. If the process
//     crashes between "picked up for firing" and "state transitioned", the
//     next run will replay, Counter will return accepted=false + the same
//     order_id, and we'll commit the same outcome.
//
// Funds are NOT reserved at Place time (ADR-0040 §Implementation). The
// inner order may fail with INSUFFICIENT_BALANCE; we surface that as
// REJECTED and expose reject_reason.
package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/etcdcfg"
)

// -----------------------------------------------------------------------------
// Errors surfaced at the gRPC boundary.
// -----------------------------------------------------------------------------

var (
	ErrMissingUserID       = errors.New("conditional: user_id required")
	ErrMissingSymbol       = errors.New("conditional: symbol required")
	ErrInvalidType         = errors.New("conditional: invalid type")
	ErrInvalidSide         = errors.New("conditional: invalid side")
	ErrInvalidStopPrice    = errors.New("conditional: stop_price must be > 0")
	ErrLimitPriceRequired  = errors.New("conditional: limit_price required for *_LIMIT variant")
	ErrLimitPriceForbidden = errors.New("conditional: limit_price not allowed for MARKET variant")
	ErrQtyRequired         = errors.New("conditional: qty required")
	ErrQuoteQtyShape       = errors.New("conditional: quote_qty only allowed for MARKET buy")
	ErrBothQtyAndQuoteQty  = errors.New("conditional: provide either qty or quote_qty for market buy, not both")
	ErrExpiryInPast        = errors.New("conditional: expires_at_unix_ms must be in the future")
	ErrTrailingDeltaNeeded    = errors.New("conditional: trailing_delta_bps required for TRAILING_STOP_LOSS")
	ErrTrailingDeltaForbidden = errors.New("conditional: trailing_delta_bps only allowed for TRAILING_STOP_LOSS")
	ErrTrailingDeltaRange     = errors.New("conditional: trailing_delta_bps must be in (0, 10000]")
	ErrActivationPriceShape   = errors.New("conditional: activation_price only allowed for TRAILING_STOP_LOSS")
	ErrStopPriceForbidden     = errors.New("conditional: stop_price not used by TRAILING_STOP_LOSS (derived from watermark)")
	ErrOCONeedsTwoLegs     = errors.New("conditional: OCO request needs at least two legs")
	ErrOCOSymbolMismatch   = errors.New("conditional: OCO legs must share the same symbol")
	ErrOCOSideMismatch     = errors.New("conditional: OCO legs must share the same side")
	ErrOCOUserMismatch     = errors.New("conditional: OCO legs must share the same user_id")
	ErrNotFound            = errors.New("conditional: not found")
	ErrNotOwner            = errors.New("conditional: user does not own this conditional")
	ErrNotActive           = errors.New("conditional: already terminal")
)

// -----------------------------------------------------------------------------
// IDGen + Placer interfaces
// -----------------------------------------------------------------------------

// IDGen generates monotonic conditional ids (snowflake).
type IDGen interface {
	Next() uint64
}

// OrderPlacer is the narrow gRPC contract the engine needs from Counter.
// When Reservations is non-nil (default in prod) the engine calls Reserve
// at Place time, ReleaseReservation on cancel / reject, and PlaceOrder
// with the reservation_id at trigger time (ADR-0041). A nil Reservations
// falls back to MVP-14a behaviour: no fund reservation, PlaceOrder may
// fail at trigger if balance is gone.
type OrderPlacer interface {
	PlaceOrder(ctx context.Context, in *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error)
}

// Reservations is the optional set of reservation operations. Split from
// OrderPlacer so tests can exercise the MVP-14a codepath without a stub
// for these methods.
type Reservations interface {
	Reserve(ctx context.Context, in *counterrpc.ReserveRequest) (*counterrpc.ReserveResponse, error)
	ReleaseReservation(ctx context.Context, in *counterrpc.ReleaseReservationRequest) (*counterrpc.ReleaseReservationResponse, error)
}

// -----------------------------------------------------------------------------
// Conditional record (in-memory twin of the proto wire form).
// -----------------------------------------------------------------------------

// Conditional holds the parsed numeric state for speed. External surfaces
// (ToProto) re-stringify on the way out.
type Conditional struct {
	ID             uint64
	ClientCondID   string
	UserID         string
	Symbol         string
	Side           eventpb.Side
	Type           condrpc.ConditionalType
	StopPrice      dec.Decimal
	LimitPrice     dec.Decimal
	Qty            dec.Decimal
	QuoteQty       dec.Decimal
	TIF            eventpb.TimeInForce
	Status         condrpc.ConditionalStatus
	CreatedAtMs    int64
	TriggeredAtMs  int64
	PlacedOrderID  uint64
	RejectReason   string
	// ExpiresAtMs, when > 0, is the absolute wall-clock ms at which a
	// PENDING conditional flips to EXPIRED via SweepExpired (ADR-0043).
	ExpiresAtMs int64
	// OCOGroupID ties this leg to its siblings. Non-empty = when any leg
	// hits a terminal status, all still-PENDING siblings cascade to
	// CANCELED (ADR-0044). Empty = standalone conditional.
	OCOGroupID string
	// Trailing-stop fields (ADR-0045). Populated only when Type is
	// CONDITIONAL_TYPE_TRAILING_STOP_LOSS. The watermark is the current
	// high (sell) or low (buy) observed since activation; effective stop
	// is watermark ± (watermark × TrailingDeltaBps / 10000).
	TrailingDeltaBps  int32
	ActivationPrice   dec.Decimal
	TrailingWatermark dec.Decimal
	TrailingActive    bool
}

// -----------------------------------------------------------------------------
// Config + Engine struct
// -----------------------------------------------------------------------------

// Config configures the Engine.
type Config struct {
	// TerminalHistoryLimit bounds how many terminal (triggered/canceled/
	// rejected) records we keep for ListConditionals(include_inactive).
	// Older entries are dropped in FIFO order. 0 disables history.
	TerminalHistoryLimit int
	// Clock overrides time.Now — tests only.
	Clock func() time.Time
	// DefaultMaxActiveConditionalOrders is the ADR-0054 fallback cap when a
	// symbol's SymbolConfig.MaxActiveConditionalOrders is zero (or
	// SymbolLookup is nil). Zero here = compatibility mode (cap disabled).
	// Production defaults to 10 (per ADR); tests leave zero to skip.
	DefaultMaxActiveConditionalOrders uint32
	// SymbolLookup, when non-nil, returns per-symbol config for the cap
	// above (and future tunables). Wired by main after the etcd watcher
	// starts; nil = compatibility mode.
	SymbolLookup SymbolLookup
}

// SymbolLookup returns the SymbolConfig for a symbol, or ok=false if
// unknown. Mirrors counter/internal/service.SymbolLookup.
type SymbolLookup func(symbol string) (etcdcfg.SymbolConfig, bool)

// ErrMaxActiveConditionalOrdersExceeded is returned by Place / PlaceOCO
// when the caller would exceed the per-(user, symbol) untriggered
// conditional cap (ADR-0054).
var ErrMaxActiveConditionalOrdersExceeded = errors.New("conditional: max active conditional orders exceeded")

// JournalSink receives a post-change clone of a Conditional after every
// state transition (PENDING / TRIGGERED / CANCELED / REJECTED / EXPIRED).
// Implementations MUST NOT block — the engine calls Emit on its hot path.
// Nil (the default) means journaling is disabled, which is the MVP-14 /
// test behaviour.
type JournalSink interface {
	Emit(c *Conditional)
}

// Engine owns the pending / terminal maps and coordinates triggers.
type Engine struct {
	cfg      Config
	idgen    IDGen
	placer   OrderPlacer
	reserver Reservations // may be nil → MVP-14a behaviour
	logger   *zap.Logger

	mu         sync.Mutex
	pending    map[uint64]*Conditional
	terminals  map[uint64]*Conditional
	termOrder  []uint64 // FIFO of terminal ids for trim
	byClient   map[string]uint64
	ocoByClient map[string]string // client_oco_id → oco_group_id (ADR-0044)
	lastPrice  map[string]dec.Decimal
	offsets    map[int32]int64
	// activeConditionals counts pending conditionals per (user, symbol) for
	// the ADR-0054 slot cap. Derived index, rebuilt from pending on
	// Restore — not persisted.
	activeConditionals map[string]map[string]int

	journal JournalSink // may be nil; set via SetJournal at main wiring time (ADR-0047)
}

// SetJournal installs (or clears) the journal sink. Safe to call before
// the engine is under traffic; callers wire this in main after the Kafka
// producer comes up.
func (e *Engine) SetJournal(j JournalSink) {
	e.mu.Lock()
	e.journal = j
	e.mu.Unlock()
}

// emitSnapshots fires the journal for a slice of already-cloned
// Conditionals. Called outside the engine lock.
func (e *Engine) emitSnapshots(snaps []Conditional) {
	if len(snaps) == 0 {
		return
	}
	// Re-read journal under the lock briefly to see the latest sink, then
	// emit without holding it.
	e.mu.Lock()
	j := e.journal
	e.mu.Unlock()
	if j == nil {
		return
	}
	for i := range snaps {
		snap := snaps[i]
		j.Emit(&snap)
	}
}

// New builds an Engine. Pass a zap.NewNop() for tests.
// reserver is optional; nil disables fund reservation (MVP-14a mode).
func New(cfg Config, idgen IDGen, placer OrderPlacer, reserver Reservations, logger *zap.Logger) *Engine {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.TerminalHistoryLimit < 0 {
		cfg.TerminalHistoryLimit = 0
	}
	return &Engine{
		cfg:                cfg,
		idgen:              idgen,
		placer:             placer,
		reserver:           reserver,
		logger:             logger,
		pending:            make(map[uint64]*Conditional),
		terminals:          make(map[uint64]*Conditional),
		byClient:           make(map[string]uint64),
		ocoByClient:        make(map[string]string),
		lastPrice:          make(map[string]dec.Decimal),
		offsets:            make(map[int32]int64),
		activeConditionals: make(map[string]map[string]int),
	}
}

// capActiveConditionalsLocked returns the per-(user, symbol) pending cap
// for ADR-0054. Callers must hold e.mu. Zero means no cap (compat mode).
func (e *Engine) capActiveConditionalsLocked(symbol string) uint32 {
	cap := e.cfg.DefaultMaxActiveConditionalOrders
	if e.cfg.SymbolLookup != nil {
		if cfg, ok := e.cfg.SymbolLookup(symbol); ok && cfg.MaxActiveConditionalOrders > 0 {
			cap = cfg.MaxActiveConditionalOrders
		}
	}
	return cap
}

// incActiveConditionalLocked / decActiveConditionalLocked maintain the
// per-(user, symbol) pending counter. Callers must hold e.mu.
func (e *Engine) incActiveConditionalLocked(userID, symbol string) {
	bySymbol, ok := e.activeConditionals[userID]
	if !ok {
		bySymbol = make(map[string]int)
		e.activeConditionals[userID] = bySymbol
	}
	bySymbol[symbol]++
}

func (e *Engine) decActiveConditionalLocked(userID, symbol string) {
	bySymbol, ok := e.activeConditionals[userID]
	if !ok {
		return
	}
	bySymbol[symbol]--
	if bySymbol[symbol] <= 0 {
		delete(bySymbol, symbol)
	}
	if len(bySymbol) == 0 {
		delete(e.activeConditionals, userID)
	}
}

// CountActiveConditionals returns the number of pending conditionals user
// currently holds on symbol. Used by tests and for admin surfacing.
func (e *Engine) CountActiveConditionals(userID, symbol string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	bySymbol, ok := e.activeConditionals[userID]
	if !ok {
		return 0
	}
	return bySymbol[symbol]
}

// -----------------------------------------------------------------------------
// Place / Cancel / Query
// -----------------------------------------------------------------------------

// Place validates req, reserves funds (if reserver wired), stores a new
// PENDING conditional, and returns its id. Duplicate
// client_conditional_id returns the existing record with accepted=false
// (idempotency per proto contract).
func (e *Engine) Place(ctx context.Context, req *condrpc.PlaceConditionalRequest) (id uint64, status condrpc.ConditionalStatus, accepted bool, err error) {
	c, err := buildConditional(req)
	if err != nil {
		return 0, 0, false, err
	}
	nowMs := e.cfg.Clock().UnixMilli()
	if c.ExpiresAtMs > 0 && c.ExpiresAtMs <= nowMs {
		return 0, 0, false, ErrExpiryInPast
	}
	c.CreatedAtMs = nowMs
	c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING

	// Fast-path dedup: return prior record without reserving anew.
	if c.ClientCondID != "" {
		e.mu.Lock()
		if existingID, ok := e.byClient[c.ClientCondID]; ok {
			if prior := e.lookupLocked(existingID); prior != nil {
				e.mu.Unlock()
				return prior.ID, prior.Status, false, nil
			}
		}
		e.mu.Unlock()
	}

	// Allocate the conditional id now so we can form the reservation
	// ref_id before calling Counter. idgen is independently
	// concurrency-safe; a "wasted" id on reservation failure is harmless
	// at snowflake scale.
	c.ID = e.nextID()
	refID := e.refIDFor(c.ID)

	// Reserve funds outside any engine lock. Counter's Reserve is
	// idempotent on ref_id so retries or replays after crash converge.
	if e.reserver != nil {
		if _, rerr := e.reserver.Reserve(ctx, buildReserveReq(c, refID)); rerr != nil {
			return 0, 0, false, rerr
		}
	}

	// Commit to the engine maps. Re-check dedup under the lock; if
	// another concurrent Place won the race for the same client id, we
	// orphan a reservation and must release it.
	e.mu.Lock()
	if c.ClientCondID != "" {
		if existingID, ok := e.byClient[c.ClientCondID]; ok {
			if prior := e.lookupLocked(existingID); prior != nil {
				priorID, priorStatus := prior.ID, prior.Status
				e.mu.Unlock()
				e.bestEffortRelease(ctx, c.UserID, refID)
				return priorID, priorStatus, false, nil
			}
		}
	}
	// ADR-0054 slot cap. Inside the lock so concurrent Places can't both
	// slip past a near-full bucket. We release the reservation before
	// returning to keep Counter's Frozen in sync.
	if cap := e.capActiveConditionalsLocked(c.Symbol); cap > 0 {
		n := 0
		if bySymbol := e.activeConditionals[c.UserID]; bySymbol != nil {
			n = bySymbol[c.Symbol]
		}
		if uint32(n) >= cap {
			e.mu.Unlock()
			e.bestEffortRelease(ctx, c.UserID, refID)
			return 0, 0, false, ErrMaxActiveConditionalOrdersExceeded
		}
	}
	e.pending[c.ID] = c
	e.incActiveConditionalLocked(c.UserID, c.Symbol)
	if c.ClientCondID != "" {
		e.byClient[c.ClientCondID] = c.ID
	}
	snap := *c
	e.mu.Unlock()

	e.emitSnapshots([]Conditional{snap})
	return c.ID, c.Status, true, nil
}

// OCOLegResult is one leg's outcome inside a PlaceOCO call.
type OCOLegResult struct {
	ID     uint64
	Status condrpc.ConditionalStatus
}

// PlaceOCO places ≥ 2 conditional legs atomically and ties them together
// via a common OCOGroupID. When any leg later hits a terminal status the
// still-PENDING siblings auto-cancel (ADR-0044). Group-level idempotency
// hangs off `clientOCOID`: a duplicate call with the same id returns the
// prior group's ids + accepted=false. Per-leg `client_conditional_id`s
// still dedup independently.
func (e *Engine) PlaceOCO(ctx context.Context, userID, clientOCOID string, legs []*condrpc.PlaceConditionalRequest) (groupID string, results []OCOLegResult, accepted bool, err error) {
	if len(legs) < 2 {
		return "", nil, false, ErrOCONeedsTwoLegs
	}
	parsed := make([]*Conditional, len(legs))
	nowMs := e.cfg.Clock().UnixMilli()
	for i, lreq := range legs {
		if lreq == nil {
			return "", nil, false, fmt.Errorf("conditional: OCO leg %d is nil", i)
		}
		// Force leg.user_id = outer user_id (defensive).
		if lreq.UserId == "" {
			lreq.UserId = userID
		} else if lreq.UserId != userID {
			return "", nil, false, ErrOCOUserMismatch
		}
		c, berr := buildConditional(lreq)
		if berr != nil {
			return "", nil, false, berr
		}
		if c.ExpiresAtMs > 0 && c.ExpiresAtMs <= nowMs {
			return "", nil, false, ErrExpiryInPast
		}
		c.CreatedAtMs = nowMs
		c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING
		parsed[i] = c
	}
	for i := 1; i < len(parsed); i++ {
		if parsed[i].Symbol != parsed[0].Symbol {
			return "", nil, false, ErrOCOSymbolMismatch
		}
		if parsed[i].Side != parsed[0].Side {
			return "", nil, false, ErrOCOSideMismatch
		}
	}

	// Group-level dedup (fast path).
	if clientOCOID != "" {
		e.mu.Lock()
		if gid, ok := e.ocoByClient[clientOCOID]; ok {
			legResults := e.legResultsForGroupLocked(gid)
			e.mu.Unlock()
			return gid, legResults, false, nil
		}
		e.mu.Unlock()
	}

	// Allocate ids + group id.
	for _, c := range parsed {
		c.ID = e.nextID()
	}
	groupID = "oco-" + formatUint(parsed[0].ID)
	for _, c := range parsed {
		c.OCOGroupID = groupID
	}

	// Reserve each outside the engine lock; roll back on error.
	var reserved []*Conditional
	if e.reserver != nil {
		for _, c := range parsed {
			refID := e.refIDFor(c.ID)
			if _, rerr := e.reserver.Reserve(ctx, buildReserveReq(c, refID)); rerr != nil {
				for _, r := range reserved {
					e.bestEffortRelease(ctx, r.UserID, e.refIDFor(r.ID))
				}
				return "", nil, false, rerr
			}
			reserved = append(reserved, c)
		}
	}

	// Commit.
	e.mu.Lock()
	if clientOCOID != "" {
		if gid, ok := e.ocoByClient[clientOCOID]; ok {
			// Lost the race: another caller committed the same clientOCOID
			// between the fast-path dedup and here. Roll back our reservations.
			legResults := e.legResultsForGroupLocked(gid)
			e.mu.Unlock()
			for _, r := range reserved {
				e.bestEffortRelease(ctx, r.UserID, e.refIDFor(r.ID))
			}
			return gid, legResults, false, nil
		}
	}
	// ADR-0054 slot cap: all OCO legs share (user, symbol), so the group
	// must fit in one go. Legs are reserved → roll back all on cap miss.
	if cap := e.capActiveConditionalsLocked(parsed[0].Symbol); cap > 0 {
		n := 0
		if bySymbol := e.activeConditionals[parsed[0].UserID]; bySymbol != nil {
			n = bySymbol[parsed[0].Symbol]
		}
		if uint32(n+len(parsed)) > cap {
			e.mu.Unlock()
			for _, r := range reserved {
				e.bestEffortRelease(ctx, r.UserID, e.refIDFor(r.ID))
			}
			return "", nil, false, ErrMaxActiveConditionalOrdersExceeded
		}
	}
	for _, c := range parsed {
		e.pending[c.ID] = c
		e.incActiveConditionalLocked(c.UserID, c.Symbol)
		if c.ClientCondID != "" {
			e.byClient[c.ClientCondID] = c.ID
		}
	}
	if clientOCOID != "" {
		e.ocoByClient[clientOCOID] = groupID
	}
	results = make([]OCOLegResult, len(parsed))
	snaps := make([]Conditional, len(parsed))
	for i, c := range parsed {
		results[i] = OCOLegResult{ID: c.ID, Status: c.Status}
		snaps[i] = *c
	}
	e.mu.Unlock()

	e.emitSnapshots(snaps)
	return groupID, results, true, nil
}

// legResultsForGroupLocked collects the current (id, status) tuples for
// every conditional tagged with the given OCO group id. Caller holds e.mu.
func (e *Engine) legResultsForGroupLocked(groupID string) []OCOLegResult {
	var out []OCOLegResult
	for id, c := range e.pending {
		if c.OCOGroupID == groupID {
			out = append(out, OCOLegResult{ID: id, Status: c.Status})
		}
	}
	for id, c := range e.terminals {
		if c.OCOGroupID == groupID {
			out = append(out, OCOLegResult{ID: id, Status: c.Status})
		}
	}
	return out
}

// Cancel transitions a PENDING conditional to CANCELED and releases its
// reservation (if any). Returns accepted=true only when a state change
// actually happened.
func (e *Engine) Cancel(ctx context.Context, userID string, id uint64) (condrpc.ConditionalStatus, bool, error) {
	if userID == "" {
		return 0, false, ErrMissingUserID
	}
	e.mu.Lock()
	c := e.lookupLocked(id)
	if c == nil {
		e.mu.Unlock()
		return 0, false, ErrNotFound
	}
	if c.UserID != userID {
		e.mu.Unlock()
		return 0, false, ErrNotOwner
	}
	if c.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		status := c.Status
		e.mu.Unlock()
		return status, false, nil
	}
	c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED
	c.TriggeredAtMs = e.cfg.Clock().UnixMilli()
	refID := e.refIDFor(c.ID)
	finalStatus := c.Status
	primary := *c
	e.graduateLocked(c)
	cascaded, cascadeSnaps := e.cascadeOCOCancelLocked(c, "sibling OCO leg canceled")
	e.mu.Unlock()

	e.bestEffortRelease(ctx, userID, refID)
	e.releaseAll(ctx, cascaded)
	e.emitSnapshots(append([]Conditional{primary}, cascadeSnaps...))
	return finalStatus, true, nil
}

// Get returns a clone of the stored conditional. ErrNotFound if unknown or
// owned by another user.
func (e *Engine) Get(userID string, id uint64) (*Conditional, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.lookupLocked(id)
	if c == nil {
		return nil, ErrNotFound
	}
	if c.UserID != userID {
		return nil, ErrNotOwner
	}
	clone := *c
	return &clone, nil
}

// List returns all records for a user. includeInactive=false only returns
// PENDING; true returns the full retention window.
func (e *Engine) List(userID string, includeInactive bool) []*Conditional {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]*Conditional, 0)
	for _, c := range e.pending {
		if c.UserID == userID {
			cp := *c
			out = append(out, &cp)
		}
	}
	if includeInactive {
		for _, c := range e.terminals {
			if c.UserID == userID {
				cp := *c
				out = append(out, &cp)
			}
		}
	}
	return out
}

// -----------------------------------------------------------------------------
// Market-data ingress
// -----------------------------------------------------------------------------

// HandleRecord is called by the market-data consumer for every record. It
// advances the saved offset under the same mutex as state mutation (so a
// Capture sees consistent state+offset), extracts PublicTrade payloads,
// collects all pending conditionals that cross their trigger threshold at
// the new price, then releases the lock and issues Counter PlaceOrder
// calls for each.
func (e *Engine) HandleRecord(ctx context.Context, evt *eventpb.MarketDataEvent, partition int32, offset int64) {
	tofire := e.handleLocked(evt, partition, offset)
	if len(tofire) == 0 {
		return
	}
	triggeredAt := e.cfg.Clock().UnixMilli()
	for _, id := range tofire {
		e.tryFire(ctx, id, triggeredAt)
	}
}

func (e *Engine) handleLocked(evt *eventpb.MarketDataEvent, partition int32, offset int64) []uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.offsets[partition] = offset + 1
	pt, ok := publicTradeOf(evt)
	if !ok {
		return nil
	}
	symbol := pt.Symbol
	if symbol == "" {
		symbol = evt.Symbol
	}
	price, err := dec.Parse(pt.Price)
	if err != nil || !dec.IsPositive(price) {
		return nil
	}
	e.lastPrice[symbol] = price
	var tofire []uint64
	for _, c := range e.pending {
		if c.Symbol != symbol {
			continue
		}
		if c.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_TRAILING_STOP_LOSS {
			if e.updateTrailingLocked(c, price) {
				tofire = append(tofire, c.ID)
			}
			continue
		}
		if ShouldFire(c.Side, c.Type, price, c.StopPrice) {
			tofire = append(tofire, c.ID)
		}
	}
	return tofire
}

// updateTrailingLocked advances the watermark / activation for a trailing
// conditional and reports whether the latest price has retraced far enough
// to fire. Called once per PublicTrade under e.mu. Mutates the passed-in
// Conditional so snapshot observers see the running state.
func (e *Engine) updateTrailingLocked(c *Conditional, lastPrice dec.Decimal) bool {
	// Gate on activation.
	if !c.TrailingActive {
		if dec.IsZero(c.ActivationPrice) {
			c.TrailingActive = true
		} else {
			switch c.Side {
			case eventpb.Side_SIDE_SELL:
				if lastPrice.Cmp(c.ActivationPrice) >= 0 {
					c.TrailingActive = true
				}
			case eventpb.Side_SIDE_BUY:
				if lastPrice.Cmp(c.ActivationPrice) <= 0 {
					c.TrailingActive = true
				}
			}
		}
		if !c.TrailingActive {
			return false
		}
	}
	// Advance watermark.
	if dec.IsZero(c.TrailingWatermark) {
		c.TrailingWatermark = lastPrice
	} else {
		switch c.Side {
		case eventpb.Side_SIDE_SELL:
			if lastPrice.Cmp(c.TrailingWatermark) > 0 {
				c.TrailingWatermark = lastPrice
			}
		case eventpb.Side_SIDE_BUY:
			if lastPrice.Cmp(c.TrailingWatermark) < 0 {
				c.TrailingWatermark = lastPrice
			}
		}
	}
	// Compute retracement and fire if crossed.
	bps := dec.FromInt(int64(c.TrailingDeltaBps))
	const basis = 10_000
	delta := c.TrailingWatermark.Mul(bps).Div(dec.FromInt(basis))
	switch c.Side {
	case eventpb.Side_SIDE_SELL:
		stop := c.TrailingWatermark.Sub(delta)
		return lastPrice.Cmp(stop) <= 0
	case eventpb.Side_SIDE_BUY:
		stop := c.TrailingWatermark.Add(delta)
		return lastPrice.Cmp(stop) >= 0
	}
	return false
}

// tryFire issues the inner Counter PlaceOrder call for id and commits the
// outcome to state. If the conditional was canceled or already terminal
// by the time we reacquire the lock, the result is ignored. triggeredAtMs
// is the wall-clock ms stamp set on the record regardless of success.
// On a successful PlaceOrder, the reservation (if any) has been consumed
// atomically by Counter. On a failure we attempt a best-effort
// ReleaseReservation so a lingering reservation does not leak frozen
// balance.
func (e *Engine) tryFire(ctx context.Context, id uint64, triggeredAtMs int64) {
	e.mu.Lock()
	c := e.lookupLocked(id)
	if c == nil || c.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		e.mu.Unlock()
		return
	}
	req := buildPlaceOrderReq(c)
	refID := e.refIDFor(c.ID)
	if e.reserver != nil {
		req.ReservationId = refID
	}
	userID := c.UserID
	e.mu.Unlock()

	resp, err := e.placer.PlaceOrder(ctx, req)

	e.mu.Lock()
	c = e.lookupLocked(id)
	if c == nil || c.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		e.mu.Unlock()
		return
	}
	c.TriggeredAtMs = triggeredAtMs
	failed := false
	if err != nil {
		reason := cleanRejectReason(err)
		c.RejectReason = reason
		// ADR-0054: Counter's per-(user, symbol) MAX_OPEN_LIMIT_ORDERS
		// reject gets its own terminal status so clients (and audit tools)
		// can distinguish "user slot full" from generic rejections.
		if reason == string(etcdcfg.RejectMaxOpenLimitOrders) {
			c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_EXPIRED_IN_MATCH
		} else {
			c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_REJECTED
		}
		failed = true
		if e.logger != nil {
			e.logger.Warn("conditional trigger rejected",
				zap.Uint64("id", id),
				zap.String("user_id", c.UserID),
				zap.String("symbol", c.Symbol),
				zap.String("err", c.RejectReason))
		}
	} else {
		c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_TRIGGERED
		c.PlacedOrderID = resp.OrderId
		if e.logger != nil {
			e.logger.Info("conditional triggered",
				zap.Uint64("id", id),
				zap.Uint64("order_id", resp.OrderId))
		}
	}
	primary := *c
	e.graduateLocked(c)
	cascaded, cascadeSnaps := e.cascadeOCOCancelLocked(c, "sibling OCO leg terminated")
	e.mu.Unlock()

	if failed {
		e.bestEffortRelease(ctx, userID, refID)
	}
	e.releaseAll(ctx, cascaded)
	e.emitSnapshots(append([]Conditional{primary}, cascadeSnaps...))
}

// -----------------------------------------------------------------------------
// Persistence / snapshot hooks
// -----------------------------------------------------------------------------

// Snapshot exports everything needed to resume after restart. Safe to call
// concurrently with HandleRecord / Place / Cancel.
func (e *Engine) Snapshot() (pending []*Conditional, terminals []*Conditional, offsets map[int32]int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	pending = make([]*Conditional, 0, len(e.pending))
	for _, c := range e.pending {
		cp := *c
		pending = append(pending, &cp)
	}
	terminals = make([]*Conditional, 0, len(e.termOrder))
	for _, id := range e.termOrder {
		if c, ok := e.terminals[id]; ok {
			cp := *c
			terminals = append(terminals, &cp)
		}
	}
	offsets = make(map[int32]int64, len(e.offsets))
	for p, o := range e.offsets {
		offsets[p] = o
	}
	return pending, terminals, offsets
}

// Restore replaces in-memory state. Engine must be fresh (no prior writes)
// or callers must accept replacement semantics. ADR-0054: the
// activeConditionals index is derived from pending and rebuilt here.
func (e *Engine) Restore(pending, terminals []*Conditional, offsets map[int32]int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = make(map[uint64]*Conditional, len(pending))
	e.byClient = make(map[string]uint64, len(pending))
	e.activeConditionals = make(map[string]map[string]int)
	for _, c := range pending {
		cp := *c
		e.pending[cp.ID] = &cp
		if cp.ClientCondID != "" {
			e.byClient[cp.ClientCondID] = cp.ID
		}
		e.incActiveConditionalLocked(cp.UserID, cp.Symbol)
	}
	e.terminals = make(map[uint64]*Conditional, len(terminals))
	e.termOrder = make([]uint64, 0, len(terminals))
	for _, c := range terminals {
		cp := *c
		e.terminals[cp.ID] = &cp
		e.termOrder = append(e.termOrder, cp.ID)
	}
	e.offsets = make(map[int32]int64, len(offsets))
	for p, o := range offsets {
		e.offsets[p] = o
	}
}

// Offsets returns a copy of the consumer watermark for restart.
func (e *Engine) Offsets() map[int32]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[int32]int64, len(e.offsets))
	for p, o := range e.offsets {
		out[p] = o
	}
	return out
}

// OCOByClient returns a copy of the client_oco_id → group_id dedup map
// (for snapshot capture).
func (e *Engine) OCOByClient() map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]string, len(e.ocoByClient))
	for k, v := range e.ocoByClient {
		out[k] = v
	}
	return out
}

// SetOCOByClient replaces the dedup map (for snapshot restore).
func (e *Engine) SetOCOByClient(m map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ocoByClient = make(map[string]string, len(m))
	for k, v := range m {
		e.ocoByClient[k] = v
	}
}

// -----------------------------------------------------------------------------
// Trigger rule
// -----------------------------------------------------------------------------

// ShouldFire implements the BN-style trigger matrix: the first crossing of
// the stop threshold fires the conditional.
//
//	side | type        | condition
//	-----+-------------+--------------------------------
//	sell | STOP_LOSS*  | last_price <= stop_price
//	sell | TAKE_PROF*  | last_price >= stop_price
//	buy  | STOP_LOSS*  | last_price >= stop_price
//	buy  | TAKE_PROF*  | last_price <= stop_price
func ShouldFire(side eventpb.Side, typ condrpc.ConditionalType, lastPrice, stopPrice dec.Decimal) bool {
	isStop := typ == condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS ||
		typ == condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT
	isTP := typ == condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT ||
		typ == condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT
	if !isStop && !isTP {
		return false
	}
	cmp := lastPrice.Cmp(stopPrice)
	switch side {
	case eventpb.Side_SIDE_SELL:
		if isStop {
			return cmp <= 0 // fell to / below stop
		}
		return cmp >= 0 // rose to / above target
	case eventpb.Side_SIDE_BUY:
		if isStop {
			return cmp >= 0 // rose to / above stop (break-in)
		}
		return cmp <= 0 // fell to / below target
	}
	return false
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// buildConditional validates the wire request and converts it to the
// internal decimal form. Does not assign id / status — the caller does
// that under the lock.
func buildConditional(req *condrpc.PlaceConditionalRequest) (*Conditional, error) {
	if req == nil {
		return nil, fmt.Errorf("conditional: nil request")
	}
	if req.UserId == "" {
		return nil, ErrMissingUserID
	}
	if req.Symbol == "" {
		return nil, ErrMissingSymbol
	}
	if req.Side != eventpb.Side_SIDE_BUY && req.Side != eventpb.Side_SIDE_SELL {
		return nil, ErrInvalidSide
	}
	switch req.Type {
	case condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS,
		condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT,
		condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT,
		condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT,
		condrpc.ConditionalType_CONDITIONAL_TYPE_TRAILING_STOP_LOSS:
	default:
		return nil, ErrInvalidType
	}
	isTrailing := req.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_TRAILING_STOP_LOSS
	var stop dec.Decimal
	if !isTrailing {
		s, err := dec.Parse(req.StopPrice)
		if err != nil || !dec.IsPositive(s) {
			return nil, ErrInvalidStopPrice
		}
		stop = s
	} else if req.StopPrice != "" && req.StopPrice != "0" {
		return nil, ErrStopPriceForbidden
	}
	isLimit := req.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT ||
		req.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT
	var limit dec.Decimal
	if isLimit {
		l, lerr := dec.Parse(req.LimitPrice)
		if lerr != nil || !dec.IsPositive(l) {
			return nil, ErrLimitPriceRequired
		}
		limit = l
	} else if req.LimitPrice != "" {
		return nil, ErrLimitPriceForbidden
	}

	// Trailing validation.
	var activation dec.Decimal
	if isTrailing {
		if req.TrailingDeltaBps <= 0 {
			return nil, ErrTrailingDeltaNeeded
		}
		if req.TrailingDeltaBps > 10_000 {
			return nil, ErrTrailingDeltaRange
		}
		if req.ActivationPrice != "" {
			ap, aerr := dec.Parse(req.ActivationPrice)
			if aerr != nil || !dec.IsPositive(ap) {
				return nil, fmt.Errorf("%w: %v", ErrActivationPriceShape, aerr)
			}
			activation = ap
		}
	} else {
		if req.TrailingDeltaBps != 0 {
			return nil, ErrTrailingDeltaForbidden
		}
		if req.ActivationPrice != "" {
			return nil, ErrActivationPriceShape
		}
	}
	qty, err := dec.Parse(req.Qty)
	if err != nil {
		return nil, ErrQtyRequired
	}
	quoteQty, err := dec.Parse(req.QuoteQty)
	if err != nil {
		return nil, ErrQuoteQtyShape
	}
	if err := validateShape(req.Side, req.Type, qty, quoteQty); err != nil {
		return nil, err
	}
	return &Conditional{
		ClientCondID:     req.ClientConditionalId,
		UserID:           req.UserId,
		Symbol:           req.Symbol,
		Side:             req.Side,
		Type:             req.Type,
		StopPrice:        stop,
		LimitPrice:       limit,
		Qty:              qty,
		QuoteQty:         quoteQty,
		TIF:              req.Tif,
		ExpiresAtMs:      req.ExpiresAtUnixMs,
		TrailingDeltaBps: req.TrailingDeltaBps,
		ActivationPrice:  activation,
	}, nil
}

// validateShape enforces the qty/quote_qty rules. For MARKET variants we
// mirror Counter's contract (ADR-0035):
//
//   - MARKET sell: qty > 0; quote_qty must be zero
//   - MARKET buy: quote_qty > 0; qty must be zero (BN quoteOrderQty form).
//     MARKET buy + qty is explicitly rejected; clients that want "qty-
//     denominated" buys should use STOP_LOSS_LIMIT / TAKE_PROFIT_LIMIT
//     with an explicit limit_price (same pattern BFF uses for slippage
//     translation).
//   - LIMIT variants: qty > 0; quote_qty must be zero
func validateShape(side eventpb.Side, typ condrpc.ConditionalType, qty, quoteQty dec.Decimal) error {
	isLimit := typ == condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT ||
		typ == condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT
	if isLimit {
		if !dec.IsPositive(qty) {
			return ErrQtyRequired
		}
		if dec.IsPositive(quoteQty) {
			return ErrQuoteQtyShape
		}
		return nil
	}
	// MARKET variant (incl. TRAILING_STOP_LOSS which always fires MARKET).
	if side == eventpb.Side_SIDE_BUY {
		if !dec.IsPositive(quoteQty) {
			return fmt.Errorf("%w: market buy requires quote_qty (ADR-0035)", ErrQtyRequired)
		}
		if dec.IsPositive(qty) {
			return ErrBothQtyAndQuoteQty
		}
		return nil
	}
	// MARKET sell.
	if !dec.IsPositive(qty) {
		return ErrQtyRequired
	}
	if dec.IsPositive(quoteQty) {
		return ErrQuoteQtyShape
	}
	return nil
}

// SweepExpired marks every PENDING conditional whose ExpiresAtMs has
// passed as EXPIRED, and best-effort releases its reservation. Called on
// a background ticker from main. Returns the number of conditionals
// flipped so callers / tests can assert (ADR-0043).
func (e *Engine) SweepExpired(ctx context.Context) int {
	nowMs := e.cfg.Clock().UnixMilli()
	type expired struct {
		id     uint64
		userID string
	}
	var victims []expired
	var cascaded []releaseTarget
	var snaps []Conditional
	e.mu.Lock()
	for id, c := range e.pending {
		if c.ExpiresAtMs > 0 && c.ExpiresAtMs <= nowMs {
			c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_EXPIRED
			c.TriggeredAtMs = nowMs
			victims = append(victims, expired{id: id, userID: c.UserID})
			snaps = append(snaps, *c)
			e.graduateLocked(c)
			targets, siblingSnaps := e.cascadeOCOCancelLocked(c, "sibling OCO leg expired")
			cascaded = append(cascaded, targets...)
			snaps = append(snaps, siblingSnaps...)
		}
	}
	e.mu.Unlock()
	for _, v := range victims {
		e.bestEffortRelease(ctx, v.userID, e.refIDFor(v.id))
		if e.logger != nil {
			e.logger.Info("conditional expired",
				zap.Uint64("id", v.id),
				zap.String("user_id", v.userID))
		}
	}
	e.releaseAll(ctx, cascaded)
	e.emitSnapshots(snaps)
	return len(victims)
}

// releaseTarget identifies one (user, refID) pair the cascade / OCO path
// wants to release outside the engine lock.
type releaseTarget struct{ userID, refID string }

// cascadeOCOCancelLocked: if c is part of an OCO group, mark every still-
// PENDING sibling as CANCELED and graduate them. Caller must hold e.mu.
// Returns the list of reservation releases the caller must perform after
// unlocking *and* clones of the transitioned siblings (for JournalSink
// emission after the caller unlocks). reason is the reject_reason stamped
// on the siblings.
func (e *Engine) cascadeOCOCancelLocked(c *Conditional, reason string) ([]releaseTarget, []Conditional) {
	if c.OCOGroupID == "" {
		return nil, nil
	}
	now := e.cfg.Clock().UnixMilli()
	var out []releaseTarget
	var snaps []Conditional
	// Collect victim ids first — mutating the map while ranging is fine
	// in Go but makes the intent clearer in two passes.
	var ids []uint64
	for id, sib := range e.pending {
		if sib.OCOGroupID == c.OCOGroupID && sib.Status == condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		sib := e.pending[id]
		if sib == nil {
			continue
		}
		sib.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED
		sib.TriggeredAtMs = now
		sib.RejectReason = reason
		out = append(out, releaseTarget{userID: sib.UserID, refID: e.refIDFor(id)})
		snaps = append(snaps, *sib)
		e.graduateLocked(sib)
	}
	return out, snaps
}

// releaseAll issues best-effort ReleaseReservation for every entry in
// targets. Used after cascadeOCOCancelLocked + lock release.
func (e *Engine) releaseAll(ctx context.Context, targets []releaseTarget) {
	for _, t := range targets {
		e.bestEffortRelease(ctx, t.userID, t.refID)
	}
}

// nextID is a thin wrapper so tests / callers can override via idgen.
func (e *Engine) nextID() uint64 { return e.idgen.Next() }

// refIDFor computes the reservation ref_id used to tie a conditional to its
// Counter reservation. Stable string so retries / replays reuse the same id.
func (e *Engine) refIDFor(id uint64) string { return "cond-" + formatUint(id) }

// bestEffortRelease calls Counter.ReleaseReservation ignoring failures.
// Used in Place dedup races, Cancel, and trigger rejection cleanup paths.
// Release is idempotent on Counter, so retries are safe.
func (e *Engine) bestEffortRelease(ctx context.Context, userID, refID string) {
	if e.reserver == nil {
		return
	}
	_, err := e.reserver.ReleaseReservation(ctx, &counterrpc.ReleaseReservationRequest{
		UserId:        userID,
		ReservationId: refID,
	})
	if err != nil && e.logger != nil {
		e.logger.Warn("release reservation failed",
			zap.String("ref_id", refID),
			zap.String("user_id", userID),
			zap.Error(err))
	}
}

// buildReserveReq projects a Conditional into the ReserveRequest Counter
// runs ComputeFreeze against. The order shape here must match what
// buildPlaceOrderReq produces at trigger time — otherwise Counter returns
// ErrReservationMismatch when the consuming PlaceOrder arrives.
func buildReserveReq(c *Conditional, refID string) *counterrpc.ReserveRequest {
	isLimit := c.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT ||
		c.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT
	req := &counterrpc.ReserveRequest{
		UserId:        c.UserID,
		ReservationId: refID,
		Symbol:        c.Symbol,
		Side:          c.Side,
		Qty:           optString(c.Qty),
		QuoteQty:      optString(c.QuoteQty),
	}
	if isLimit {
		req.OrderType = eventpb.OrderType_ORDER_TYPE_LIMIT
		req.Price = c.LimitPrice.String()
	} else {
		req.OrderType = eventpb.OrderType_ORDER_TYPE_MARKET
	}
	return req
}

// buildPlaceOrderReq turns a Conditional into the Counter gRPC request that
// should fire. Order type is MARKET for the base variants and LIMIT + TIF
// for the *_LIMIT variants.
func buildPlaceOrderReq(c *Conditional) *counterrpc.PlaceOrderRequest {
	isLimit := c.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT ||
		c.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT
	req := &counterrpc.PlaceOrderRequest{
		UserId:        c.UserID,
		ClientOrderId: "cond-" + formatUint(c.ID),
		Symbol:        c.Symbol,
		Side:          c.Side,
		Qty:           optString(c.Qty),
		QuoteQty:      optString(c.QuoteQty),
	}
	if isLimit {
		req.OrderType = eventpb.OrderType_ORDER_TYPE_LIMIT
		req.Price = c.LimitPrice.String()
		req.Tif = c.TIF
	} else {
		req.OrderType = eventpb.OrderType_ORDER_TYPE_MARKET
	}
	return req
}

// optString returns d.String() unless d is zero, in which case it returns
// "" so Counter treats the field as absent.
func optString(d dec.Decimal) string {
	if dec.IsZero(d) {
		return ""
	}
	return d.String()
}

func formatUint(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for v > 0 {
		pos--
		buf[pos] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[pos:])
}

// publicTradeOf extracts the PublicTrade payload from a market-data event,
// or (nil, false) if the event is a different shape.
func publicTradeOf(evt *eventpb.MarketDataEvent) (*eventpb.PublicTrade, bool) {
	if evt == nil {
		return nil, false
	}
	p, ok := evt.Payload.(*eventpb.MarketDataEvent_PublicTrade)
	if !ok {
		return nil, false
	}
	return p.PublicTrade, p.PublicTrade != nil
}

// lookupLocked returns the conditional from either the pending or terminal
// map. Caller must hold e.mu.
func (e *Engine) lookupLocked(id uint64) *Conditional {
	if c, ok := e.pending[id]; ok {
		return c
	}
	if c, ok := e.terminals[id]; ok {
		return c
	}
	return nil
}

// graduateLocked moves a conditional from pending to the terminals map +
// FIFO list, trimming to TerminalHistoryLimit. Caller must hold e.mu.
func (e *Engine) graduateLocked(c *Conditional) {
	if _, wasPending := e.pending[c.ID]; wasPending {
		e.decActiveConditionalLocked(c.UserID, c.Symbol)
	}
	delete(e.pending, c.ID)
	if e.cfg.TerminalHistoryLimit == 0 {
		// Still record it so in-flight Query / List responses see the
		// new status, but do not retain beyond this turn.
		e.terminals[c.ID] = c
		e.termOrder = append(e.termOrder, c.ID)
		return
	}
	e.terminals[c.ID] = c
	e.termOrder = append(e.termOrder, c.ID)
	for len(e.termOrder) > e.cfg.TerminalHistoryLimit {
		drop := e.termOrder[0]
		e.termOrder = e.termOrder[1:]
		if t, ok := e.terminals[drop]; ok {
			if t.ClientCondID != "" {
				delete(e.byClient, t.ClientCondID)
			}
			delete(e.terminals, drop)
		}
	}
}

// cleanRejectReason pulls a compact string out of a gRPC error, falling
// back to err.Error() for non-gRPC failures.
func cleanRejectReason(err error) string {
	if err == nil {
		return ""
	}
	if st, ok := status.FromError(err); ok {
		msg := st.Message()
		if msg == "" && st.Code() != codes.OK {
			msg = st.Code().String()
		}
		return msg
	}
	return err.Error()
}
