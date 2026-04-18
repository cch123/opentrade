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
type OrderPlacer interface {
	PlaceOrder(ctx context.Context, in *counterrpc.PlaceOrderRequest) (*counterrpc.PlaceOrderResponse, error)
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
}

// Engine owns the pending / terminal maps and coordinates triggers.
type Engine struct {
	cfg     Config
	idgen   IDGen
	placer  OrderPlacer
	logger  *zap.Logger

	mu         sync.Mutex
	pending    map[uint64]*Conditional
	terminals  map[uint64]*Conditional
	termOrder  []uint64 // FIFO of terminal ids for trim
	byClient   map[string]uint64
	lastPrice  map[string]dec.Decimal
	offsets    map[int32]int64
}

// New builds an Engine. Pass a zap.NewNop() for tests.
func New(cfg Config, idgen IDGen, placer OrderPlacer, logger *zap.Logger) *Engine {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.TerminalHistoryLimit < 0 {
		cfg.TerminalHistoryLimit = 0
	}
	return &Engine{
		cfg:       cfg,
		idgen:     idgen,
		placer:    placer,
		logger:    logger,
		pending:   make(map[uint64]*Conditional),
		terminals: make(map[uint64]*Conditional),
		byClient:  make(map[string]uint64),
		lastPrice: make(map[string]dec.Decimal),
		offsets:   make(map[int32]int64),
	}
}

// -----------------------------------------------------------------------------
// Place / Cancel / Query
// -----------------------------------------------------------------------------

// Place validates req, stores a new PENDING conditional, returns its id.
// Duplicate client_conditional_id returns the existing record with
// accepted=false (idempotency per proto contract).
func (e *Engine) Place(req *condrpc.PlaceConditionalRequest) (id uint64, status condrpc.ConditionalStatus, accepted bool, err error) {
	c, err := buildConditional(req)
	if err != nil {
		return 0, 0, false, err
	}
	c.CreatedAtMs = e.cfg.Clock().UnixMilli()
	c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING

	e.mu.Lock()
	defer e.mu.Unlock()
	if c.ClientCondID != "" {
		if existing, ok := e.byClient[c.ClientCondID]; ok {
			prior := e.lookupLocked(existing)
			if prior != nil {
				return prior.ID, prior.Status, false, nil
			}
		}
	}
	c.ID = e.idgen.Next()
	e.pending[c.ID] = c
	if c.ClientCondID != "" {
		e.byClient[c.ClientCondID] = c.ID
	}
	return c.ID, c.Status, true, nil
}

// Cancel transitions a PENDING conditional to CANCELED. The second return
// value is accepted=true only when a state change actually happened.
func (e *Engine) Cancel(userID string, id uint64) (condrpc.ConditionalStatus, bool, error) {
	if userID == "" {
		return 0, false, ErrMissingUserID
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.lookupLocked(id)
	if c == nil {
		return 0, false, ErrNotFound
	}
	if c.UserID != userID {
		return 0, false, ErrNotOwner
	}
	if c.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		return c.Status, false, nil
	}
	c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_CANCELED
	c.TriggeredAtMs = e.cfg.Clock().UnixMilli()
	e.graduateLocked(c)
	return c.Status, true, nil
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
		if ShouldFire(c.Side, c.Type, price, c.StopPrice) {
			tofire = append(tofire, c.ID)
		}
	}
	return tofire
}

// tryFire issues the inner Counter PlaceOrder call for id and commits the
// outcome to state. If the conditional was canceled or already terminal
// by the time we reacquire the lock, the result is ignored. triggeredAtMs
// is the wall-clock ms stamp set on the record regardless of success.
func (e *Engine) tryFire(ctx context.Context, id uint64, triggeredAtMs int64) {
	e.mu.Lock()
	c := e.lookupLocked(id)
	if c == nil || c.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		e.mu.Unlock()
		return
	}
	req := buildPlaceOrderReq(c)
	e.mu.Unlock()

	resp, err := e.placer.PlaceOrder(ctx, req)

	e.mu.Lock()
	defer e.mu.Unlock()
	c = e.lookupLocked(id)
	if c == nil || c.Status != condrpc.ConditionalStatus_CONDITIONAL_STATUS_PENDING {
		return
	}
	c.TriggeredAtMs = triggeredAtMs
	if err != nil {
		c.Status = condrpc.ConditionalStatus_CONDITIONAL_STATUS_REJECTED
		c.RejectReason = cleanRejectReason(err)
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
	e.graduateLocked(c)
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
// or callers must accept replacement semantics.
func (e *Engine) Restore(pending, terminals []*Conditional, offsets map[int32]int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = make(map[uint64]*Conditional, len(pending))
	e.byClient = make(map[string]uint64, len(pending))
	for _, c := range pending {
		cp := *c
		e.pending[cp.ID] = &cp
		if cp.ClientCondID != "" {
			e.byClient[cp.ClientCondID] = cp.ID
		}
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
		condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT:
	default:
		return nil, ErrInvalidType
	}
	stop, err := dec.Parse(req.StopPrice)
	if err != nil || !dec.IsPositive(stop) {
		return nil, ErrInvalidStopPrice
	}
	isLimit := req.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_STOP_LOSS_LIMIT ||
		req.Type == condrpc.ConditionalType_CONDITIONAL_TYPE_TAKE_PROFIT_LIMIT
	var limit dec.Decimal
	if isLimit {
		limit, err = dec.Parse(req.LimitPrice)
		if err != nil || !dec.IsPositive(limit) {
			return nil, ErrLimitPriceRequired
		}
	} else if req.LimitPrice != "" {
		return nil, ErrLimitPriceForbidden
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
		ClientCondID: req.ClientConditionalId,
		UserID:       req.UserId,
		Symbol:       req.Symbol,
		Side:         req.Side,
		Type:         req.Type,
		StopPrice:    stop,
		LimitPrice:   limit,
		Qty:          qty,
		QuoteQty:     quoteQty,
		TIF:          req.Tif,
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
	// MARKET variant.
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
