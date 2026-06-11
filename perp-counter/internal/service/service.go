// Package service is perp-counter's order + settlement logic (ADR-0068 M3).
// It sits between the Connect server and the engine: PlaceOrder runs the
// pre-trade margin gate (the check spot deliberately skips, ADR-0068 §4) and
// dispatches to Match; HandleTrade settles fills into positions under the
// per-user serializer (invariant #1) with the match_seq replay guard
// (invariant #3). Match dispatch and the perp-journal are injected
// interfaces so the logic is testable without Kafka; the real adapters are
// wired in cmd/perp-counter (M3 wiring / later).
package service

import (
	"sync"
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpcfg"
	"github.com/xargin/opentrade/pkg/perpstate"
)

var zero = dec.FromInt(0)

// Dispatcher forwards order-event records to Match (order-event-<symbol>
// topic, ADR-0050/0068 §1). The service builds the wire event so the
// adapter stays a thin, symbol-agnostic Kafka producer (mirrors the
// counter service → journal layering); symbol is passed alongside so the
// adapter can derive the per-symbol topic without re-parsing the payload.
type Dispatcher interface {
	DispatchOrder(symbol string, evt *eventpb.OrderEvent) error
	DispatchCancel(symbol string, evt *eventpb.OrderEvent) error
}

// Journal emits perp-journal events (perp-counter's WAL, ADR-0068).
type Journal interface {
	Emit(evt *eventpb.PerpJournalEvent)
}

// TriggerChecker reports whether (user, symbol) holds any ACTIVE
// position-bound trigger (TP/SL/OCO/TrailingStop) — the ADR-0077 §3
// mode-switch guard, wired to the trigger service's CountActiveTriggers RPC
// (ADR-0078 §6). A non-nil error means the query could not be answered;
// callers fail CLOSED (the gated op rejects with its own reason rather than
// assuming "no triggers"). This guard is best-effort UX — the hard
// guarantee stays the fail-closed admission matrix at fire time, which
// expires an orphan trigger as EXPIRED_POSITION_GONE instead of letting it
// reverse-open.
type TriggerChecker interface {
	HasActiveTriggers(user uint64, symbol string) (bool, error)
}

// Order is an in-flight perp order record held by the service. Mode is the
// position's margin mode at admission time (ADR-0074): it routes the IM
// reservation to the right wallet bucket — isolated reservations convert to
// position margin on fill, cross reservations release back to free balance
// per filled proportion. PositionIdx is the ADR-0077 position intent: fills
// route to the (user, symbol, PositionIdx) leg; the mapping lives only here
// (+ snapshot + PerpOrderStatusEvent), never on the Match wire (§6).
type Order struct {
	OrderID     uint64
	ClientID    string
	UserID      uint64
	Symbol      string
	Side        perpstate.Side
	Type        eventpb.OrderType
	TIF         eventpb.TimeInForce
	Price       dec.Decimal
	Qty         dec.Decimal
	Leverage    dec.Decimal
	Mode        perpstate.MarginMode
	PositionIdx uint8
	ReduceOnly  bool
	// SlippageBps marks an ADR-0083 protected market order (>0). Stamped
	// onto the OrderEvent wire so Match derives the collar; locally it only
	// shifts the IM reservation reference price for buys.
	SlippageBps uint32
	ReservedIM  dec.Decimal
	// ReservedFee is the still-held ADR-0079 fee buffer (taker rate on the IM
	// reference notional, reserved with the IM at PlaceOrder). Fills consume
	// it as fees are charged; the remainder releases with the terminal
	// transition (多退少补 settles at terminal, not per fill).
	ReservedFee dec.Decimal
	FilledQty   dec.Decimal
	Status      eventpb.InternalOrderStatus
	CreatedMs   int64
	UpdatedMs   int64

	// ConfigVersion is the SymbolConfig version this order was admitted
	// under (ADR-0075); stamped into the OrderEvent for Match's handshake.
	// 0 = catalog disabled.
	ConfigVersion uint64

	// ADR-0079 §1 fee pin: rates/rule resolved at admission from the SAME
	// catalog view the order was admitted under (+ per-user override), then
	// carried by the order (and its snapshot) so settlement and crash replay
	// charge identical fees. FeeMakerSuppressed records that a negative maker
	// rate was degraded to zero at admission (negative rates disabled).
	FeeRuleID          string
	FeeMakerRate       dec.Decimal
	FeeTakerRate       dec.Decimal
	FeeAsset           string
	FeeMakerSuppressed bool
}

// Config tunes the service.
type Config struct {
	ShardID     int
	ProducerID  string
	MaxLeverage dec.Decimal // legacy cap; zero = no cap
	MMR         dec.Decimal // legacy maintenance margin rate; zero disables liquidation when no tier supplies MMR
	RiskTiers   []perpstate.RiskTier

	// ADR-0070 knobs. Zero values keep the old behavior except that the
	// backstop still has a deterministic system account, so an explicitly
	// enabled liquidation flow never wedges on missing liquidity forever.
	LiquidationFeeRate dec.Decimal
	TargetMarginBuffer dec.Decimal
	BackstopAccount    uint64
	BackstopAfterTicks int

	// ADR-0073: the global perp-risk coordinator owns TakenOverLot lifecycle and
	// ADL planning. The shard keeps a local insurance cache for legacy market
	// liquidation math, but ADL is only executed from version-stamped lot tasks.
	RiskCoordinatorEnabled bool

	// ADR-0074 §7 auto-add-margin product knobs. The trigger line is
	// MMR+AutoAddTriggerBuffer, the top-up target is MMR+AutoAddTargetBuffer,
	// and AutoAddMaxPerEvent caps a single transfer platform-wide (0 =
	// uncapped). Zero buffers get conservative defaults in New.
	AutoAddTriggerBuffer dec.Decimal
	AutoAddTargetBuffer  dec.Decimal
	AutoAddMaxPerEvent   dec.Decimal

	// Catalog is the ADR-0075 SymbolConfig cache. When set it is the
	// admission authority (status machine, precision, order limits, per-
	// symbol risk tiers) and every order/journal record carries its
	// config_version; the legacy risk flags only back symbols it does not
	// cover. nil = flag-driven legacy mode (dev), versions stamp 0.
	Catalog *perpcfg.Cache

	// Triggers is the ADR-0077 §3 mode-switch seam. nil = no checker: exact
	// while perp position-bound triggers do not exist (pre-ADR-0078).
	Triggers TriggerChecker

	// TerminalCOIDCap bounds the terminal client_order_id idempotency ring
	// (ADR-0078 修订 #3, the ADR-0062 mirror). 0 → 4096.
	TerminalCOIDCap int

	// BlockTradeBandBps is the ADR-0078 §8 price sanity band: a block trade
	// price must sit within mark ± band. 0 → 500 (5%).
	BlockTradeBandBps uint32

	// AllowNegativeMakerFee is the ADR-0079 §5 deployment attestation that
	// Match runs with STP enabled. While false (default), a negative maker
	// rate — from SymbolConfig or a per-user override — is degraded to zero
	// at admission pinning (deterministic; the settlement journal marks
	// rebate_suppressed), and SetCustomerFeeRate rejects negative maker
	// overrides outright.
	AllowNegativeMakerFee bool

	Clock func() time.Time // nil → time.Now
}

// Service is the perp order + settlement coordinator.
type Service struct {
	eng      *engine.Engine
	dispatch Dispatcher
	journal  Journal
	cfg      Config
	risk     perpstate.RiskModel
	catalog  *perpcfg.Cache
	riskMemo riskMemo
	nextID   func() uint64
	seq      *userSeq

	// snapshotMu is the ADR-0048 capture barrier. Both consumer entry points
	// (HandleTradeEvent, HandlePerpPriceEvent) hold it RLocked for the duration
	// of a mutation; Capture takes it Locked so the engine state + order store +
	// consumed offsets are a single consistent image with no handler mid-flight.
	snapshotMu sync.RWMutex

	mu       sync.Mutex
	orders   map[uint64]*Order
	perpSeq  uint64          // shard-scoped perp-journal sequence (ADR-0051 style)
	orderSeq uint64          // shard-scoped order-event sequence (separate stream from perpSeq)
	offsets  map[int32]int64 // next-to-consume perp-trade-event offset per partition (ADR-0048 snapshot binding)

	// ADR-0078 修订 #3 client_order_id idempotency: live orders indexed by
	// (user, coid), recently-terminal ids retained in a bounded ring (the
	// ADR-0062 mirror) so a client retry / trigger crash-replay converges on
	// the original order id instead of double-placing.
	activeByCOID map[uint64]map[string]uint64
	coidRing     coidRing

	// ADR-0078 §2 pending amends keyed by the OLD order id; the terminal
	// trade-event continuation places the pre-allocated replacement.
	amends map[uint64]*pendingAmend

	// ADR-0078 §5 close-all registry, one slot per user (terminal entries
	// stay for idempotent re-reads until a new request replaces them).
	closeAlls map[uint64]*closeAllState

	// ADR-0078 §7/§8 admin-op idempotency caches (client_op_id /
	// block_trade_id → first outcome), snapshot-persisted.
	adjustDone map[string]*perprpc.ForceAdjustPositionResponse
	blockDone  map[string]*perprpc.BlockTradeResponse

	// In-flight liquidations (ADR-0068 §8). liqByKey guards against
	// re-triggering a position already being liquidated on the next mark tick;
	// liqByOrder routes the bankruptcy order's fills to insurance settlement.
	liqByKey   map[string]*liquidation
	liqByOrder map[uint64]*liquidation
	adlRound   uint64
}

// New wires the service. nextID supplies order ids (snowflake in prod, a
// counter in tests). A nil dispatcher/journal is replaced with a no-op.
func New(eng *engine.Engine, dispatch Dispatcher, journal Journal, nextID func() uint64, cfg Config) *Service {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if dispatch == nil {
		dispatch = noopDispatcher{}
	}
	if journal == nil {
		journal = noopJournal{}
	}
	if cfg.BackstopAfterTicks <= 0 {
		cfg.BackstopAfterTicks = 2
	}
	if cfg.AutoAddTriggerBuffer.Sign() <= 0 {
		cfg.AutoAddTriggerBuffer = dec.New("0.005")
	}
	if cfg.AutoAddTargetBuffer.Sign() <= 0 {
		cfg.AutoAddTargetBuffer = dec.Max(dec.New("0.01"), cfg.AutoAddTriggerBuffer)
	}
	if cfg.TerminalCOIDCap <= 0 {
		cfg.TerminalCOIDCap = 4096
	}
	if cfg.BlockTradeBandBps == 0 {
		cfg.BlockTradeBandBps = 500
	}
	svc := &Service{
		eng: eng, dispatch: dispatch, journal: journal, cfg: cfg,
		risk:     perpstate.NewRiskModel(cfg.RiskTiers, cfg.MMR, cfg.MaxLeverage, cfg.LiquidationFeeRate),
		catalog:  cfg.Catalog,
		riskMemo: riskMemo{m: map[string]perpstate.RiskModel{}},
		nextID:   nextID, seq: newUserSeq(), orders: map[uint64]*Order{},
		offsets:      map[int32]int64{},
		liqByKey:     map[string]*liquidation{},
		liqByOrder:   map[uint64]*liquidation{},
		activeByCOID: map[uint64]map[string]uint64{},
		coidRing:     newCOIDRing(cfg.TerminalCOIDCap),
		amends:       map[uint64]*pendingAmend{},
		closeAlls:    map[uint64]*closeAllState{},
		adjustDone:   map[string]*perprpc.ForceAdjustPositionResponse{},
		blockDone:    map[string]*perprpc.BlockTradeResponse{},
	}
	// ADR-0072/0074: Engine maintains the liq-price index and resolves each
	// position's effective MMR (tier table + RiskID) itself, so the service
	// installs the whole risk model once after construction (and again only
	// if config is explicitly reloaded in a future admin path).
	eng.SetRiskModel(svc.risk)
	// ADR-0075: the catalog resolver takes precedence per symbol; the flag
	// model above stays as the fallback for symbols it does not cover.
	if svc.catalog != nil {
		eng.SetRiskResolver(svc.resolveRisk)
	}
	return svc
}

func (s *Service) now() int64 { return s.cfg.Clock().UnixMilli() }

// PlaceOrder runs the pre-trade margin gate and dispatches to Match. The
// match outcome arrives asynchronously via HandleTrade (ADR-0007). Idempotent
// on client_order_id (ADR-0078 修订 #3): an active or recently-terminal hit
// returns the original order id with accepted=false.
func (s *Service) PlaceOrder(req *perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
	sp, err := parseOrderSpec(req)
	if err != nil {
		return nil, err
	}
	resp := &perprpc.PlaceOrderResponse{ReceivedTsUnixMs: s.now()}
	s.seq.do(sp.User, func() {
		if sp.ClientID != "" {
			if id, ok := s.lookupByCOID(sp.User, sp.ClientID); ok {
				resp.OrderId, resp.ClientOrderId = id, sp.ClientID
				return // dedup hit: accepted stays false, no reject reason
			}
		}
		// ADR-0078 §5: a running close-all owns the scope — every other
		// placement (including trigger fires) is rejected until it finishes.
		if s.closeAllBlocksLocked(sp.User, sp.Symbol) {
			resp = s.reject(req, "close_all_in_progress")
			return
		}
		id, reason := s.placeOrderLocked(sp, 0)
		if reason != "" {
			resp = s.reject(req, reason)
			return
		}
		resp.OrderId = id
		resp.ClientOrderId = sp.ClientID
		resp.Accepted = true
	})
	return resp, nil
}

// CancelOrder forwards a cancel to Match. The CANCELED transition + margin
// release happen on the resulting OrderCancelled trade-event (HandleCancelled).
func (s *Service) CancelOrder(req *perprpc.CancelOrderRequest) (*perprpc.CancelOrderResponse, error) {
	resp := &perprpc.CancelOrderResponse{OrderId: req.GetOrderId()}
	s.seq.do(req.GetUserId(), func() {
		s.cancelOrderLocked(req.GetUserId(), req.GetOrderId(), resp)
	})
	return resp, nil
}

// cancelOrderLocked is the per-order cancel body shared by CancelOrder,
// BatchCancelOrders and CancelAllOrders (ADR-0078 §3). Caller holds the
// user's seq lock; the outcome (accepted / reject_reason) is written into
// resp.
func (s *Service) cancelOrderLocked(user, orderID uint64, resp *perprpc.CancelOrderResponse) {
	o := s.getOrder(orderID)
	if o == nil || o.UserID != user || isTerminal(o.Status) {
		resp.RejectReason = "not_found"
		return
	}
	// A bankruptcy reduce_only order is system-owned — the user cannot
	// cancel it to dodge liquidation (ADR-0068 §8).
	if s.liquidationFor(o.OrderID) != nil {
		resp.RejectReason = "liquidation_owned"
		return
	}
	// ADR-0075 §2: SETTLING and later states accept system ops only.
	if !s.cancelAllowed(o.Symbol) {
		resp.RejectReason = "symbol_not_cancelable"
		return
	}
	// ADR-0078 §2: an explicit cancel is a stronger intent than a pending
	// amend — abort the amend (no replacement will be placed), then cancel.
	if pa := s.takeAmend(o.OrderID); pa != nil {
		s.emitAmend(pa, eventpb.PerpAmendEvent_STATE_ABORTED_BY_CANCEL, "", zero)
	}
	if err := s.dispatch.DispatchCancel(o.Symbol, s.cancelOrderEvent(o)); err != nil {
		resp.RejectReason = "dispatch_failed"
		return
	}
	old := o.Status
	o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
	o.UpdatedMs = s.now()
	s.emitOrderStatus(o, old, o.Status)
	resp.Accepted = true
	resp.RejectReason = ""
}

// QueryOrder returns a live order by id (terminal orders are evicted — query
// history for those). found=false maps to NOT_FOUND at the server.
func (s *Service) QueryOrder(req *perprpc.QueryOrderRequest) (*perprpc.QueryOrderResponse, bool) {
	o := s.getOrder(req.GetOrderId())
	if o == nil || o.UserID != req.GetUserId() {
		return nil, false
	}
	return &perprpc.QueryOrderResponse{
		OrderId: o.OrderID, ClientOrderId: o.ClientID, Symbol: o.Symbol,
		Side: toEventSide(o.Side), OrderType: o.Type, Tif: o.TIF,
		Price: o.Price.String(), Qty: o.Qty.String(), FilledQty: o.FilledQty.String(),
		ReduceOnly: o.ReduceOnly, Status: o.Status,
		CreatedAtUnixMs: o.CreatedMs, UpdatedAtUnixMs: o.UpdatedMs,
	}, true
}

// HandleTrade settles a Match trade into both legs' positions. Self-trades
// (maker == taker) are applied as one guarded operation so the second leg is
// not dropped by the match_seq replay guard (cf. spot bug 4ccaf23).
func (s *Service) HandleTrade(t *eventpb.Trade, matchSeq uint64) {
	if t.GetMakerUserId() != 0 && t.GetMakerUserId() == t.GetTakerUserId() {
		user := t.GetTakerUserId()
		s.settleSelfTrade(user, t, matchSeq)
		return
	}
	takerSide := fromEventSide(t.GetTakerSide())
	if taker := t.GetTakerUserId(); taker != 0 {
		s.settleLeg(taker, t.GetTakerOrderId(), takerSide, eventpb.LiquidityRole_LIQUIDITY_ROLE_TAKER,
			matchSeq, t, t.GetTakerStatusAfter(), t.GetTakerFilledQtyAfter())
	}
	if maker := t.GetMakerUserId(); maker != 0 {
		s.settleLeg(maker, t.GetMakerOrderId(), takerSide.Opposite(), eventpb.LiquidityRole_LIQUIDITY_ROLE_MAKER,
			matchSeq, t, t.GetMakerStatusAfter(), t.GetMakerFilledQtyAfter())
	}
}

func (s *Service) settleLeg(user uint64, orderID uint64, side perpstate.Side, role eventpb.LiquidityRole,
	matchSeq uint64, t *eventpb.Trade, statusAfter eventpb.InternalOrderStatus, filledAfter string) {
	s.seq.do(user, func() {
		o := s.getOrder(orderID)
		if o == nil || o.UserID != user {
			return // not owned by this shard / not a perp order we track
		}
		// A fill of the bankruptcy reduce_only order settles to insurance, not
		// the wallet (ADR-0068 §8), via a separate path. No trade fee either —
		// liquidation economics are liq_fee_rate → insurance (ADR-0070), not
		// maker/taker fees (ADR-0079 §4).
		if liq := s.liquidationFor(orderID); liq != nil {
			s.settleLiquidationFill(o, liq, side, matchSeq, t, statusAfter, filledAfter)
			return
		}
		fill := perpstate.Fill{Side: side, Price: dec.New(t.GetPrice()), Qty: dec.New(t.GetQty()), Fee: zero}
		charge, fee := s.feeChargeFor(o, role, fill.Price, fill.Qty, false)
		res, excess, feeOut, applied := s.eng.ApplyFillWithFee(user, o.Symbol, o.PositionIdx, o.Leverage, matchSeq, fill, charge)
		if !applied {
			return // replay
		}
		fee.Outcome = feeOut
		s.emitBreachIfAny(o, t, excess)
		s.afterFill(o, t, side, res, fee, statusAfter, filledAfter)
	})
}

// settleSelfTrade applies both legs of a same-user trade in one serialized
// step, bypassing the per-leg seq guard (it would skip the second leg) and
// advancing the watermark once at the end. In hedge mode the two orders may
// target different position legs (e.g. open-long matching open-short) or the
// same leg — each routes by its own order's PositionIdx. Fees: the taker leg
// pays its (non-negative) taker rate as usual; a negative maker rate is
// suppressed to zero (ADR-0079 §5 self-trade rule — deterministic from the
// Trade payload, defense in depth under Match's STP).
func (s *Service) settleSelfTrade(user uint64, t *eventpb.Trade, matchSeq uint64) {
	takerSide := fromEventSide(t.GetTakerSide())
	s.seq.do(user, func() {
		taker := s.getOrder(t.GetTakerOrderId())
		maker := s.getOrder(t.GetMakerOrderId())
		price := dec.New(t.GetPrice())
		qty := dec.New(t.GetQty())
		// Guard once on the taker order's leg watermark.
		if taker != nil {
			charge, fee := s.feeChargeFor(taker, eventpb.LiquidityRole_LIQUIDITY_ROLE_TAKER, price, qty, true)
			res, excess, feeOut, applied := s.eng.ApplyFillWithFee(user, taker.Symbol, taker.PositionIdx, taker.Leverage, matchSeq,
				perpstate.Fill{Side: takerSide, Price: price, Qty: qty, Fee: zero}, charge)
			if !applied {
				return
			}
			fee.Outcome = feeOut
			s.emitBreachIfAny(taker, t, excess)
			s.afterFill(taker, t, takerSide, res, fee, t.GetTakerStatusAfter(), t.GetTakerFilledQtyAfter())
		}
		if maker != nil {
			// seq=0 bypasses the guard (already advanced by the taker leg).
			charge, fee := s.feeChargeFor(maker, eventpb.LiquidityRole_LIQUIDITY_ROLE_MAKER, price, qty, true)
			res, excess, feeOut, _ := s.eng.ApplyFillWithFee(user, maker.Symbol, maker.PositionIdx, maker.Leverage, 0,
				perpstate.Fill{Side: takerSide.Opposite(), Price: price, Qty: qty, Fee: zero}, charge)
			fee.Outcome = feeOut
			s.emitBreachIfAny(maker, t, excess)
			s.afterFill(maker, t, takerSide.Opposite(), res, fee, t.GetMakerStatusAfter(), t.GetMakerFilledQtyAfter())
		}
	})
}

// settleFee bundles one leg's resolved fee for journaling: the requested
// signed amount (from the order's pinned rates) plus how it actually routed.
type settleFee struct {
	Role       eventpb.LiquidityRole
	Amount     dec.Decimal // signed: > 0 user pays, < 0 rebate
	Rate       dec.Decimal // signed rate applied
	Suppressed bool        // a negative maker rate was degraded to zero
	Outcome    engine.FeeOutcome
}

// feeChargeFor computes one fill's fee from the order's ADR-0079 pin. The
// only settle-time inputs are the Trade payload (price/qty/role/self-trade)
// and the order record — both deterministic under replay.
func (s *Service) feeChargeFor(o *Order, role eventpb.LiquidityRole, price, qty dec.Decimal, selfTrade bool) (engine.FeeCharge, settleFee) {
	rate := o.FeeTakerRate
	suppressed := false
	if role == eventpb.LiquidityRole_LIQUIDITY_ROLE_MAKER {
		rate = o.FeeMakerRate
		suppressed = o.FeeMakerSuppressed
		if rate.Sign() < 0 && selfTrade {
			rate = zero
			suppressed = true
		}
	}
	amount := price.Mul(qty).Mul(rate)
	charge := engine.FeeCharge{
		Amount: amount, Asset: o.FeeAsset,
		FromReserve: o.ReservedFee, Cross: o.Mode == perpstate.MarginCross,
	}
	return charge, settleFee{Role: role, Amount: amount, Rate: rate, Suppressed: suppressed}
}

// afterFill updates order bookkeeping + emits settlement & status journal.
// Caller holds the user's seq lock.
func (s *Service) afterFill(o *Order, t *eventpb.Trade, side perpstate.Side, res perpstate.FillResult,
	fee settleFee, statusAfter eventpb.InternalOrderStatus, filledAfter string) {
	old := o.Status
	prevFilled := o.FilledQty
	// Isolated: drain this order's still-held initial margin by what this
	// fill committed to position margin (engine.routeCash draws MarginAdded
	// from Reserved first). What's left is the residual a later
	// cancel/reject/expire must release — releasing the full original
	// ReservedIM would double-count the part already converted to
	// position_margin.
	if res.MarginAdded.Sign() > 0 {
		o.ReservedIM = o.ReservedIM.Sub(res.MarginAdded)
		if o.ReservedIM.Sign() < 0 {
			o.ReservedIM = zero
		}
	}
	// Cross: no margin bucket exists — the reservation for the filled
	// proportion converts back to free cash and the exposure is carried as a
	// derived requirement instead (ADR-0074 §4).
	if o.Mode == perpstate.MarginCross && o.ReservedIM.Sign() > 0 {
		remaining := o.Qty.Sub(prevFilled)
		fillQty := dec.New(t.GetQty())
		if remaining.Sign() > 0 && fillQty.Sign() > 0 {
			release := dec.Min(o.ReservedIM,
				o.ReservedIM.Mul(dec.Min(fillQty, remaining)).Div(remaining))
			if release.Sign() > 0 {
				s.eng.ReleaseCross(o.UserID, release)
				o.ReservedIM = o.ReservedIM.Sub(release)
			}
		}
	}
	// The engine drew this fill's fee from the order's fee reservation
	// (ADR-0079 §4); mirror the draw so the terminal release frees only what
	// is still held. The unconsumed buffer (maker filled below the taker-rate
	// buffer) stays reserved until the terminal transition — 多退少补 settles
	// at terminal, not per fill.
	if fee.Outcome.ReserveUsed.Sign() > 0 {
		o.ReservedFee = o.ReservedFee.Sub(fee.Outcome.ReserveUsed)
		if o.ReservedFee.Sign() < 0 {
			o.ReservedFee = zero
		}
	}
	if filledAfter != "" {
		o.FilledQty = dec.New(filledAfter)
	}
	if statusAfter != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED {
		o.Status = statusAfter
	}
	o.UpdatedMs = s.now()
	s.emitSettlement(o, t, side, res, fee)
	if o.Status != old {
		s.emitOrderStatus(o, old, o.Status)
	}
	if isTerminal(o.Status) {
		// A taker filled at a better price than it reserved for leaves a
		// residual hold that no later lifecycle event would free — release it
		// with the terminal transition.
		s.releaseRemainingIM(o)
		s.retireOrder(o)
		s.onOrderTerminalLocked(o)
	}
}
