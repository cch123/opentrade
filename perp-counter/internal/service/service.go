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
	"errors"
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
// mode-switch guard. Position-bound perp triggers arrive with ADR-0078; until
// that lands no perp trigger can exist, so the nil default (no checker)
// reporting none is exact, not fail-open. ADR-0078's implementation MUST wire
// its real active-set query here (its implementation notes carry the
// dependency).
type TriggerChecker interface {
	HasActiveTriggers(user uint64, symbol string) bool
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
	svc := &Service{
		eng: eng, dispatch: dispatch, journal: journal, cfg: cfg,
		risk:     perpstate.NewRiskModel(cfg.RiskTiers, cfg.MMR, cfg.MaxLeverage, cfg.LiquidationFeeRate),
		catalog:  cfg.Catalog,
		riskMemo: riskMemo{m: map[string]perpstate.RiskModel{}},
		nextID:   nextID, seq: newUserSeq(), orders: map[uint64]*Order{},
		offsets:    map[int32]int64{},
		liqByKey:   map[string]*liquidation{},
		liqByOrder: map[uint64]*liquidation{},
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
// match outcome arrives asynchronously via HandleTrade (ADR-0007).
func (s *Service) PlaceOrder(req *perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
	if req.GetUserId() == 0 || req.GetSymbol() == "" {
		return nil, errors.New("user_id and symbol required")
	}
	side := fromEventSide(req.GetSide())
	if side == 0 {
		return nil, errors.New("invalid side")
	}
	qty, err := dec.Parse(req.GetQty())
	if err != nil || qty.Sign() <= 0 {
		return nil, errors.New("invalid qty")
	}
	// ADR-0074 §8: leverage is position config; the order field is only a
	// convenience entry that creates/overwrites the config. Empty = use the
	// configured leverage.
	reqLev := zero
	if v := req.GetLeverage(); v != "" {
		reqLev, err = dec.Parse(v)
		if err != nil || reqLev.Sign() <= 0 {
			return nil, errors.New("invalid leverage")
		}
	}
	var price dec.Decimal
	isMarket := req.GetOrderType() == eventpb.OrderType_ORDER_TYPE_MARKET
	if !isMarket {
		price, err = dec.Parse(req.GetPrice())
		if err != nil || price.Sign() <= 0 {
			return nil, errors.New("invalid price")
		}
	}
	// ADR-0083: slippage protection is a market-order-only attribute; Match
	// derives the collar from its book, perp-counter only validates the range
	// and adjusts the IM reference price below.
	if req.GetSlippageBps() > 10_000 || (req.GetSlippageBps() > 0 && !isMarket) {
		return nil, errors.New("invalid slippage_bps")
	}

	if req.GetPositionIdx() > uint32(perpstate.IdxShort) {
		return nil, errors.New("invalid position_idx")
	}
	posIdx := uint8(req.GetPositionIdx())

	resp := &perprpc.PlaceOrderResponse{ReceivedTsUnixMs: s.now()}
	s.seq.do(req.GetUserId(), func() {
		user, symbol := req.GetUserId(), req.GetSymbol()
		// ADR-0075 admission gates: status machine + precision + order
		// limits against ONE captured catalog view; its config_version is
		// stamped into the OrderEvent for Match's handshake. Fail-closed on
		// unknown symbol or a stale cache.
		postOnly := req.GetTif() == eventpb.TimeInForce_TIME_IN_FORCE_POST_ONLY
		cfgVersion, cfgReject := s.admitAgainstCatalog(symbol, price, qty, isMarket, postOnly, req.GetReduceOnly())
		if cfgReject != "" {
			resp = s.reject(req, cfgReject)
			return
		}
		// ADR-0077 §2 position-intent matrix, fail-closed both ways. Read
		// inside the sequencer: SetPositionMode runs there too, so the mode
		// this order is validated against cannot change before the order is
		// recorded.
		symCfg := s.eng.SymbolOrderConfigOf(user, symbol)
		if reason := perpstate.ValidateOrderIntent(symCfg.PosMode, posIdx, side, req.GetReduceOnly()); reason != "" {
			resp = s.reject(req, reason)
			return
		}
		// reduce_only must close, never increase: requires an existing
		// position on the side opposite this order, on the targeted leg. This
		// is intentionally only the admission gate; the settlement side clamps
		// (and a hedge leg never flips) once Match fills can arrive after the
		// position has changed.
		if req.GetReduceOnly() {
			pos, ok := s.eng.PositionOf(user, symbol, posIdx)
			if !ok || pos.Side == side {
				resp = s.reject(req, "reduce_only_requires_opposite_position")
				return
			}
		}
		lev, mode, riskID, reason := s.resolveOrderLeverage(user, symbol, symCfg, reqLev)
		if reason != "" {
			resp = s.reject(req, reason)
			return
		}
		// ADR-0079 §1: pin the fee quadruple at admission, from the SAME
		// catalog view the gates ran against (content fetched by version —
		// immutable, no second Active() read). Every order pins, including
		// reduce-only (closing fills pay fees too); only the buffer below is
		// open-order-only.
		pin, pinReject := s.feePinFor(user, symbol, cfgVersion)
		if pinReject != "" {
			resp = s.reject(req, pinReject)
			return
		}
		var reservedIM, reservedFee dec.Decimal = zero, zero
		if !req.GetReduceOnly() {
			imPrice := price
			if isMarket {
				// Market orders have no limit price to bound exposure, so the MVP
				// reserves initial margin at the current mark. That may reject
				// aggressively, but it keeps the counter service independent from
				// order-book liquidity and avoids using last-trade noise for margin.
				imPrice = s.eng.MarkOf(symbol)
				if imPrice.Sign() <= 0 {
					resp = s.reject(req, "no_mark_for_market_order")
					return
				}
				// ADR-0083 §5: a protected market buy admits fills up to
				// best_ask × (1 + bps), so reserve IM at the adverse-adjusted
				// mark × (1 + bps). Sells fill below mark, where mark itself is
				// already the conservative notional bound. This is a best-effort
				// approximation, not a strict upper bound (the book can sit above
				// mark); the settlement-side margin recompute + liquidation
				// engine remain the hard backstop.
				if req.GetSlippageBps() > 0 && side == perpstate.SideBuy {
					imPrice = imPrice.Mul(dec.FromInt(10_000 + int64(req.GetSlippageBps()))).Shift(-4)
				}
			}
			if maxLev := s.maxLeverageForOrder(user, symbol, posIdx, side, imPrice, qty); maxLev.Sign() > 0 && lev.Cmp(maxLev) > 0 {
				resp = s.reject(req, "leverage_exceeds_max")
				return
			}
			// ADR-0074 §9: position + resting orders + this order must fit the
			// selected risk tier's notional cap (per-symbol tiers, ADR-0075;
			// hedge legs sum GROSS — ADR-0077 §7).
			if tierCap := s.riskModelForOrder(symbol).MaxNotionalFor(riskID); tierCap.Sign() > 0 {
				total := s.eng.SymbolNotionalForCap(user, symbol).
					Add(s.activeOrderNotional(user, symbol)).
					Add(imPrice.Mul(qty))
				if total.Cmp(tierCap) > 0 {
					resp = s.reject(req, "notional_exceeds_tier_cap")
					return
				}
			}
			im := perpstate.InitMargin(imPrice, qty, lev)
			// ADR-0079 §4: the order cost adds a fee buffer at the pinned
			// taker rate (the worst-case role) on the same IM reference
			// notional, so the open fee can never dig Available negative.
			// Reserved in ONE call with the IM — affordability is judged on
			// the whole order cost atomically.
			feeBuf := imPrice.Mul(qty).Mul(pin.Taker)
			cost := im.Add(feeBuf)
			if mode == perpstate.MarginCross {
				// ADR-0074 §4: candidate-pool admission, then reserve the order
				// IM from free cash (rule #4).
				if reason, ok := s.eng.CrossOrderCheck(user, symbol, posIdx, side, imPrice, qty, lev, im, s.cfg.TargetMarginBuffer); !ok {
					resp = s.reject(req, reason)
					return
				}
				if !s.eng.ReserveCross(user, cost) {
					resp = s.reject(req, "insufficient_margin")
					return
				}
			} else if !s.eng.Reserve(user, cost) {
				resp = s.reject(req, "insufficient_margin")
				return
			}
			reservedIM = im
			reservedFee = feeBuf
		}

		o := &Order{
			OrderID: s.nextID(), ClientID: req.GetClientOrderId(), UserID: user,
			Symbol: symbol, Side: side, Type: req.GetOrderType(), TIF: req.GetTif(),
			Price: price, Qty: qty, Leverage: lev, Mode: mode, PositionIdx: posIdx,
			ReduceOnly:  req.GetReduceOnly(),
			SlippageBps: req.GetSlippageBps(),
			ReservedIM:  reservedIM, ReservedFee: reservedFee, FilledQty: zero,
			Status:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
			CreatedMs: s.now(), UpdatedMs: s.now(),
			ConfigVersion: cfgVersion,
			FeeRuleID:     pin.RuleID, FeeMakerRate: pin.Maker, FeeTakerRate: pin.Taker,
			FeeAsset: pin.Asset, FeeMakerSuppressed: pin.MakerSuppressed,
		}
		s.putOrder(o)

		if err := s.dispatch.DispatchOrder(o.Symbol, s.placedOrderEvent(o)); err != nil {
			// Dispatch failure means Match never became responsible for the
			// order, so the reservation must be undone synchronously. Once the
			// event is accepted by Match, all later release paths are driven by
			// trade-event lifecycle records for replay safety.
			if cost := reservedIM.Add(reservedFee); cost.Sign() > 0 {
				if mode == perpstate.MarginCross {
					s.eng.ReleaseCross(user, cost)
				} else {
					s.eng.Release(user, cost)
				}
			}
			s.delOrder(o.OrderID)
			resp = s.reject(req, "dispatch_failed")
			return
		}
		s.emitOrderStatus(o, eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED,
			eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW)

		resp.OrderId = o.OrderID
		resp.ClientOrderId = o.ClientID
		resp.Accepted = true
	})
	return resp, nil
}

// CancelOrder forwards a cancel to Match. The CANCELED transition + margin
// release happen on the resulting OrderCancelled trade-event (HandleCancelled).
func (s *Service) CancelOrder(req *perprpc.CancelOrderRequest) (*perprpc.CancelOrderResponse, error) {
	resp := &perprpc.CancelOrderResponse{OrderId: req.GetOrderId()}
	s.seq.do(req.GetUserId(), func() {
		o := s.getOrder(req.GetOrderId())
		if o == nil || o.UserID != req.GetUserId() || isTerminal(o.Status) {
			return
		}
		// A bankruptcy reduce_only order is system-owned — the user cannot
		// cancel it to dodge liquidation (ADR-0068 §8).
		if s.liquidationFor(o.OrderID) != nil {
			return
		}
		// ADR-0075 §2: SETTLING and later states accept system ops only.
		if !s.cancelAllowed(o.Symbol) {
			return
		}
		if err := s.dispatch.DispatchCancel(o.Symbol, s.cancelOrderEvent(o)); err != nil {
			return
		}
		old := o.Status
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
		o.UpdatedMs = s.now()
		s.emitOrderStatus(o, old, o.Status)
		resp.Accepted = true
	})
	return resp, nil
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
		s.delOrder(o.OrderID)
	}
}
