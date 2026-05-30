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

// Order is an in-flight perp order record held by the service.
type Order struct {
	OrderID    uint64
	ClientID   string
	UserID     string
	Symbol     string
	Side       perpstate.Side
	Type       eventpb.OrderType
	TIF        eventpb.TimeInForce
	Price      dec.Decimal
	Qty        dec.Decimal
	Leverage   dec.Decimal
	ReduceOnly bool
	ReservedIM dec.Decimal
	FilledQty  dec.Decimal
	Status     eventpb.InternalOrderStatus
	CreatedMs  int64
	UpdatedMs  int64
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
	BackstopAccount    string
	BackstopAfterTicks int

	Clock func() time.Time // nil → time.Now
}

// Service is the perp order + settlement coordinator.
type Service struct {
	eng      *engine.Engine
	dispatch Dispatcher
	journal  Journal
	cfg      Config
	risk     perpstate.RiskModel
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
	if cfg.BackstopAccount == "" {
		cfg.BackstopAccount = "__perp_backstop__"
	}
	if cfg.BackstopAfterTicks <= 0 {
		cfg.BackstopAfterTicks = 2
	}
	return &Service{
		eng: eng, dispatch: dispatch, journal: journal, cfg: cfg,
		risk:   perpstate.NewRiskModel(cfg.RiskTiers, cfg.MMR, cfg.MaxLeverage, cfg.LiquidationFeeRate),
		nextID: nextID, seq: newUserSeq(), orders: map[uint64]*Order{},
		offsets:    map[int32]int64{},
		liqByKey:   map[string]*liquidation{},
		liqByOrder: map[uint64]*liquidation{},
	}
}

func (s *Service) now() int64 { return s.cfg.Clock().UnixMilli() }

// PlaceOrder runs the pre-trade margin gate and dispatches to Match. The
// match outcome arrives asynchronously via HandleTrade (ADR-0007).
func (s *Service) PlaceOrder(req *perprpc.PlaceOrderRequest) (*perprpc.PlaceOrderResponse, error) {
	if req.GetUserId() == "" || req.GetSymbol() == "" {
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
	lev, err := dec.Parse(req.GetLeverage())
	if err != nil || lev.Sign() <= 0 {
		return nil, errors.New("invalid leverage")
	}
	var price dec.Decimal
	isMarket := req.GetOrderType() == eventpb.OrderType_ORDER_TYPE_MARKET
	if !isMarket {
		price, err = dec.Parse(req.GetPrice())
		if err != nil || price.Sign() <= 0 {
			return nil, errors.New("invalid price")
		}
	}

	resp := &perprpc.PlaceOrderResponse{ReceivedTsUnixMs: s.now()}
	s.seq.do(req.GetUserId(), func() {
		// reduce_only must close, never increase: requires an existing
		// position on the side opposite this order.
		if req.GetReduceOnly() {
			pos, ok := s.eng.PositionOf(req.GetUserId(), req.GetSymbol())
			if !ok || pos.Side == side {
				resp = s.reject(req, "reduce_only_requires_opposite_position")
				return
			}
		}
		var reservedIM dec.Decimal = zero
		if !req.GetReduceOnly() {
			imPrice := price
			if isMarket {
				imPrice = s.eng.MarkOf(req.GetSymbol())
				if imPrice.Sign() <= 0 {
					resp = s.reject(req, "no_mark_for_market_order")
					return
				}
			}
			if maxLev := s.maxLeverageForOrder(req.GetUserId(), req.GetSymbol(), side, imPrice, qty); maxLev.Sign() > 0 && lev.Cmp(maxLev) > 0 {
				resp = s.reject(req, "leverage_exceeds_max")
				return
			}
			im := perpstate.InitMargin(imPrice, qty, lev)
			if !s.eng.Reserve(req.GetUserId(), im) {
				resp = s.reject(req, "insufficient_margin")
				return
			}
			reservedIM = im
		}

		o := &Order{
			OrderID: s.nextID(), ClientID: req.GetClientOrderId(), UserID: req.GetUserId(),
			Symbol: req.GetSymbol(), Side: side, Type: req.GetOrderType(), TIF: req.GetTif(),
			Price: price, Qty: qty, Leverage: lev, ReduceOnly: req.GetReduceOnly(),
			ReservedIM: reservedIM, FilledQty: zero,
			Status:    eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW,
			CreatedMs: s.now(), UpdatedMs: s.now(),
		}
		s.putOrder(o)

		if err := s.dispatch.DispatchOrder(o.Symbol, s.placedOrderEvent(o)); err != nil {
			if reservedIM.Sign() > 0 {
				s.eng.Release(req.GetUserId(), reservedIM)
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
	if t.GetMakerUserId() != "" && t.GetMakerUserId() == t.GetTakerUserId() {
		s.settleSelfTrade(t, matchSeq)
		return
	}
	takerSide := fromEventSide(t.GetTakerSide())
	s.settleLeg(t.GetTakerUserId(), t.GetTakerOrderId(), takerSide, matchSeq, t,
		t.GetTakerStatusAfter(), t.GetTakerFilledQtyAfter())
	s.settleLeg(t.GetMakerUserId(), t.GetMakerOrderId(), takerSide.Opposite(), matchSeq, t,
		t.GetMakerStatusAfter(), t.GetMakerFilledQtyAfter())
}

func (s *Service) settleLeg(user string, orderID uint64, side perpstate.Side, matchSeq uint64,
	t *eventpb.Trade, statusAfter eventpb.InternalOrderStatus, filledAfter string) {
	s.seq.do(user, func() {
		o := s.getOrder(orderID)
		if o == nil || o.UserID != user {
			return // not owned by this shard / not a perp order we track
		}
		// A fill of the bankruptcy reduce_only order settles to insurance, not
		// the wallet (ADR-0068 §8), via a separate path.
		if liq := s.liquidationFor(orderID); liq != nil {
			s.settleLiquidationFill(o, liq, side, matchSeq, t, statusAfter, filledAfter)
			return
		}
		fill := perpstate.Fill{Side: side, Price: dec.New(t.GetPrice()), Qty: dec.New(t.GetQty()), Fee: zero}
		res, applied := s.eng.ApplyFillWithSeq(user, o.Symbol, o.Leverage, matchSeq, fill)
		if !applied {
			return // replay
		}
		s.afterFill(o, t, side, res, statusAfter, filledAfter)
	})
}

// settleSelfTrade applies both legs of a same-user trade in one serialized
// step, bypassing the per-leg seq guard (it would skip the second leg) and
// advancing the watermark once at the end.
func (s *Service) settleSelfTrade(t *eventpb.Trade, matchSeq uint64) {
	user := t.GetTakerUserId()
	takerSide := fromEventSide(t.GetTakerSide())
	s.seq.do(user, func() {
		taker := s.getOrder(t.GetTakerOrderId())
		maker := s.getOrder(t.GetMakerOrderId())
		price := dec.New(t.GetPrice())
		qty := dec.New(t.GetQty())
		// Guard once on the taker order's symbol watermark.
		if taker != nil {
			res, applied := s.eng.ApplyFillWithSeq(user, taker.Symbol, taker.Leverage, matchSeq,
				perpstate.Fill{Side: takerSide, Price: price, Qty: qty, Fee: zero})
			if !applied {
				return
			}
			s.afterFill(taker, t, takerSide, res, t.GetTakerStatusAfter(), t.GetTakerFilledQtyAfter())
		}
		if maker != nil {
			// seq=0 bypasses the guard (already advanced by the taker leg).
			res, _ := s.eng.ApplyFillWithSeq(user, maker.Symbol, maker.Leverage, 0,
				perpstate.Fill{Side: takerSide.Opposite(), Price: price, Qty: qty, Fee: zero})
			s.afterFill(maker, t, takerSide.Opposite(), res, t.GetMakerStatusAfter(), t.GetMakerFilledQtyAfter())
		}
	})
}

// afterFill updates order bookkeeping + emits settlement & status journal.
// Caller holds the user's seq lock.
func (s *Service) afterFill(o *Order, t *eventpb.Trade, side perpstate.Side, res perpstate.FillResult,
	statusAfter eventpb.InternalOrderStatus, filledAfter string) {
	old := o.Status
	// Drain this order's still-held initial margin by what this fill committed
	// to position margin (engine.routeCash draws MarginAdded from Reserved
	// first). What's left is the residual a later cancel/reject/expire must
	// release — releasing the full original ReservedIM would double-count the
	// part already converted to position_margin.
	if res.MarginAdded.Sign() > 0 {
		o.ReservedIM = o.ReservedIM.Sub(res.MarginAdded)
		if o.ReservedIM.Sign() < 0 {
			o.ReservedIM = zero
		}
	}
	if filledAfter != "" {
		o.FilledQty = dec.New(filledAfter)
	}
	if statusAfter != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_UNSPECIFIED {
		o.Status = statusAfter
	}
	o.UpdatedMs = s.now()
	s.emitSettlement(o, t, side, res)
	if o.Status != old {
		s.emitOrderStatus(o, old, o.Status)
	}
	if isTerminal(o.Status) {
		s.delOrder(o.OrderID)
	}
}
