package service

import (
	"sync"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// userSeq serializes work per user (ADR-0068 invariant #1): at most one
// in-flight operation per user, full cross-user parallelism. A keyed mutex
// is sufficient for the MVP; it can be upgraded to a channel-FIFO worker
// (like counter's UserSequencer) if strict ordering under contention is
// needed.
type userSeq struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newUserSeq() *userSeq { return &userSeq{locks: map[string]*sync.Mutex{}} }

func (s *userSeq) do(user string, fn func()) {
	s.mu.Lock()
	l := s.locks[user]
	if l == nil {
		l = &sync.Mutex{}
		s.locks[user] = l
	}
	s.mu.Unlock()
	l.Lock()
	defer l.Unlock()
	fn()
}

// --- order store (guarded by s.mu; ops run inside seq.do) -------------------

func (s *Service) putOrder(o *Order) {
	s.mu.Lock()
	s.orders[o.OrderID] = o
	s.mu.Unlock()
}

func (s *Service) getOrder(id uint64) *Order {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.orders[id]
}

func (s *Service) delOrder(id uint64) {
	s.mu.Lock()
	delete(s.orders, id)
	s.mu.Unlock()
}

// OrderCount reports the number of tracked (non-terminal) orders — test/obs.
func (s *Service) OrderCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orders)
}

// --- journal emit -----------------------------------------------------------

func (s *Service) nextPerpSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.perpSeq++
	return s.perpSeq
}

func (s *Service) meta() *eventpb.EventMeta {
	return &eventpb.EventMeta{TsUnixMs: s.now(), ProducerId: s.cfg.ProducerID}
}

func (s *Service) emitOrderStatus(o *Order, oldSt, newSt eventpb.InternalOrderStatus) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_OrderStatus{OrderStatus: &eventpb.PerpOrderStatusEvent{
			UserId: o.UserID, OrderId: o.OrderID, Symbol: o.Symbol,
			OldStatus: oldSt, NewStatus: newSt, FilledQty: o.FilledQty.String(),
			ReduceOnly: o.ReduceOnly,
		}},
	})
}

func (s *Service) emitSettlement(o *Order, t *eventpb.Trade, side perpstate.Side, res perpstate.FillResult) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Settlement{Settlement: &eventpb.PerpSettlementEvent{
			UserId: o.UserID, OrderId: o.OrderID, TradeId: t.GetTradeId(), Symbol: o.Symbol,
			FillSide: toEventSide(side), Price: t.GetPrice(), Qty: t.GetQty(),
			RealizedPnl: res.Realized.String(), Fee: res.Fee.String(),
			MarginAdded: res.MarginAdded.String(), MarginReleased: res.MarginReleased.String(),
			PositionAfter: s.positionSnap(o.UserID, o.Symbol),
		}},
	})
}

// positionSnap builds the post-change snapshot, including a flat position
// (zeros) so consumers always see the resulting state.
func (s *Service) positionSnap(user, symbol string) *eventpb.PerpPositionSnapshot {
	p, ok := s.eng.PositionRaw(user, symbol)
	if !ok {
		return &eventpb.PerpPositionSnapshot{UserId: user, Symbol: symbol,
			Size: "0", EntryPrice: "0", Margin: "0", Leverage: "0", RealizedPnl: "0"}
	}
	return &eventpb.PerpPositionSnapshot{
		UserId: p.UserID, Symbol: p.Symbol, Side: toEventSide(p.Side),
		Size: p.Size.String(), EntryPrice: p.Entry.String(), Margin: p.Margin.String(),
		Leverage: p.Leverage.String(), RealizedPnl: p.Realized.String(), Version: p.Version,
	}
}

func (s *Service) reject(req *perprpc.PlaceOrderRequest, reason string) *perprpc.PlaceOrderResponse {
	return &perprpc.PlaceOrderResponse{
		ClientOrderId: req.GetClientOrderId(), Accepted: false,
		RejectReason: reason, ReceivedTsUnixMs: s.now(),
	}
}

// --- enum helpers -----------------------------------------------------------

func fromEventSide(s eventpb.Side) perpstate.Side {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return perpstate.SideBuy
	case eventpb.Side_SIDE_SELL:
		return perpstate.SideSell
	default:
		return 0
	}
}

func toEventSide(s perpstate.Side) eventpb.Side {
	switch s {
	case perpstate.SideBuy:
		return eventpb.Side_SIDE_BUY
	case perpstate.SideSell:
		return eventpb.Side_SIDE_SELL
	default:
		return eventpb.Side_SIDE_UNSPECIFIED
	}
}

func isTerminal(st eventpb.InternalOrderStatus) bool {
	switch st {
	case eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED,
		eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED:
		return true
	default:
		return false
	}
}

// --- no-op sinks (used when a dispatcher/journal isn't wired) ---------------

type noopDispatcher struct{}

func (noopDispatcher) DispatchOrder(Order) error                   { return nil }
func (noopDispatcher) DispatchCancel(string, string, uint64) error { return nil }

type noopJournal struct{}

func (noopJournal) Emit(*eventpb.PerpJournalEvent) {}
