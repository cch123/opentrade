package service

// product_api.go is the ADR-0078 order-command surface: client_order_id
// idempotency (修订 #3), amend as cancel + new with a terminal-event
// continuation (§2), best-effort batches (§3), and cancel-all. The shared
// admission/placement pipeline lives in admission.go; the close-all position
// command lives in close_all.go.

import (
	"sort"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/dec"
)

// maxBatchItems bounds one batch request (ADR-0078 §3): the loop runs the
// full per-item admission inside the user's sequencer, so an unbounded batch
// would head-of-line-block the user's settlement events.
const maxBatchItems = 20

// --- client_order_id idempotency (ADR-0078 修订 #3) --------------------------

// coidEntry is one retired (user, coid) → order id record in the terminal
// ring.
type coidEntry struct {
	User    uint64
	COID    string
	OrderID uint64
}

// coidRing is the terminal client_order_id idempotency ring — the ADR-0062
// mirror for perp. FIFO-bounded: the dedup window is "the last cap terminal
// orders", which covers the retry/crash-replay windows the dedup exists for
// (trigger fires, batch retries); ids older than the ring may be reused.
type coidRing struct {
	cap   int
	fifo  []coidEntry
	index map[string]uint64
}

func newCOIDRing(cap int) coidRing {
	return coidRing{cap: cap, index: map[string]uint64{}}
}

func coidKey(user uint64, coid string) string {
	return userIDString(user) + "\x00" + coid
}

func (r *coidRing) add(user uint64, coid string, orderID uint64) {
	if coid == "" || r.cap <= 0 {
		return
	}
	r.fifo = append(r.fifo, coidEntry{User: user, COID: coid, OrderID: orderID})
	r.index[coidKey(user, coid)] = orderID
	for len(r.fifo) > r.cap {
		ev := r.fifo[0]
		r.fifo = r.fifo[1:]
		// Only drop the index entry if it still points at the evicted order —
		// a newer order may have reused the same (user, coid) key.
		if r.index[coidKey(ev.User, ev.COID)] == ev.OrderID {
			delete(r.index, coidKey(ev.User, ev.COID))
		}
	}
}

func (r *coidRing) lookup(user uint64, coid string) (uint64, bool) {
	id, ok := r.index[coidKey(user, coid)]
	return id, ok
}

// lookupByCOID resolves (user, client_order_id) against live orders first,
// then the terminal ring. Runs inside the user's sequencer; takes s.mu for
// the store maps.
func (s *Service) lookupByCOID(user uint64, coid string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if byCOID := s.activeByCOID[user]; byCOID != nil {
		if id, ok := byCOID[coid]; ok {
			return id, true
		}
	}
	return s.coidRing.lookup(user, coid)
}

// indexCOIDLocked records a live order's coid mapping. Caller holds s.mu.
func (s *Service) indexCOIDLocked(o *Order) {
	if o.ClientID == "" {
		return
	}
	byCOID := s.activeByCOID[o.UserID]
	if byCOID == nil {
		byCOID = map[string]uint64{}
		s.activeByCOID[o.UserID] = byCOID
	}
	byCOID[o.ClientID] = o.OrderID
}

// unindexCOIDLocked removes a live order's coid mapping (only if it still
// points at this order). Caller holds s.mu.
func (s *Service) unindexCOIDLocked(o *Order) {
	if o.ClientID == "" {
		return
	}
	if byCOID := s.activeByCOID[o.UserID]; byCOID != nil && byCOID[o.ClientID] == o.OrderID {
		delete(byCOID, o.ClientID)
		if len(byCOID) == 0 {
			delete(s.activeByCOID, o.UserID)
		}
	}
}

// retireOrder evicts a genuinely-terminal order and moves its coid into the
// idempotency ring, keeping the dedup promise across the terminal eviction
// (ADR-0078 修订 #3).
func (s *Service) retireOrder(o *Order) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unindexCOIDLocked(o)
	s.coidRing.add(o.UserID, o.ClientID, o.OrderID)
	delete(s.orders, o.OrderID)
}

// dropOrderNoRing removes an order that never reached Match (dispatch
// failure rollback): the coid must NOT enter the terminal ring — a retry
// with the same client_order_id should be free to try again.
func (s *Service) dropOrderNoRing(o *Order) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unindexCOIDLocked(o)
	delete(s.orders, o.OrderID)
}

// --- amend (ADR-0078 §2: cancel + new, conservative) --------------------------

// pendingAmend is one in-flight amend: the cancel of OldOrderID has been
// dispatched; when its terminal trade-event arrives, the continuation places
// the replacement under the pre-allocated NewOrderID (the replay-convergence
// anchor, 修订 #6).
type pendingAmend struct {
	UserID     uint64
	Symbol     string
	OldOrderID uint64
	NewOrderID uint64
	NewPrice   dec.Decimal
	NewQty     dec.Decimal // new TOTAL intent qty
}

func (s *Service) amendFor(oldOrderID uint64) *pendingAmend {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.amends[oldOrderID]
}

func (s *Service) registerAmend(pa *pendingAmend) {
	s.mu.Lock()
	s.amends[pa.OldOrderID] = pa
	s.mu.Unlock()
}

// takeAmend removes and returns the pending amend for an old order id, if
// any — the single-consumption point for continuation/abort paths.
func (s *Service) takeAmend(oldOrderID uint64) *pendingAmend {
	s.mu.Lock()
	defer s.mu.Unlock()
	pa := s.amends[oldOrderID]
	if pa != nil {
		delete(s.amends, oldOrderID)
	}
	return pa
}

// AmendOrder is the ADR-0078 §2 cancel+new amend, conservative mode only:
// validate, pre-allocate the replacement id, register the pending amend,
// dispatch the cancel, and return. The replacement is placed by the old
// order's terminal-event continuation with qty = new_qty - filled.
func (s *Service) AmendOrder(req *perprpc.AmendOrderRequest) (*perprpc.AmendOrderResponse, error) {
	if req.GetUserId() == 0 || req.GetOrderId() == 0 {
		return nil, errInvalid("user_id and order_id required")
	}
	newPrice, err := dec.Parse(req.GetNewPrice())
	if err != nil || newPrice.Sign() <= 0 {
		return nil, errInvalid("invalid new_price")
	}
	newQty, err := dec.Parse(req.GetNewQty())
	if err != nil || newQty.Sign() <= 0 {
		return nil, errInvalid("invalid new_qty")
	}
	resp := &perprpc.AmendOrderResponse{OldOrderId: req.GetOrderId(), ReceivedTsUnixMs: s.now()}
	s.seq.do(req.GetUserId(), func() {
		o := s.getOrder(req.GetOrderId())
		if o == nil || o.UserID != req.GetUserId() || isTerminal(o.Status) {
			resp.RejectReason = "not_found"
			return
		}
		if s.liquidationFor(o.OrderID) != nil {
			resp.RejectReason = "liquidation_owned"
			return
		}
		if o.Type != eventpb.OrderType_ORDER_TYPE_LIMIT {
			resp.RejectReason = "market_order_not_amendable"
			return
		}
		if o.Status == eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL {
			resp.RejectReason = "cancel_in_progress"
			return
		}
		if !s.cancelAllowed(o.Symbol) {
			resp.RejectReason = "symbol_not_cancelable"
			return
		}
		if s.closeAllBlocksLocked(o.UserID, o.Symbol) {
			resp.RejectReason = "close_all_in_progress"
			return
		}
		if s.amendFor(o.OrderID) != nil {
			resp.RejectReason = "amend_in_progress"
			return
		}
		// qty is the new TOTAL intent — amending to at-or-below what already
		// executed is a no-op the client should observe as such, not a
		// zero-qty replacement.
		if newQty.Cmp(o.FilledQty) <= 0 {
			resp.RejectReason = "qty_not_above_filled"
			return
		}
		pa := &pendingAmend{
			UserID: o.UserID, Symbol: o.Symbol, OldOrderID: o.OrderID,
			NewOrderID: s.nextID(), NewPrice: newPrice, NewQty: newQty,
		}
		s.registerAmend(pa)
		s.emitAmend(pa, eventpb.PerpAmendEvent_STATE_REQUESTED, "", zero)
		if err := s.dispatch.DispatchCancel(o.Symbol, s.cancelOrderEvent(o)); err != nil {
			s.takeAmend(pa.OldOrderID)
			s.emitAmend(pa, eventpb.PerpAmendEvent_STATE_FAILED, "cancel_dispatch_failed", zero)
			resp.RejectReason = "dispatch_failed"
			return
		}
		old := o.Status
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_CANCEL
		o.UpdatedMs = s.now()
		s.emitOrderStatus(o, old, o.Status)
		resp.Accepted = true
		resp.NewOrderId = pa.NewOrderID
	})
	return resp, nil
}

// amendOnTerminalLocked is the §2 continuation: the old order just reached a
// terminal status (its IM already released, record retired). Caller holds
// the user's seq lock.
func (s *Service) amendOnTerminalLocked(o *Order) {
	pa := s.takeAmend(o.OrderID)
	if pa == nil {
		return
	}
	remaining := pa.NewQty.Sub(o.FilledQty)
	if remaining.Sign() <= 0 {
		s.emitAmend(pa, eventpb.PerpAmendEvent_STATE_ALREADY_FILLED, "", zero)
		return
	}
	if s.closeAllBlocksLocked(o.UserID, o.Symbol) {
		s.emitAmend(pa, eventpb.PerpAmendEvent_STATE_FAILED, "close_all_in_progress", zero)
		return
	}
	// Full re-admission at placement time (margin gate runs against the
	// post-cancel state). Everything but price/qty carries over from the old
	// order; ReqLev 0 re-reads the config, which is pinned while the old
	// order was live (SetPositionLeverage rejects under active orders) and
	// cannot have changed inside this critical section.
	sp := orderSpec{
		User: o.UserID, Symbol: o.Symbol, ClientID: o.ClientID,
		Side: o.Side, Type: o.Type, TIF: o.TIF,
		Price: pa.NewPrice, Qty: remaining, ReqLev: zero,
		ReduceOnly: o.ReduceOnly, PosIdx: o.PositionIdx,
	}
	if _, reason := s.placeOrderLocked(sp, pa.NewOrderID); reason != "" {
		s.emitAmend(pa, eventpb.PerpAmendEvent_STATE_FAILED, reason, zero)
		return
	}
	s.emitAmend(pa, eventpb.PerpAmendEvent_STATE_COMPLETED, "", remaining)
}

// emitAmend journals one amend state transition (ADR-0078 §2).
func (s *Service) emitAmend(pa *pendingAmend, state eventpb.PerpAmendEvent_State, reason string, placedQty dec.Decimal) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Amend{Amend: &eventpb.PerpAmendEvent{
			UserId: pa.UserID, Symbol: pa.Symbol,
			OldOrderId: pa.OldOrderID, NewOrderId: pa.NewOrderID,
			NewPrice: pa.NewPrice.String(), NewQty: pa.NewQty.String(),
			State: state, Reason: reason, PlacedQty: placedQty.String(),
		}},
	})
}

// onOrderTerminalLocked runs the ADR-0078 event-driven continuations after
// an order reached a terminal status and was retired. Order matters: the
// close-all bookkeeping first (it may flip the scope into PLACING), then the
// amend continuation (whose placement re-checks the close-all guard and
// fails cleanly if the scope is owned). Caller holds the user's seq lock.
func (s *Service) onOrderTerminalLocked(o *Order) {
	s.closeAllOnTerminalLocked(o)
	s.amendOnTerminalLocked(o)
}

// --- batch + cancel-all (ADR-0078 §3) -----------------------------------------

// BatchPlaceOrders runs each item through the full PlaceOrder path
// (admission, dedup, journal) in submission order — best-effort per-item
// results, no transactionality. batch_id is a correlation key only.
func (s *Service) BatchPlaceOrders(req *perprpc.BatchPlaceOrdersRequest) (*perprpc.BatchPlaceOrdersResponse, error) {
	if req.GetUserId() == 0 {
		return nil, errInvalid("user_id required")
	}
	if n := len(req.GetItems()); n == 0 || n > maxBatchItems {
		return nil, errInvalid("batch size must be in [1, 20]")
	}
	resp := &perprpc.BatchPlaceOrdersResponse{BatchId: req.GetBatchId()}
	for _, item := range req.GetItems() {
		if item.GetUserId() != 0 && item.GetUserId() != req.GetUserId() {
			resp.Items = append(resp.Items, &perprpc.PlaceOrderResponse{
				ClientOrderId: item.GetClientOrderId(), Accepted: false,
				RejectReason: "user_mismatch", ReceivedTsUnixMs: s.now(),
			})
			continue
		}
		item.UserId = req.GetUserId()
		out, err := s.PlaceOrder(item)
		if err != nil {
			// Shape errors stay per-item in a batch: the sibling items must
			// still get their shot (best-effort semantics).
			out = &perprpc.PlaceOrderResponse{
				ClientOrderId: item.GetClientOrderId(), Accepted: false,
				RejectReason: err.Error(), ReceivedTsUnixMs: s.now(),
			}
		}
		resp.Items = append(resp.Items, out)
	}
	return resp, nil
}

// BatchCancelOrders cancels each id best-effort with per-item outcomes.
func (s *Service) BatchCancelOrders(req *perprpc.BatchCancelOrdersRequest) (*perprpc.BatchCancelOrdersResponse, error) {
	if req.GetUserId() == 0 {
		return nil, errInvalid("user_id required")
	}
	if n := len(req.GetOrderIds()); n == 0 || n > maxBatchItems {
		return nil, errInvalid("batch size must be in [1, 20]")
	}
	resp := &perprpc.BatchCancelOrdersResponse{BatchId: req.GetBatchId()}
	for _, id := range req.GetOrderIds() {
		item := &perprpc.CancelOrderResponse{OrderId: id}
		s.seq.do(req.GetUserId(), func() {
			s.cancelOrderLocked(req.GetUserId(), id, item)
		})
		resp.Items = append(resp.Items, item)
	}
	return resp, nil
}

// CancelAllOrders cancels every active order, optionally scoped to one
// symbol (ADR-0078 §3 "cancel all / by symbol"). One sequencer entry covers
// the whole pass so the targeted set cannot change mid-collection.
func (s *Service) CancelAllOrders(req *perprpc.CancelAllOrdersRequest) (*perprpc.CancelAllOrdersResponse, error) {
	if req.GetUserId() == 0 {
		return nil, errInvalid("user_id required")
	}
	resp := &perprpc.CancelAllOrdersResponse{}
	s.seq.do(req.GetUserId(), func() {
		for _, o := range s.activeOrdersOf(req.GetUserId(), req.GetSymbol()) {
			item := &perprpc.CancelOrderResponse{OrderId: o.OrderID}
			s.cancelOrderLocked(req.GetUserId(), o.OrderID, item)
			resp.Items = append(resp.Items, item)
		}
	})
	return resp, nil
}

// activeOrdersOf snapshots the user's non-terminal orders (optional symbol
// filter) in a stable order. Runs inside the user's sequencer.
func (s *Service) activeOrdersOf(user uint64, symbol string) []*Order {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Order
	for _, o := range s.orders {
		if o.UserID != user || isTerminal(o.Status) {
			continue
		}
		if symbol != "" && o.Symbol != symbol {
			continue
		}
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OrderID < out[j].OrderID })
	return out
}
