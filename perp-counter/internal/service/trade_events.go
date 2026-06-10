package service

// trade_events.go is the consume side of the perp-counter ↔ Match Kafka loop
// (ADR-0068 §1). Match emits a TradeEvent stream (perp-trade-event topic) whose
// payloads are NOT all fills: an order can be accepted onto the book, rejected,
// cancelled, or expired. The settlement path (Trade) lives in service.go; this
// file handles the order-lifecycle payloads, whose key correctness duty is
// releasing the order's still-held initial margin so a reject / cancel / expire
// does not leak the reservation taken at PlaceOrder time (ADR-0068 §4).
//
// Idempotency: these handlers key off order state, not the position's
// match_seq watermark (which guards fills). A reject / cancel / expire moves the
// order to a terminal status and evicts it, so a redelivered record finds no
// order and is a no-op. Accept only fires on the PENDING_NEW → NEW edge.
// Within a single order's lifetime every event carries the same user_id, so
// Match's per-symbol ordering reaches this shard in order on one partition —
// a lifecycle event can never overtake an earlier fill for the same order.

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// HandleTradeEvent is the perp-trade-event consumer entry point. It routes a
// decoded TradeEvent to the matching per-user serialized handler and records
// the partition's consumed offset for the snapshot binding (ADR-0048 #5).
// partition/offset come from the Kafka record; offset is the record's own
// position (the consumer stores offset+1 as the resume point).
func (s *Service) HandleTradeEvent(evt *eventpb.TradeEvent, partition int32, offset int64) {
	if evt == nil {
		return
	}
	// Hold the capture barrier so a snapshot cannot read state + offsets while
	// this record is being applied (ADR-0048 atomic offset binding).
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()
	matchSeq := evt.GetMatchSeqId()
	switch p := evt.Payload.(type) {
	case *eventpb.TradeEvent_Trade:
		s.HandleTrade(p.Trade, matchSeq)
	case *eventpb.TradeEvent_Accepted:
		s.handleAccepted(p.Accepted)
	case *eventpb.TradeEvent_Rejected:
		s.handleRejected(p.Rejected)
	case *eventpb.TradeEvent_Cancelled:
		s.handleCancelled(p.Cancelled)
	case *eventpb.TradeEvent_Expired:
		s.handleExpired(p.Expired)
	default:
		// Unknown payload kind — forward-compat skip.
	}
	s.recordOffset(partition, offset)
}

// handleAccepted moves a resting order PENDING_NEW → NEW (Match acknowledged it
// onto the book). No margin moves — the IM was reserved at PlaceOrder. Replays
// (status already past PENDING_NEW) are no-ops.
func (s *Service) handleAccepted(a *eventpb.OrderAccepted) {
	user := a.GetUserId()
	if user == 0 {
		return
	}
	s.seq.do(user, func() {
		o := s.getOrder(a.GetOrderId())
		if o == nil || o.UserID != user {
			return
		}
		if o.Status != eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW {
			return // already advanced (idempotent)
		}
		old := o.Status
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW
		o.UpdatedMs = s.now()
		s.emitOrderStatus(o, old, o.Status)
	})
}

// handleRejected terminates an order Match refused (e.g. post-only would take,
// symbol halted) and releases its full remaining reserved IM.
func (s *Service) handleRejected(r *eventpb.OrderRejected) {
	user := r.GetUserId()
	if user == 0 {
		return
	}
	s.seq.do(user, func() {
		o := s.getOrder(r.GetOrderId())
		if o == nil || o.UserID != user || isTerminal(o.Status) {
			return
		}
		old := o.Status
		s.releaseRemainingIM(o)
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_REJECTED
		o.UpdatedMs = s.now()
		s.emitOrderStatusReason(o, old, o.Status, r.GetReason())
		s.clearLiquidationIfAny(o.OrderID) // re-arm if this was a bankruptcy order
		s.delOrder(o.OrderID)
	})
}

// handleCancelled terminates an order whose cancel Match confirmed, releasing
// the IM still held against the unfilled remainder.
func (s *Service) handleCancelled(c *eventpb.OrderCancelled) {
	user := c.GetUserId()
	if user == 0 {
		return
	}
	s.seq.do(user, func() {
		o := s.getOrder(c.GetOrderId())
		if o == nil || o.UserID != user || isTerminal(o.Status) {
			return
		}
		old := o.Status
		s.releaseRemainingIM(o)
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_CANCELED
		o.UpdatedMs = s.now()
		s.emitOrderStatus(o, old, o.Status)
		s.clearLiquidationIfAny(o.OrderID)
		s.delOrder(o.OrderID)
	})
}

// handleExpired terminates the unfilled remainder of an IOC/FOK order, releasing
// the IM held against it.
func (s *Service) handleExpired(e *eventpb.OrderExpired) {
	user := e.GetUserId()
	if user == 0 {
		return
	}
	s.seq.do(user, func() {
		o := s.getOrder(e.GetOrderId())
		if o == nil || o.UserID != user || isTerminal(o.Status) {
			return
		}
		old := o.Status
		s.releaseRemainingIM(o)
		o.Status = eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_EXPIRED
		o.UpdatedMs = s.now()
		s.emitOrderStatusReason(o, old, o.Status, e.GetReason())
		s.clearLiquidationIfAny(o.OrderID)
		s.delOrder(o.OrderID)
	})
}

// releaseRemainingIM returns an order's still-held initial margin to the
// wallet's available balance, from the bucket matching the order's margin
// mode (ADR-0074). afterFill has already drained the part converted to
// position margin (isolated) or released per filled proportion (cross), so
// this releases only the unfilled remainder's hold. Caller holds the user's
// seq lock.
func (s *Service) releaseRemainingIM(o *Order) {
	if o.ReservedIM.Sign() > 0 {
		if o.Mode == perpstate.MarginCross {
			s.eng.ReleaseCross(o.UserID, o.ReservedIM)
		} else {
			s.eng.Release(o.UserID, o.ReservedIM)
		}
		o.ReservedIM = zero
	}
}

// recordOffset stores the next-to-consume offset for a perp-trade-event
// partition. The snapshot pipeline (ADR-0048 #5, a later milestone) binds these
// to the engine state so recovery resumes the consumer at the right position.
func (s *Service) recordOffset(partition int32, offset int64) {
	s.mu.Lock()
	s.offsets[partition] = offset + 1
	s.mu.Unlock()
}

// ConsumedOffsets returns a copy of the per-partition next-to-consume offsets
// for the perp-trade-event topic (snapshot binding / observability).
func (s *Service) ConsumedOffsets() map[int32]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int32]int64, len(s.offsets))
	for p, o := range s.offsets {
		out[p] = o
	}
	return out
}
