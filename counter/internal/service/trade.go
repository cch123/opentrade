package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/pkg/dec"
)

// ErrUnknownPayload is surfaced for trade-event records whose oneof payload
// we do not recognize (forward-compat).
var ErrUnknownPayload = errors.New("trade consumer: unknown payload")

// HandleTradeEvent applies a trade-event produced by Match to the Counter
// state. The consumer is at-least-once; per ADR-0048 backlog item 2 the
// handler guards against replay using per-(user, symbol) match_seq carried
// in evt.Meta.SeqId. A matchSeq of 0 (unset — legacy fixtures / cold path)
// bypasses the guard so in-process tests without Meta still exercise the
// business logic.
func (s *Service) HandleTradeEvent(ctx context.Context, evt *eventpb.TradeEvent) error {
	if evt == nil {
		return errors.New("nil event")
	}
	matchSeq := uint64(0)
	if evt.Meta != nil {
		matchSeq = evt.Meta.SeqId
	}
	switch p := evt.Payload.(type) {
	case *eventpb.TradeEvent_Accepted:
		return s.handleAccepted(ctx, p.Accepted, matchSeq)
	case *eventpb.TradeEvent_Rejected:
		return s.handleRejected(ctx, p.Rejected, matchSeq)
	case *eventpb.TradeEvent_Trade:
		return s.handleTrade(ctx, p.Trade, matchSeq)
	case *eventpb.TradeEvent_Cancelled:
		return s.handleCancelled(ctx, p.Cancelled, matchSeq)
	case *eventpb.TradeEvent_Expired:
		return s.handleExpired(ctx, p.Expired, matchSeq)
	default:
		return ErrUnknownPayload
	}
}

// matchSeqDuplicate reports whether a non-zero matchSeq has already been
// applied for (user, symbol). Must be called inside the user's sequencer
// fn so the read + advance pair is atomic.
func matchSeqDuplicate(acc *engine.Account, symbol string, matchSeq uint64) bool {
	if matchSeq == 0 {
		return false
	}
	return matchSeq <= acc.LastMatchSeq(symbol)
}

// -----------------------------------------------------------------------------
// Individual handlers
// -----------------------------------------------------------------------------

func (s *Service) handleAccepted(ctx context.Context, a *eventpb.OrderAccepted, matchSeq uint64) error {
	if !s.OwnsUser(a.UserId) {
		s.logForeignSkip("accepted", a.UserId, a.OrderId)
		return nil
	}
	_, err := s.seq.Execute(a.UserId, func(seqID uint64) (any, error) {
		acc := s.state.Account(a.UserId)
		if matchSeqDuplicate(acc, a.Symbol, matchSeq) {
			return nil, nil // ADR-0048 backlog: already seen this match_seq
		}
		o := s.state.Orders().Get(a.OrderId)
		if o == nil || o.Status != engine.OrderStatusPendingNew {
			acc.AdvanceMatchSeq(a.Symbol, matchSeq)
			return nil, nil // already past PENDING_NEW or not ours
		}
		oldStatus := o.Status
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusNew, time.Now().UnixMilli()); err != nil {
			return nil, err
		}
		if err := s.emitStatus(ctx, seqID, o, oldStatus, engine.OrderStatusNew, 0); err != nil {
			return nil, err
		}
		acc.AdvanceMatchSeq(a.Symbol, matchSeq)
		return nil, nil
	})
	return err
}

func (s *Service) handleRejected(ctx context.Context, r *eventpb.OrderRejected, matchSeq uint64) error {
	if !s.OwnsUser(r.UserId) {
		s.logForeignSkip("rejected", r.UserId, r.OrderId)
		return nil
	}
	_, err := s.seq.Execute(r.UserId, func(seqID uint64) (any, error) {
		acc := s.state.Account(r.UserId)
		if matchSeqDuplicate(acc, r.Symbol, matchSeq) {
			return nil, nil
		}
		o := s.state.Orders().Get(r.OrderId)
		if o == nil || o.Status.IsTerminal() {
			acc.AdvanceMatchSeq(r.Symbol, matchSeq)
			return nil, nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return nil, err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusRejected, time.Now().UnixMilli()); err != nil {
			return nil, err
		}
		if err := s.emitStatus(ctx, seqID, o, oldStatus, engine.OrderStatusRejected, r.Reason); err != nil {
			return nil, err
		}
		acc.AdvanceMatchSeq(r.Symbol, matchSeq)
		return nil, nil
	})
	return err
}

func (s *Service) handleTrade(ctx context.Context, t *eventpb.Trade, matchSeq uint64) error {
	makerFilledAfter, err := dec.Parse(t.MakerFilledQtyAfter)
	if err != nil {
		return fmt.Errorf("parse maker_filled_qty_after: %w", err)
	}
	takerFilledAfter, err := dec.Parse(t.TakerFilledQtyAfter)
	if err != nil {
		return fmt.Errorf("parse taker_filled_qty_after: %w", err)
	}
	price, err := dec.Parse(t.Price)
	if err != nil {
		return fmt.Errorf("parse trade price: %w", err)
	}
	qty, err := dec.Parse(t.Qty)
	if err != nil {
		return fmt.Errorf("parse trade qty: %w", err)
	}
	ti := engine.TradeInput{
		TradeID:             t.TradeId,
		Symbol:              t.Symbol,
		Price:               price,
		Qty:                 qty,
		MakerUserID:         t.MakerUserId,
		MakerOrderID:        t.MakerOrderId,
		TakerUserID:         t.TakerUserId,
		TakerOrderID:        t.TakerOrderId,
		TakerSide:           sideFromProto(t.TakerSide),
		MakerFilledQtyAfter: makerFilledAfter,
		TakerFilledQtyAfter: takerFilledAfter,
	}

	// Self-trade (same user on both sides): the two applyPartyViaSequencer
	// calls would share one account and the match_seq guard on
	// (user, symbol) would cause the second call to short-circuit, dropping
	// half of the settlement. Route through a merged path that applies both
	// parties inside a single sequencer pass with a single match_seq advance.
	if ti.MakerUserID == ti.TakerUserID {
		if err := s.applySelfTrade(ctx, ti, matchSeq); err != nil {
			return fmt.Errorf("self-trade settlement: %w", err)
		}
		return nil
	}

	// Apply maker side, then taker side. Each goes through its own user
	// sequencer so concurrent order flow for that user is serialized.
	// Per-party shard ownership is checked inside applyPartyViaSequencer so
	// that mixed-shard trades (maker on shard A, taker on shard B) still
	// work: each Counter shard settles only its own user.
	if err := s.applyPartyViaSequencer(ctx, ti, true, matchSeq); err != nil {
		return fmt.Errorf("maker settlement: %w", err)
	}
	if err := s.applyPartyViaSequencer(ctx, ti, false, matchSeq); err != nil {
		return fmt.Errorf("taker settlement: %w", err)
	}
	return nil
}

// applySelfTrade settles both parties of a self-trade (maker_user==taker_user)
// inside one sequencer pass. Idempotency: the (user, symbol) match_seq guard
// still covers replays because we advance it once at the end — a replay of
// the same event is dropped before doing any work.
func (s *Service) applySelfTrade(ctx context.Context, ti engine.TradeInput, matchSeq uint64) error {
	userID := ti.MakerUserID
	if !s.OwnsUser(userID) {
		s.logForeignSkip("trade-self", userID, ti.MakerOrderID)
		return nil
	}
	_, err := s.seq.Execute(userID, func(seqID uint64) (any, error) {
		acc := s.state.Account(userID)
		if matchSeqDuplicate(acc, ti.Symbol, matchSeq) {
			return nil, nil
		}
		mSet, tSet, err := engine.ComputeSettlement(s.state, ti)
		if err != nil {
			return nil, err
		}
		base, quote, _ := engine.SymbolAssets(ti.Symbol)
		// Apply maker then taker. Each call mutates the same account; the
		// engine applies deltas sequentially so the final state reflects
		// both sides.
		for _, party := range []engine.PartySettlement{mSet, tSet} {
			o := s.state.Orders().Get(party.OrderID)
			if o == nil {
				continue
			}
			// Fill-qty idempotency (layer 2): matches the guard in
			// applyPartyViaSequencer for legacy zero-seq records.
			if o.FilledQty.Cmp(party.FilledQtyAfter) >= 0 {
				continue
			}
			if err := s.state.ApplyPartySettlement(ti.Symbol, party); err != nil {
				return nil, err
			}
			baseAfter := s.state.Balance(userID, base)
			quoteAfter := s.state.Balance(userID, quote)
			evt, err := journal.BuildSettlementEvent(journal.SettlementEventInput{
				SeqID:          seqID,
				ProducerID:     s.cfg.ProducerID,
				AccountVersion: acc.Version(),
				Symbol:         ti.Symbol,
				TradeID:        ti.TradeID,
				Party:          party,
				BaseAfter:      baseAfter,
				QuoteAfter:     quoteAfter,
			})
			if err != nil {
				return nil, err
			}
			if err := s.publisher.Publish(ctx, userID, evt); err != nil {
				s.logger.Error("publish self-trade settlement",
					zap.String("user", userID),
					zap.Uint64("order_id", party.OrderID),
					zap.Error(err))
			}
		}
		acc.AdvanceMatchSeq(ti.Symbol, matchSeq)
		return nil, nil
	})
	return err
}

func (s *Service) handleCancelled(ctx context.Context, c *eventpb.OrderCancelled, matchSeq uint64) error {
	if !s.OwnsUser(c.UserId) {
		s.logForeignSkip("cancelled", c.UserId, c.OrderId)
		return nil
	}
	_, err := s.seq.Execute(c.UserId, func(seqID uint64) (any, error) {
		acc := s.state.Account(c.UserId)
		if matchSeqDuplicate(acc, c.Symbol, matchSeq) {
			return nil, nil
		}
		o := s.state.Orders().Get(c.OrderId)
		if o == nil || o.Status.IsTerminal() {
			acc.AdvanceMatchSeq(c.Symbol, matchSeq)
			return nil, nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return nil, err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusCanceled, time.Now().UnixMilli()); err != nil {
			return nil, err
		}
		if err := s.emitStatus(ctx, seqID, o, oldStatus, engine.OrderStatusCanceled, 0); err != nil {
			return nil, err
		}
		acc.AdvanceMatchSeq(c.Symbol, matchSeq)
		return nil, nil
	})
	return err
}

func (s *Service) handleExpired(ctx context.Context, e *eventpb.OrderExpired, matchSeq uint64) error {
	if !s.OwnsUser(e.UserId) {
		s.logForeignSkip("expired", e.UserId, e.OrderId)
		return nil
	}
	_, err := s.seq.Execute(e.UserId, func(seqID uint64) (any, error) {
		acc := s.state.Account(e.UserId)
		if matchSeqDuplicate(acc, e.Symbol, matchSeq) {
			return nil, nil
		}
		o := s.state.Orders().Get(e.OrderId)
		if o == nil || o.Status.IsTerminal() {
			acc.AdvanceMatchSeq(e.Symbol, matchSeq)
			return nil, nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return nil, err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusExpired, time.Now().UnixMilli()); err != nil {
			return nil, err
		}
		if err := s.emitStatus(ctx, seqID, o, oldStatus, engine.OrderStatusExpired, e.Reason); err != nil {
			return nil, err
		}
		acc.AdvanceMatchSeq(e.Symbol, matchSeq)
		return nil, nil
	})
	return err
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// applyPartyViaSequencer runs the per-party settlement under the user's
// sequencer. Idempotency has two layers:
//   - ADR-0048 match_seq guard on (user, symbol): skip if this seq is already
//     applied.
//   - Fill-qty check: belt-and-suspenders against mid-rollout / zero-seq
//     legacy records where match_seq is unset.
func (s *Service) applyPartyViaSequencer(ctx context.Context, ti engine.TradeInput, maker bool, matchSeq uint64) error {
	userID := ti.TakerUserID
	orderID := ti.TakerOrderID
	kind := "trade-taker"
	if maker {
		userID = ti.MakerUserID
		orderID = ti.MakerOrderID
		kind = "trade-maker"
	}
	if !s.OwnsUser(userID) {
		s.logForeignSkip(kind, userID, orderID)
		return nil
	}
	_, err := s.seq.Execute(userID, func(seqID uint64) (any, error) {
		acc := s.state.Account(userID)
		if matchSeqDuplicate(acc, ti.Symbol, matchSeq) {
			return nil, nil
		}
		o := s.state.Orders().Get(orderID)
		if o == nil {
			// Defensive: OwnsUser claims this user but we have no record of the
			// order. Can happen mid-rollout or if upstream shard routing drifts.
			acc.AdvanceMatchSeq(ti.Symbol, matchSeq)
			return nil, nil
		}
		// Idempotency layer 2: compare stored FilledQty with the event's
		// after-value. Covers legacy records whose Meta.SeqId is zero.
		after := ti.MakerFilledQtyAfter
		if !maker {
			after = ti.TakerFilledQtyAfter
		}
		if o.FilledQty.Cmp(after) >= 0 {
			acc.AdvanceMatchSeq(ti.Symbol, matchSeq)
			return nil, nil // already applied
		}

		mSet, tSet, err := engine.ComputeSettlement(s.state, ti)
		if err != nil {
			return nil, err
		}
		party := tSet
		if maker {
			party = mSet
		}
		if err := s.state.ApplyPartySettlement(ti.Symbol, party); err != nil {
			return nil, err
		}

		// Emit settlement event to counter-journal (best-effort, non-txn).
		// ApplyPartySettlement above has already bumped versions through
		// setBalance, so reading back picks up the post-commit values.
		base, quote, _ := engine.SymbolAssets(ti.Symbol)
		baseAfter := s.state.Balance(party.UserID, base)
		quoteAfter := s.state.Balance(party.UserID, quote)
		evt, err := journal.BuildSettlementEvent(journal.SettlementEventInput{
			SeqID:          seqID,
			ProducerID:     s.cfg.ProducerID,
			AccountVersion: acc.Version(),
			Symbol:         ti.Symbol,
			TradeID:        ti.TradeID,
			Party:          party,
			BaseAfter:      baseAfter,
			QuoteAfter:     quoteAfter,
		})
		if err != nil {
			return nil, err
		}
		if err := s.publisher.Publish(ctx, party.UserID, evt); err != nil {
			s.logger.Error("publish settlement",
				zap.Uint64("order_id", party.OrderID),
				zap.Error(err))
			// For MVP-3 we don't roll back state on publish failure; the
			// event will be re-sent on consumer restart via idempotency.
		}
		acc.AdvanceMatchSeq(ti.Symbol, matchSeq)
		return nil, nil
	})
	return err
}

// unfreezeResidual releases the still-frozen funds of a terminal order
// (cancel / reject / expire). Residual = FrozenAmount − FrozenSpent, where
// FrozenSpent is accumulated by settlement per fill (see engine.ApplyPartySettlement).
// The formula is uniform across:
//
//	limit buy:            Price * Qty    − Price * FilledQty
//	limit sell:           Qty            − FilledQty
//	market sell:          Qty            − FilledQty
//	market buy by quote:  QuoteQty       − Σ matchPrice * matchQty
//
// ADR-0035.
func (s *Service) unfreezeResidual(o *engine.Order) error {
	if o.FrozenAsset == "" {
		return nil
	}
	residual := o.FrozenAmount.Sub(o.FrozenSpent)
	if residual.Sign() <= 0 {
		return nil
	}
	b := s.state.Balance(o.UserID, o.FrozenAsset)
	b.Frozen = b.Frozen.Sub(residual)
	b.Available = b.Available.Add(residual)
	if b.Frozen.Sign() < 0 {
		return fmt.Errorf("unfreeze: frozen would be negative for %s %s", o.UserID, o.FrozenAsset)
	}
	// CommitBalance goes through setBalance → version bumps on both layers.
	s.state.CommitBalance(o.UserID, o.FrozenAsset, b)
	return nil
}

// logForeignSkip records a trade-event record that belongs to another shard
// (MVP-8: every Counter instance consumes the full trade-event topic and
// filters by user_id ownership here). Debug level — foreign traffic is the
// common case for any single shard so routine info-level logs would be spam.
func (s *Service) logForeignSkip(kind, userID string, orderID uint64) {
	s.logger.Debug("skip foreign trade-event",
		zap.String("kind", kind),
		zap.String("user_id", userID),
		zap.Uint64("order_id", orderID),
		zap.Int("shard_id", s.cfg.ShardID),
		zap.Int("total_shards", s.cfg.TotalShards))
}

// emitStatus publishes an OrderStatusEvent for downstream observers.
//
// AccountVersion is read AT emit time. Callers that just called
// unfreezeResidual will see the bumped value; pure-status transitions
// (PENDING_NEW → NEW) emit the still-stable pre-call value.
func (s *Service) emitStatus(ctx context.Context, seqID uint64, o *engine.Order, oldStatus, newStatus engine.OrderStatus, reason eventpb.RejectReason) error {
	evt := journal.BuildOrderStatusEvent(journal.OrderStatusEventInput{
		SeqID:          seqID,
		ProducerID:     s.cfg.ProducerID,
		AccountVersion: s.state.Account(o.UserID).Version(),
		UserID:         o.UserID,
		OrderID:        o.ID,
		OldStatus:      oldStatus,
		NewStatus:      newStatus,
		FilledQty:      o.FilledQty.String(),
		Reject:         reason,
	})
	return s.publisher.Publish(ctx, o.UserID, evt)
}

// sideFromProto converts the wire Side enum to the internal one. Returns
// SideBid for unknown values for safety; callers may ignore the mismatch
// since Side is used only for settlement arithmetic, which also takes the
// order's stored side.
func sideFromProto(s eventpb.Side) engine.Side {
	switch s {
	case eventpb.Side_SIDE_BUY:
		return engine.SideBid
	case eventpb.Side_SIDE_SELL:
		return engine.SideAsk
	}
	return engine.SideBid
}
