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
// in evt.MatchSeqId. A matchSeq of 0 (unset — legacy fixtures / cold path)
// bypasses the guard so in-process tests without it still exercise the
// business logic.
//
// This is the legacy synchronous entry point: each per-user fn is
// enqueued via seq.Execute and blocks HandleTradeEvent until completion.
// Used by tests, admin tooling, and anywhere a caller needs the return
// error to reflect fn-level outcome. ADR-0060 adds
// HandleTradeEventAsync for the fire-and-forget consumer loop path.
func (s *Service) HandleTradeEvent(ctx context.Context, evt *eventpb.TradeEvent) error {
	if evt == nil {
		return errors.New("nil event")
	}
	matchSeq := evt.MatchSeqId
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

// HandleTradeEventAsync is the ADR-0060 fire-and-forget sibling of
// HandleTradeEvent. Two callbacks are passed in:
//
//   - onCount is invoked exactly once, synchronously, BEFORE any fn is
//     submitted to the sequencer. Its int32 argument is the number of
//     fns about to be submitted (0, 1, or 2). Callers use this to
//     pre-register the trade-event in pendingList so cb's MarkFnDone
//     cannot race with Enqueue.
//   - cb is invoked from the drain goroutine after each fn completes
//     (0, 1, or 2 times — matches the onCount value). cb MUST be
//     short and non-blocking (typically pendingList.MarkFnDone +
//     advancer signal).
//
// Contract:
//   - onCount is always called, even on 0 (no owned parties / parse
//     error / unknown payload). When onCount(0), cb is never called.
//   - When onCount(n>0), cb is called exactly n times across one or
//     more drain goroutines.
//   - onCount returns before any SubmitAsync happens, so the caller's
//     pendingList state setup is visible to every future cb invocation.
//
// Same match_seq idempotency and OwnsUser gating as HandleTradeEvent —
// only the submission semantics differ.
func (s *Service) HandleTradeEventAsync(ctx context.Context, evt *eventpb.TradeEvent, onCount func(count int32), cb func(err error)) {
	if evt == nil {
		onCount(0)
		return
	}
	matchSeq := evt.MatchSeqId
	switch p := evt.Payload.(type) {
	case *eventpb.TradeEvent_Accepted:
		s.handleAcceptedAsync(ctx, p.Accepted, matchSeq, onCount, cb)
	case *eventpb.TradeEvent_Rejected:
		s.handleRejectedAsync(ctx, p.Rejected, matchSeq, onCount, cb)
	case *eventpb.TradeEvent_Trade:
		s.handleTradeAsync(ctx, p.Trade, matchSeq, onCount, cb)
	case *eventpb.TradeEvent_Cancelled:
		s.handleCancelledAsync(ctx, p.Cancelled, matchSeq, onCount, cb)
	case *eventpb.TradeEvent_Expired:
		s.handleExpiredAsync(ctx, p.Expired, matchSeq, onCount, cb)
	default:
		onCount(0)
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

// buildAcceptedFn captures the per-user fn body for OrderAccepted. Caller
// must have confirmed s.OwnsUser(a.UserId) before submitting this fn;
// the fn itself assumes ownership.
func (s *Service) buildAcceptedFn(ctx context.Context, a *eventpb.OrderAccepted, matchSeq uint64) func(uint64) error {
	return func(counterSeq uint64) error {
		acc := s.state.Account(a.UserId)
		if matchSeqDuplicate(acc, a.Symbol, matchSeq) {
			return nil // ADR-0048 backlog: already seen this match_seq
		}
		o := s.state.Orders().Get(a.OrderId)
		if o == nil || o.Status != engine.OrderStatusPendingNew {
			acc.AdvanceMatchSeq(a.Symbol, matchSeq)
			return nil // already past PENDING_NEW or not ours
		}
		oldStatus := o.Status
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusNew, time.Now().UnixMilli()); err != nil {
			return err
		}
		if err := s.emitStatus(ctx, counterSeq, o, oldStatus, engine.OrderStatusNew, 0); err != nil {
			return err
		}
		acc.AdvanceMatchSeq(a.Symbol, matchSeq)
		return nil
	}
}

func (s *Service) handleAccepted(ctx context.Context, a *eventpb.OrderAccepted, matchSeq uint64) error {
	if !s.OwnsUser(a.UserId) {
		s.logForeignSkip("accepted", a.UserId, a.OrderId)
		return nil
	}
	fn := s.buildAcceptedFn(ctx, a, matchSeq)
	_, err := s.seq.Execute(a.UserId, func(cs uint64) (any, error) {
		return nil, fn(cs)
	})
	return err
}

// handleAcceptedAsync — ADR-0060 async wrapper. Calls onCount(0 or 1)
// before any SubmitAsync so the caller's pendingList registration wins
// the race against cb invocation. cb runs on the user's drain goroutine
// after fn completes (once if owned, never if foreign).
func (s *Service) handleAcceptedAsync(ctx context.Context, a *eventpb.OrderAccepted, matchSeq uint64, onCount func(int32), cb func(err error)) {
	if !s.OwnsUser(a.UserId) {
		s.logForeignSkip("accepted", a.UserId, a.OrderId)
		onCount(0)
		return
	}
	onCount(1)
	s.seq.SubmitAsync(a.UserId, s.buildAcceptedFn(ctx, a, matchSeq), cb)
}

// buildRejectedFn — see buildAcceptedFn's contract.
func (s *Service) buildRejectedFn(ctx context.Context, r *eventpb.OrderRejected, matchSeq uint64) func(uint64) error {
	return func(counterSeq uint64) error {
		acc := s.state.Account(r.UserId)
		if matchSeqDuplicate(acc, r.Symbol, matchSeq) {
			return nil
		}
		o := s.state.Orders().Get(r.OrderId)
		if o == nil || o.Status.IsTerminal() {
			acc.AdvanceMatchSeq(r.Symbol, matchSeq)
			return nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusRejected, time.Now().UnixMilli()); err != nil {
			return err
		}
		if err := s.emitStatus(ctx, counterSeq, o, oldStatus, engine.OrderStatusRejected, r.Reason); err != nil {
			return err
		}
		acc.AdvanceMatchSeq(r.Symbol, matchSeq)
		return nil
	}
}

func (s *Service) handleRejected(ctx context.Context, r *eventpb.OrderRejected, matchSeq uint64) error {
	if !s.OwnsUser(r.UserId) {
		s.logForeignSkip("rejected", r.UserId, r.OrderId)
		return nil
	}
	fn := s.buildRejectedFn(ctx, r, matchSeq)
	_, err := s.seq.Execute(r.UserId, func(cs uint64) (any, error) {
		return nil, fn(cs)
	})
	return err
}

func (s *Service) handleRejectedAsync(ctx context.Context, r *eventpb.OrderRejected, matchSeq uint64, onCount func(int32), cb func(err error)) {
	if !s.OwnsUser(r.UserId) {
		s.logForeignSkip("rejected", r.UserId, r.OrderId)
		onCount(0)
		return
	}
	onCount(1)
	s.seq.SubmitAsync(r.UserId, s.buildRejectedFn(ctx, r, matchSeq), cb)
}

// parseTradeInput decodes the protobuf Trade fields into the engine's
// TradeInput. Extracted so sync and async handlers share the same
// parse path.
func parseTradeInput(t *eventpb.Trade) (engine.TradeInput, error) {
	makerFilledAfter, err := dec.Parse(t.MakerFilledQtyAfter)
	if err != nil {
		return engine.TradeInput{}, fmt.Errorf("parse maker_filled_qty_after: %w", err)
	}
	takerFilledAfter, err := dec.Parse(t.TakerFilledQtyAfter)
	if err != nil {
		return engine.TradeInput{}, fmt.Errorf("parse taker_filled_qty_after: %w", err)
	}
	price, err := dec.Parse(t.Price)
	if err != nil {
		return engine.TradeInput{}, fmt.Errorf("parse trade price: %w", err)
	}
	qty, err := dec.Parse(t.Qty)
	if err != nil {
		return engine.TradeInput{}, fmt.Errorf("parse trade qty: %w", err)
	}
	return engine.TradeInput{
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
	}, nil
}

func (s *Service) handleTrade(ctx context.Context, t *eventpb.Trade, matchSeq uint64) error {
	ti, err := parseTradeInput(t)
	if err != nil {
		return err
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

// handleTradeAsync is the ADR-0060 counterpart of handleTrade. Calls
// onCount exactly once with the fn count (0/1/2) before any
// SubmitAsync:
//   - 0: parse error or all-foreign parties (cb never invoked)
//   - 1: self-trade with owned user, OR non-self trade with exactly
//     one party owned
//   - 2: non-self trade with both parties owned
//
// cb is invoked exactly onCount-reported times, from drain goroutines.
//
// Parse errors are logged (the trade-event is poisoned and cannot be
// retried) and produce an onCount(0) so the caller's pendingList
// advances past the offset.
func (s *Service) handleTradeAsync(ctx context.Context, t *eventpb.Trade, matchSeq uint64, onCount func(int32), cb func(err error)) {
	ti, err := parseTradeInput(t)
	if err != nil {
		s.logger.Error("handleTradeAsync: parse failed",
			zap.String("trade_id", t.TradeId), zap.Error(err))
		onCount(0)
		return
	}

	// Self-trade: merged single-fn path.
	if ti.MakerUserID == ti.TakerUserID {
		if !s.OwnsUser(ti.MakerUserID) {
			s.logForeignSkip("trade-self", ti.MakerUserID, ti.MakerOrderID)
			onCount(0)
			return
		}
		onCount(1)
		s.seq.SubmitAsync(ti.MakerUserID, s.buildSelfTradeFn(ctx, ti, matchSeq), cb)
		return
	}

	ownsMaker := s.OwnsUser(ti.MakerUserID)
	ownsTaker := s.OwnsUser(ti.TakerUserID)
	count := int32(0)
	if ownsMaker {
		count++
	}
	if ownsTaker {
		count++
	}
	onCount(count)
	if ownsMaker {
		s.seq.SubmitAsync(ti.MakerUserID, s.buildPartyFn(ctx, ti, true, matchSeq), cb)
	} else {
		s.logForeignSkip("trade-maker", ti.MakerUserID, ti.MakerOrderID)
	}
	if ownsTaker {
		s.seq.SubmitAsync(ti.TakerUserID, s.buildPartyFn(ctx, ti, false, matchSeq), cb)
	} else {
		s.logForeignSkip("trade-taker", ti.TakerUserID, ti.TakerOrderID)
	}
}

// buildSelfTradeFn — see buildAcceptedFn's contract. Settles both
// maker and taker sides in a single fn (both sides share the account).
func (s *Service) buildSelfTradeFn(ctx context.Context, ti engine.TradeInput, matchSeq uint64) func(uint64) error {
	userID := ti.MakerUserID
	return func(counterSeq uint64) error {
		acc := s.state.Account(userID)
		if matchSeqDuplicate(acc, ti.Symbol, matchSeq) {
			return nil
		}
		mSet, tSet, err := engine.ComputeSettlement(s.state, ti)
		if err != nil {
			return err
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
				return err
			}
			baseAfter := s.state.Balance(userID, base)
			quoteAfter := s.state.Balance(userID, quote)
			evt, err := journal.BuildSettlementEvent(journal.SettlementEventInput{
				CounterSeqID:   counterSeq,
				ProducerID:     s.cfg.ProducerID,
				AccountVersion: acc.Version(),
				Symbol:         ti.Symbol,
				TradeID:        ti.TradeID,
				Party:          party,
				BaseAfter:      baseAfter,
				QuoteAfter:     quoteAfter,
			})
			if err != nil {
				return err
			}
			if err := s.publisher.Publish(ctx, userID, evt); err != nil {
				s.logger.Error("publish self-trade settlement",
					zap.String("user", userID),
					zap.Uint64("order_id", party.OrderID),
					zap.Error(err))
			}
		}
		acc.AdvanceMatchSeq(ti.Symbol, matchSeq)
		return nil
	}
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
	fn := s.buildSelfTradeFn(ctx, ti, matchSeq)
	_, err := s.seq.Execute(userID, func(cs uint64) (any, error) {
		return nil, fn(cs)
	})
	return err
}

// buildCancelledFn — see buildAcceptedFn's contract.
func (s *Service) buildCancelledFn(ctx context.Context, c *eventpb.OrderCancelled, matchSeq uint64) func(uint64) error {
	return func(counterSeq uint64) error {
		acc := s.state.Account(c.UserId)
		if matchSeqDuplicate(acc, c.Symbol, matchSeq) {
			return nil
		}
		o := s.state.Orders().Get(c.OrderId)
		if o == nil || o.Status.IsTerminal() {
			acc.AdvanceMatchSeq(c.Symbol, matchSeq)
			return nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusCanceled, time.Now().UnixMilli()); err != nil {
			return err
		}
		if err := s.emitStatus(ctx, counterSeq, o, oldStatus, engine.OrderStatusCanceled, 0); err != nil {
			return err
		}
		acc.AdvanceMatchSeq(c.Symbol, matchSeq)
		return nil
	}
}

func (s *Service) handleCancelled(ctx context.Context, c *eventpb.OrderCancelled, matchSeq uint64) error {
	if !s.OwnsUser(c.UserId) {
		s.logForeignSkip("cancelled", c.UserId, c.OrderId)
		return nil
	}
	fn := s.buildCancelledFn(ctx, c, matchSeq)
	_, err := s.seq.Execute(c.UserId, func(cs uint64) (any, error) {
		return nil, fn(cs)
	})
	return err
}

func (s *Service) handleCancelledAsync(ctx context.Context, c *eventpb.OrderCancelled, matchSeq uint64, onCount func(int32), cb func(err error)) {
	if !s.OwnsUser(c.UserId) {
		s.logForeignSkip("cancelled", c.UserId, c.OrderId)
		onCount(0)
		return
	}
	onCount(1)
	s.seq.SubmitAsync(c.UserId, s.buildCancelledFn(ctx, c, matchSeq), cb)
}

// buildExpiredFn — see buildAcceptedFn's contract.
func (s *Service) buildExpiredFn(ctx context.Context, e *eventpb.OrderExpired, matchSeq uint64) func(uint64) error {
	return func(counterSeq uint64) error {
		acc := s.state.Account(e.UserId)
		if matchSeqDuplicate(acc, e.Symbol, matchSeq) {
			return nil
		}
		o := s.state.Orders().Get(e.OrderId)
		if o == nil || o.Status.IsTerminal() {
			acc.AdvanceMatchSeq(e.Symbol, matchSeq)
			return nil
		}
		oldStatus := o.Status
		if err := s.unfreezeResidual(o); err != nil {
			return err
		}
		if _, err := s.state.Orders().UpdateStatus(o.ID, engine.OrderStatusExpired, time.Now().UnixMilli()); err != nil {
			return err
		}
		if err := s.emitStatus(ctx, counterSeq, o, oldStatus, engine.OrderStatusExpired, e.Reason); err != nil {
			return err
		}
		acc.AdvanceMatchSeq(e.Symbol, matchSeq)
		return nil
	}
}

func (s *Service) handleExpired(ctx context.Context, e *eventpb.OrderExpired, matchSeq uint64) error {
	if !s.OwnsUser(e.UserId) {
		s.logForeignSkip("expired", e.UserId, e.OrderId)
		return nil
	}
	fn := s.buildExpiredFn(ctx, e, matchSeq)
	_, err := s.seq.Execute(e.UserId, func(cs uint64) (any, error) {
		return nil, fn(cs)
	})
	return err
}

func (s *Service) handleExpiredAsync(ctx context.Context, e *eventpb.OrderExpired, matchSeq uint64, onCount func(int32), cb func(err error)) {
	if !s.OwnsUser(e.UserId) {
		s.logForeignSkip("expired", e.UserId, e.OrderId)
		onCount(0)
		return
	}
	onCount(1)
	s.seq.SubmitAsync(e.UserId, s.buildExpiredFn(ctx, e, matchSeq), cb)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// buildPartyFn — fn body for one side of a normal (non-self) trade.
// Assumes OwnsUser(party_uid) == true; caller must gate on that.
func (s *Service) buildPartyFn(ctx context.Context, ti engine.TradeInput, maker bool, matchSeq uint64) func(uint64) error {
	userID := ti.TakerUserID
	orderID := ti.TakerOrderID
	if maker {
		userID = ti.MakerUserID
		orderID = ti.MakerOrderID
	}
	return func(counterSeq uint64) error {
		acc := s.state.Account(userID)
		if matchSeqDuplicate(acc, ti.Symbol, matchSeq) {
			return nil
		}
		o := s.state.Orders().Get(orderID)
		if o == nil {
			// Defensive: OwnsUser claims this user but we have no record of the
			// order. Can happen mid-rollout or if upstream shard routing drifts.
			acc.AdvanceMatchSeq(ti.Symbol, matchSeq)
			return nil
		}
		// Idempotency layer 2: compare stored FilledQty with the event's
		// after-value. Covers legacy records whose match_seq_id is zero.
		after := ti.MakerFilledQtyAfter
		if !maker {
			after = ti.TakerFilledQtyAfter
		}
		if o.FilledQty.Cmp(after) >= 0 {
			acc.AdvanceMatchSeq(ti.Symbol, matchSeq)
			return nil // already applied
		}

		mSet, tSet, err := engine.ComputeSettlement(s.state, ti)
		if err != nil {
			return err
		}
		party := tSet
		if maker {
			party = mSet
		}
		if err := s.state.ApplyPartySettlement(ti.Symbol, party); err != nil {
			return err
		}

		// Emit settlement event to counter-journal (best-effort, non-txn).
		// ApplyPartySettlement above has already bumped versions through
		// setBalance, so reading back picks up the post-commit values.
		base, quote, _ := engine.SymbolAssets(ti.Symbol)
		baseAfter := s.state.Balance(party.UserID, base)
		quoteAfter := s.state.Balance(party.UserID, quote)
		evt, err := journal.BuildSettlementEvent(journal.SettlementEventInput{
			CounterSeqID:   counterSeq,
			ProducerID:     s.cfg.ProducerID,
			AccountVersion: acc.Version(),
			Symbol:         ti.Symbol,
			TradeID:        ti.TradeID,
			Party:          party,
			BaseAfter:      baseAfter,
			QuoteAfter:     quoteAfter,
		})
		if err != nil {
			return err
		}
		if err := s.publisher.Publish(ctx, party.UserID, evt); err != nil {
			s.logger.Error("publish settlement",
				zap.Uint64("order_id", party.OrderID),
				zap.Error(err))
			// For MVP-3 we don't roll back state on publish failure; the
			// event will be re-sent on consumer restart via idempotency.
		}
		acc.AdvanceMatchSeq(ti.Symbol, matchSeq)
		return nil
	}
}

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
	fn := s.buildPartyFn(ctx, ti, maker, matchSeq)
	_, err := s.seq.Execute(userID, func(cs uint64) (any, error) {
		return nil, fn(cs)
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
func (s *Service) emitStatus(ctx context.Context, counterSeq uint64, o *engine.Order, oldStatus, newStatus engine.OrderStatus, reason eventpb.RejectReason) error {
	evt := journal.BuildOrderStatusEvent(journal.OrderStatusEventInput{
		CounterSeqID:   counterSeq,
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
