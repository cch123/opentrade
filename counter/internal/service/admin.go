package service

import (
	"context"
	"errors"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/engine"
)

// ErrAdminCancelFilterEmpty is returned when neither user_id nor symbol is
// supplied. A completely empty filter would cancel every open order on the
// shard — we require an explicit commitment to that scope.
var ErrAdminCancelFilterEmpty = errors.New("service: admin cancel requires user_id or symbol")

// AdminCancelFilter narrows the set of orders to cancel. Empty strings
// mean "match any".
type AdminCancelFilter struct {
	UserID string
	Symbol string
}

// AdminCancelResult is the Service-level response for bulk cancel calls.
// Shared by AdminCancelOrders (operator path) and CancelMyOrders (user
// path) since the post-filter execution is identical.
type AdminCancelResult struct {
	Cancelled uint32
	Skipped   uint32
}

// AdminCancelOrders walks the shard's OrderStore, selects active orders
// matching the filter, and routes each one through the standard
// CancelOrder path (per-user sequencer + Kafka txn).
//
// Failures on individual cancellations are logged and counted as Skipped;
// the overall call only returns an error on filter validation or when
// PlaceOrder deps are not wired.
func (s *Service) AdminCancelOrders(ctx context.Context, filter AdminCancelFilter) (*AdminCancelResult, error) {
	if filter.UserID == "" && filter.Symbol == "" {
		return nil, ErrAdminCancelFilterEmpty
	}
	// When UserID is given, verify shard ownership before touching state.
	// Without a user filter, the call iterates every user owned by this
	// shard — OwnsUser is checked implicitly inside the loop.
	if filter.UserID != "" && !s.OwnsUser(filter.UserID) {
		return nil, ErrWrongShard
	}
	return s.cancelOrdersMatching(ctx, filter)
}

// CancelMyOrders is the user-facing bulk cancel. user_id is required and
// must hash to this shard; symbol is optional (empty = every active order
// owned by the user). Shares the post-filter execution with
// AdminCancelOrders but enforces a stricter precondition (no admin-style
// "symbol only" scope).
func (s *Service) CancelMyOrders(ctx context.Context, userID, symbol string) (*AdminCancelResult, error) {
	if userID == "" {
		return nil, ErrMissingUserID
	}
	if !s.OwnsUser(userID) {
		return nil, ErrWrongShard
	}
	return s.cancelOrdersMatching(ctx, AdminCancelFilter{UserID: userID, Symbol: symbol})
}

// cancelOrdersMatching is the shared execution body: snapshot the order
// store, narrow by filter, then run the standard CancelOrder path on each
// match. Callers are responsible for validating the filter shape and
// shard ownership before invoking.
func (s *Service) cancelOrdersMatching(ctx context.Context, filter AdminCancelFilter) (*AdminCancelResult, error) {
	if s.txn == nil {
		return nil, ErrOrderDepsNotConfigured
	}

	snapshot := s.state.Orders().All() // clone slice

	targets := make([]*engine.Order, 0, len(snapshot))
	var skipped uint32
	for _, o := range snapshot {
		if filter.UserID != "" && o.UserID != filter.UserID {
			continue
		}
		if filter.Symbol != "" && o.Symbol != filter.Symbol {
			continue
		}
		// Re-verify ownership when iterating all users (no user filter):
		// a cross-user symbol cancel must not reach into foreign shards.
		if filter.UserID == "" && !s.OwnsUser(o.UserID) {
			continue
		}
		if o.Status.IsTerminal() || o.Status == engine.OrderStatusPendingCancel {
			skipped++
			continue
		}
		targets = append(targets, o)
	}

	var cancelled uint32
	for _, o := range targets {
		res, err := s.CancelOrder(ctx, CancelOrderRequest{UserID: o.UserID, OrderID: o.ID})
		if err != nil {
			s.logger.Warn("bulk cancel: per-order CancelOrder failed",
				zap.String("user", o.UserID),
				zap.Uint64("order", o.ID),
				zap.Error(err))
			skipped++
			continue
		}
		if res.Accepted {
			cancelled++
		} else {
			skipped++
		}
	}
	return &AdminCancelResult{Cancelled: cancelled, Skipped: skipped}, nil
}
