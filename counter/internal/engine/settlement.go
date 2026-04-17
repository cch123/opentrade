package engine

import (
	"errors"
	"fmt"

	"github.com/xargin/opentrade/pkg/dec"
)

// TradeInput describes a single fill as produced by Match (trade-event Trade
// payload). Counter settles both parties (maker + taker).
type TradeInput struct {
	TradeID      string
	Symbol       string
	Price        dec.Decimal // maker's price (match price)
	Qty          dec.Decimal

	MakerUserID  string
	MakerOrderID uint64

	TakerUserID  string
	TakerOrderID uint64
	TakerSide    Side // side of the taker

	// Post-fill cumulative filled qty (from Match — authoritative).
	MakerFilledQtyAfter dec.Decimal
	TakerFilledQtyAfter dec.Decimal
}

// PartySettlement describes the balance deltas for one side of a trade.
// Positive deltas add, negative deltas subtract.
type PartySettlement struct {
	UserID          string
	OrderID         uint64

	BaseDelta       dec.Decimal // signed change to available base
	QuoteDelta      dec.Decimal // signed change to available quote
	FrozenBaseDelta dec.Decimal // signed change to frozen base (negative = release)
	FrozenQuoteDelta dec.Decimal // signed change to frozen quote (negative = release)

	// Updated order fields (caller applies via OrderStore.SetFilledQty +
	// UpdateStatus).
	FilledQtyAfter dec.Decimal
	StatusAfter    OrderStatus
}

// ComputeSettlement derives the balance deltas + new order status for a
// single Trade, given the in-memory view of the maker and taker orders.
//
// For MVP-3 fees are zero. Price improvement refund is applied to the taker:
// when the taker is a limit buyer whose price Pt > match price Pm, the portion
// (Pt - Pm) * X is refunded to available_quote (the unused reservation).
func ComputeSettlement(state *ShardState, ti TradeInput) (maker, taker PartySettlement, err error) {
	base, quote, err := SymbolAssets(ti.Symbol)
	if err != nil {
		return PartySettlement{}, PartySettlement{}, err
	}
	makerOrder := state.Orders().Get(ti.MakerOrderID)
	takerOrder := state.Orders().Get(ti.TakerOrderID)
	if makerOrder == nil {
		return PartySettlement{}, PartySettlement{}, fmt.Errorf("%w: maker %d", ErrOrderNotFound, ti.MakerOrderID)
	}
	if takerOrder == nil {
		return PartySettlement{}, PartySettlement{}, fmt.Errorf("%w: taker %d", ErrOrderNotFound, ti.TakerOrderID)
	}

	maker = settleMaker(makerOrder, ti, base, quote)
	taker = settleTaker(takerOrder, ti, base, quote)
	return maker, taker, nil
}

// settleMaker computes the maker's deltas. Maker always trades at its own
// price (Pm = ti.Price).
func settleMaker(o *Order, ti TradeInput, base, quote string) PartySettlement {
	quoteAmt := ti.Price.Mul(ti.Qty)
	s := PartySettlement{
		UserID:         o.UserID,
		OrderID:        o.ID,
		FilledQtyAfter: ti.MakerFilledQtyAfter,
	}
	switch o.Side {
	case SideBid: // maker bought: frozen quote, receives base
		s.FrozenQuoteDelta = quoteAmt.Neg()
		s.BaseDelta = ti.Qty
	case SideAsk: // maker sold: frozen base, receives quote
		s.FrozenBaseDelta = ti.Qty.Neg()
		s.QuoteDelta = quoteAmt
	}
	s.StatusAfter = statusAfterFill(o, s.FilledQtyAfter)
	_ = base
	_ = quote
	return s
}

// settleTaker computes the taker's deltas. If the taker is a limit order
// whose price is better than the match price, the unused reservation is
// refunded to available_quote (for buys) or left alone (for sells, since
// sell reservation is in base units and always equals qty).
func settleTaker(o *Order, ti TradeInput, base, quote string) PartySettlement {
	matchQuote := ti.Price.Mul(ti.Qty) // actual quote exchanged
	s := PartySettlement{
		UserID:         o.UserID,
		OrderID:        o.ID,
		FilledQtyAfter: ti.TakerFilledQtyAfter,
	}
	switch o.Side {
	case SideBid: // taker bought: frozen at taker's price, receives base
		// Reservation for this slice = o.Price * ti.Qty (taker's price).
		reserved := o.Price.Mul(ti.Qty)
		s.FrozenQuoteDelta = reserved.Neg()
		// Refund the price-improvement difference back to available quote.
		if o.Price.Cmp(ti.Price) > 0 {
			refund := o.Price.Sub(ti.Price).Mul(ti.Qty)
			s.QuoteDelta = refund
		}
		s.BaseDelta = ti.Qty
	case SideAsk: // taker sold: frozen in base, receives quote at match price
		s.FrozenBaseDelta = ti.Qty.Neg()
		s.QuoteDelta = matchQuote
	}
	s.StatusAfter = statusAfterFill(o, s.FilledQtyAfter)
	_ = base
	_ = quote
	return s
}

// statusAfterFill chooses the new internal status for an order given its
// post-fill cumulative qty. Fully-filled orders become FILLED; otherwise
// PARTIALLY_FILLED (or PENDING_CANCEL stays the same — caller handles that).
func statusAfterFill(o *Order, filledAfter dec.Decimal) OrderStatus {
	if filledAfter.Cmp(o.Qty) >= 0 {
		return OrderStatusFilled
	}
	// PENDING_CANCEL + partial fill keeps the pending state so the eventual
	// CANCELED includes the partial fills.
	if o.Status == OrderStatusPendingCancel {
		return OrderStatusPendingCancel
	}
	return OrderStatusPartiallyFilled
}

// ApplyPartySettlement mutates the relevant account balance and order state
// atomically (for a single user). Caller ensures per-user serialization
// (UserSequencer or external lock).
func (s *ShardState) ApplyPartySettlement(symbol string, p PartySettlement) error {
	base, quote, err := SymbolAssets(symbol)
	if err != nil {
		return err
	}

	acc := s.Account(p.UserID)
	if !dec.IsZero(p.BaseDelta) || !dec.IsZero(p.FrozenBaseDelta) {
		b := acc.Balance(base)
		if !dec.IsZero(p.BaseDelta) {
			b.Available = b.Available.Add(p.BaseDelta)
		}
		if !dec.IsZero(p.FrozenBaseDelta) {
			b.Frozen = b.Frozen.Add(p.FrozenBaseDelta)
		}
		if b.Available.Sign() < 0 || b.Frozen.Sign() < 0 {
			return fmt.Errorf("settlement produced negative balance for %s %s: %+v", p.UserID, base, b)
		}
		acc.PutForRestore(base, b)
	}
	if !dec.IsZero(p.QuoteDelta) || !dec.IsZero(p.FrozenQuoteDelta) {
		b := acc.Balance(quote)
		if !dec.IsZero(p.QuoteDelta) {
			b.Available = b.Available.Add(p.QuoteDelta)
		}
		if !dec.IsZero(p.FrozenQuoteDelta) {
			b.Frozen = b.Frozen.Add(p.FrozenQuoteDelta)
		}
		if b.Available.Sign() < 0 || b.Frozen.Sign() < 0 {
			return fmt.Errorf("settlement produced negative balance for %s %s: %+v", p.UserID, quote, b)
		}
		acc.PutForRestore(quote, b)
	}
	if _, err := s.Orders().SetFilledQty(p.OrderID, p.FilledQtyAfter, 0); err != nil {
		return err
	}
	if _, err := s.Orders().UpdateStatus(p.OrderID, p.StatusAfter, 0); err != nil {
		return err
	}
	return nil
}

// UnfreezeOnTerminal releases remaining frozen funds for an order that ended
// without trading all of its Qty (cancel / reject / expire). It computes the
// unused reservation based on FrozenAsset and FrozenAmount originally frozen
// at placement minus what was already released by fills.
//
// For MVP-3 simplification we track FrozenAmount at the order level as the
// ORIGINAL frozen; after fills, the unused residual is Original - (consumed).
// Consumed depends on side: buy consumed = Σ ti.Price * ti.Qty (per-fill);
// sell consumed = Σ ti.Qty.
func (s *ShardState) UnfreezeOnTerminal(o *Order, consumedFromFrozen dec.Decimal) error {
	if o.FrozenAsset == "" {
		return nil
	}
	residual := o.FrozenAmount.Sub(consumedFromFrozen)
	if residual.Sign() <= 0 {
		return nil
	}
	acc := s.Account(o.UserID)
	b := acc.Balance(o.FrozenAsset)
	b.Frozen = b.Frozen.Sub(residual)
	b.Available = b.Available.Add(residual)
	if b.Frozen.Sign() < 0 {
		return errors.New("UnfreezeOnTerminal: frozen would be negative")
	}
	acc.PutForRestore(o.FrozenAsset, b)
	return nil
}
