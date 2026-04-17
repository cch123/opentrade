// Package engine implements the matching algorithm on top of an orderbook.
//
// The engine is stateless: all state lives on the Book. All calls in this
// package must happen on the SymbolWorker goroutine (ADR-0016, 0019).
package engine

import (
	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/pkg/dec"
)

// Trade is the atomic result of a single match between a maker and a taker.
type Trade struct {
	Symbol string

	Price dec.Decimal // maker's price (taker may receive price improvement)
	Qty   dec.Decimal

	MakerUserID    string
	MakerOrderID   uint64
	MakerSide      orderbook.Side
	MakerRemaining dec.Decimal // after this fill

	TakerUserID    string
	TakerOrderID   uint64
	TakerSide      orderbook.Side
	TakerRemaining dec.Decimal // after this fill
}

// TakerStatus is the terminal disposition of a taker order after matching.
type TakerStatus uint8

const (
	// TakerAcceptedOnBook — no crossings; the taker was placed on the book.
	TakerAcceptedOnBook TakerStatus = 1
	// TakerFilled — fully filled.
	TakerFilled TakerStatus = 2
	// TakerPartialOnBook — partially filled; remainder rested on the book (GTC).
	TakerPartialOnBook TakerStatus = 3
	// TakerExpired — IOC remainder / market out-of-liquidity / FOK guard after
	// matching (should not happen if pre-check was correct).
	TakerExpired TakerStatus = 4
	// TakerRejected — rule violation (Post-Only would take, STP, FOK cannot
	// fully fill, etc.). No fills occurred.
	TakerRejected TakerStatus = 5
)

// STPMode configures self-trade prevention.
type STPMode uint8

const (
	// STPNone — self-trades are allowed (MVP-1 default).
	STPNone STPMode = 0
	// STPRejectTaker — if the taker would cross with any maker from the same
	// user within its cross range, the entire taker is rejected.
	STPRejectTaker STPMode = 1
)

// Result is the outcome of Match.
type Result struct {
	Status       TakerStatus
	RejectReason orderbook.RejectReason
	Trades       []Trade
}

// Match applies taker against book. On return:
//
//   - taker.Remaining reflects the unfilled quantity after matching.
//   - if Status is TakerAcceptedOnBook or TakerPartialOnBook, the taker has
//     been inserted into the book.
//   - otherwise the taker is NOT on the book.
//
// Caller should pass taker with Remaining == Qty; Match will reset it
// defensively.
func Match(book *orderbook.Book, taker *orderbook.Order, stp STPMode) Result {
	if !taker.Remaining.Equal(taker.Qty) {
		taker.Remaining = taker.Qty
	}

	// --- Pre-checks ---------------------------------------------------------

	if taker.TIF == orderbook.PostOnly {
		if wouldCross(book, taker) {
			return Result{Status: TakerRejected, RejectReason: orderbook.RejectPostOnlyWouldTake}
		}
		// PostOnly that doesn't cross rests on the book as GTC.
		taker.TIF = orderbook.GTC
	}

	if stp == STPRejectTaker && hasSelfCross(book, taker) {
		return Result{Status: TakerRejected, RejectReason: orderbook.RejectSelfTradePrevented}
	}

	if taker.TIF == orderbook.FOK {
		if !canFullyFill(book, taker) {
			return Result{Status: TakerRejected, RejectReason: orderbook.RejectFOKNotFilled}
		}
	}

	// --- Matching loop ------------------------------------------------------

	oppSide := taker.Side.Opposite()
	var trades []Trade

	for dec.IsPositive(taker.Remaining) {
		best, ok := book.Best(oppSide)
		if !ok {
			break
		}
		if !crosses(taker, best) {
			break
		}

		matchQty := dec.Min(taker.Remaining, best.Remaining)
		matchPrice := best.Price

		maker, _, err := book.Fill(best.ID, matchQty)
		if err != nil {
			// This shouldn't happen — defensive.
			break
		}
		taker.Remaining = taker.Remaining.Sub(matchQty)

		trades = append(trades, Trade{
			Symbol:         taker.Symbol,
			Price:          matchPrice,
			Qty:            matchQty,
			MakerUserID:    maker.UserID,
			MakerOrderID:   maker.ID,
			MakerSide:      maker.Side,
			MakerRemaining: maker.Remaining,
			TakerUserID:    taker.UserID,
			TakerOrderID:   taker.ID,
			TakerSide:      taker.Side,
			TakerRemaining: taker.Remaining,
		})
	}

	return finalize(book, taker, trades)
}

func finalize(book *orderbook.Book, taker *orderbook.Order, trades []Trade) Result {
	filled := dec.IsPositive(taker.Qty.Sub(taker.Remaining))
	hasRemainder := dec.IsPositive(taker.Remaining)

	if !hasRemainder {
		return Result{Status: TakerFilled, Trades: trades}
	}

	switch taker.TIF {
	case orderbook.GTC:
		if taker.Type == orderbook.Market {
			// Market with no more liquidity → expire remainder.
			return Result{Status: TakerExpired, Trades: trades}
		}
		// Limit GTC: rest remainder on book.
		_ = book.Insert(taker)
		if filled {
			return Result{Status: TakerPartialOnBook, Trades: trades}
		}
		return Result{Status: TakerAcceptedOnBook, Trades: trades}

	case orderbook.IOC:
		return Result{Status: TakerExpired, Trades: trades}

	case orderbook.FOK:
		// Pre-check should have guaranteed full fill; this is a defensive
		// fallback. Any trades produced already applied to the book.
		return Result{Status: TakerExpired, Trades: trades}
	}

	return Result{Status: TakerExpired, Trades: trades}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// crosses reports whether taker can match the given maker.
//   - Market taker: always crosses.
//   - Limit Buy: taker.Price >= maker.Price
//   - Limit Sell: taker.Price <= maker.Price
func crosses(taker, maker *orderbook.Order) bool {
	if taker.Type == orderbook.Market {
		return true
	}
	if taker.Side == orderbook.Bid {
		return taker.Price.Cmp(maker.Price) >= 0
	}
	return taker.Price.Cmp(maker.Price) <= 0
}

// priceAcceptable reports whether the given maker price is acceptable to the
// taker (at or better than taker's limit).
func priceAcceptable(taker *orderbook.Order, makerPrice dec.Decimal) bool {
	if taker.Type == orderbook.Market {
		return true
	}
	if taker.Side == orderbook.Bid {
		return taker.Price.Cmp(makerPrice) >= 0
	}
	return taker.Price.Cmp(makerPrice) <= 0
}

// wouldCross reports whether the taker would consume any liquidity at the
// current top-of-book.
func wouldCross(book *orderbook.Book, taker *orderbook.Order) bool {
	best, ok := book.Best(taker.Side.Opposite())
	if !ok {
		return false
	}
	return crosses(taker, best)
}

// canFullyFill walks the opposite side in priority order and reports whether
// the cumulative liquidity (at prices acceptable to the taker) covers
// taker.Remaining.
func canFullyFill(book *orderbook.Book, taker *orderbook.Order) bool {
	need := taker.Remaining
	fillable := true
	book.Walk(taker.Side.Opposite(), func(o *orderbook.Order) bool {
		if !priceAcceptable(taker, o.Price) {
			fillable = false
			return false
		}
		need = need.Sub(o.Remaining)
		return dec.IsPositive(need) // keep walking while still needed
	})
	return fillable && !dec.IsPositive(need)
}

// hasSelfCross reports whether any maker in the crossing range belongs to the
// same user as the taker.
func hasSelfCross(book *orderbook.Book, taker *orderbook.Order) bool {
	found := false
	book.Walk(taker.Side.Opposite(), func(o *orderbook.Order) bool {
		if !priceAcceptable(taker, o.Price) {
			return false
		}
		if o.UserID == taker.UserID {
			found = true
			return false
		}
		return true
	})
	return found
}
