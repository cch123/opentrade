package sequencer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xargin/opentrade/match/internal/engine"
	"github.com/xargin/opentrade/match/internal/orderbook"
	"github.com/xargin/opentrade/pkg/dec"
)

// ErrSymbolMismatch is returned when an OrderPlaced event carries a symbol
// that does not match the worker's symbol.
var ErrSymbolMismatch = errors.New("sequencer: symbol mismatch")

// Config configures a SymbolWorker.
type Config struct {
	Symbol  string
	Inbox   int // channel capacity; default 2048
	STPMode engine.STPMode
}

// SymbolWorker serializes all matching for a single symbol.
// It owns a Book and a per-symbol monotonic match seq counter. It reads
// Events from Inbox and writes Outputs to Outbox.
//
// See ADR-0016 (per-symbol single-thread matching) and ADR-0019 (constant
// goroutine actor model).
type SymbolWorker struct {
	symbol string
	stp    engine.STPMode

	// mu guards book / matchSeq / bookSeq / offsets so snapshot readers
	// (WithStateLocked / Offsets) can take a consistent view without racing
	// with the worker goroutine. handle() takes mu for the duration of one
	// event — usually <100µs but bounded by outbox send (emit → Pump).
	mu       sync.Mutex
	book     *orderbook.Book
	matchSeq uint64
	bookSeq  uint64          // per-symbol monotonic orderbook seq (ADR-0055)
	offsets  map[int32]int64 // partition → next-to-consume offset (ADR-0048)

	inbox    chan *Event
	outbox   chan<- *Output
	mdOutbox chan<- *MarketDataOutput // optional; nil disables market-data emission

	done    chan struct{}
	started bool
}

// NewSymbolWorker constructs a worker. outbox receives trade-event emissions;
// callers must drain it or give it sufficient capacity. mdOutbox (optional,
// ADR-0055) receives market-data OrderBook Delta / Full frames when non-nil.
func NewSymbolWorker(cfg Config, outbox chan<- *Output, mdOutbox chan<- *MarketDataOutput) *SymbolWorker {
	if cfg.Inbox <= 0 {
		cfg.Inbox = 2048
	}
	return &SymbolWorker{
		symbol:   cfg.Symbol,
		book:     orderbook.NewBook(cfg.Symbol),
		stp:      cfg.STPMode,
		inbox:    make(chan *Event, cfg.Inbox),
		outbox:   outbox,
		mdOutbox: mdOutbox,
		done:     make(chan struct{}),
	}
}

// Symbol returns the symbol this worker owns.
func (w *SymbolWorker) Symbol() string { return w.symbol }

// Book returns the underlying orderbook. Callers must NOT mutate it from
// outside the worker goroutine. Read-only access without holding mu is
// only safe before Run starts or after Done fires — concurrent callers
// during Run should go through WithStateLocked.
func (w *SymbolWorker) Book() *orderbook.Book { return w.book }

// MatchSeq returns the current per-symbol monotonic match seq (for snapshot
// / recovery). Takes mu so the value is consistent with the book even while
// Run is dispatching.
func (w *SymbolWorker) MatchSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.matchSeq
}

// SetMatchSeq sets the starting match seq — used when restoring from a
// snapshot. Must be called before Run.
func (w *SymbolWorker) SetMatchSeq(seq uint64) { w.matchSeq = seq }

// WithStateLocked runs f while holding the worker's state lock. Use it to
// read book / matchSeq / offsets as a consistent snapshot from outside the
// worker goroutine (e.g. snapshot.Capture). f must NOT mutate the book or
// retain the offsets map past the call — the map is the worker's internal
// buffer.
func (w *SymbolWorker) WithStateLocked(f func(book *orderbook.Book, matchSeq uint64, offsets map[int32]int64)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f(w.book, w.matchSeq, w.offsets)
}

// WithStateLockedErr is the error-returning form of WithStateLocked. It exists
// for snapshot barriers that need to perform I/O while holding the state lock:
// the caller can flush producer output first, then capture book+offsets in the
// same critical section so recovery never observes state ahead of committed
// trade-event output.
func (w *SymbolWorker) WithStateLockedErr(f func(book *orderbook.Book, matchSeq uint64, offsets map[int32]int64) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return f(w.book, w.matchSeq, w.offsets)
}

// Offsets returns a copy of the per-partition next-to-consume offsets. Safe
// to call while Run is dispatching (takes mu).
func (w *SymbolWorker) Offsets() map[int32]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.offsets) == 0 {
		return nil
	}
	out := make(map[int32]int64, len(w.offsets))
	for p, o := range w.offsets {
		out[p] = o
	}
	return out
}

// SetOffsets replaces the per-partition offsets. Used by snapshot.Restore
// before Run starts; callers must not invoke it concurrently with Run.
func (w *SymbolWorker) SetOffsets(offs map[int32]int64) {
	if len(offs) == 0 {
		w.offsets = nil
		return
	}
	w.offsets = make(map[int32]int64, len(offs))
	for p, o := range offs {
		w.offsets[p] = o
	}
}

// Submit enqueues an event. Blocks if inbox is full.
func (w *SymbolWorker) Submit(evt *Event) { w.inbox <- evt }

// TrySubmit enqueues an event without blocking. Returns false if inbox is full.
func (w *SymbolWorker) TrySubmit(evt *Event) bool {
	select {
	case w.inbox <- evt:
		return true
	default:
		return false
	}
}

// Close closes the inbox; the worker will drain remaining events and exit.
func (w *SymbolWorker) Close() { close(w.inbox) }

// Done returns a channel that is closed after the worker exits.
func (w *SymbolWorker) Done() <-chan struct{} { return w.done }

// Run is the worker loop. Returns when ctx is cancelled or Close is called
// and the inbox is drained.
func (w *SymbolWorker) Run(ctx context.Context) {
	defer close(w.done)
	for {
		select {
		case <-ctx.Done():
			w.drainInbox()
			return
		case evt, ok := <-w.inbox:
			if !ok {
				return
			}
			w.handle(evt)
		}
	}
}

// drainInbox applies every event already queued when shutdown wins the select.
// Registry.RemoveSymbol closes the inbox before cancelling the worker context;
// draining here preserves the "close means finish accepted work" contract even
// if ctx.Done and the closed inbox become ready at the same time.
func (w *SymbolWorker) drainInbox() {
	for {
		select {
		case evt, ok := <-w.inbox:
			if !ok {
				return
			}
			w.handle(evt)
		default:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

func (w *SymbolWorker) handle(evt *Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch evt.Kind {
	case EventOrderPlaced:
		w.handlePlaced(evt)
	case EventOrderCancel:
		w.handleCancel(evt)
	default:
		// Unknown event kind: drop with no emission.
	}
	// Advance per-partition offset after a fully applied event (ADR-0048).
	// evt.Source is zero-valued for in-process tests; we gate on Topic so
	// those paths don't accidentally populate a bogus partition 0 entry.
	if evt.Source.Topic != "" {
		if w.offsets == nil {
			w.offsets = make(map[int32]int64)
		}
		if next := evt.Source.Offset + 1; next > w.offsets[evt.Source.Partition] {
			w.offsets[evt.Source.Partition] = next
		}
	}
	// ADR-0055: emit one Delta per input event carrying every level that
	// changed during the mutations above. Skipped when no mdOutbox is wired
	// (tests / legacy paths) or when no level actually changed (cancel of a
	// never-resting order, duplicate rejection, symbol mismatch).
	if w.mdOutbox != nil {
		bids, asks := w.book.DrainDirty()
		if len(bids) > 0 || len(asks) > 0 {
			w.emitMarketData(&MarketDataOutput{
				Kind:   MDKindDelta,
				Symbol: w.symbol,
				Bids:   bids,
				Asks:   asks,
			})
		}
	}
}

func (w *SymbolWorker) handlePlaced(evt *Event) {
	o := evt.Order
	if o.Symbol != w.symbol {
		w.emit(&Output{
			Kind:         OutputOrderRejected,
			Symbol:       w.symbol,
			UserID:       o.UserID,
			OrderID:      o.ID,
			RejectReason: orderbook.RejectSymbolNotTrading,
			SourceOffset: evt.Source,
		})
		return
	}

	// Duplicate order id — treat as defensive rejection (ADR-0015 match-side
	// defense-in-depth).
	if w.book.Has(o.ID) {
		w.emit(&Output{
			Kind:         OutputOrderRejected,
			Symbol:       w.symbol,
			UserID:       o.UserID,
			OrderID:      o.ID,
			RejectReason: orderbook.RejectDuplicateOrderID,
			SourceOffset: evt.Source,
		})
		return
	}

	res := engine.Match(w.book, o, w.stp)

	// Emit trades first (they precede the final order status).
	for i := range res.Trades {
		tr := res.Trades[i]
		w.emit(&Output{
			Kind:             OutputTrade,
			Symbol:           w.symbol,
			UserID:           tr.TakerUserID,
			OrderID:          tr.TakerOrderID,
			Side:             tr.TakerSide,
			Price:            tr.Price,
			Qty:              tr.Qty,
			MakerUserID:      tr.MakerUserID,
			MakerOrderID:     tr.MakerOrderID,
			MakerSide:        tr.MakerSide,
			MakerRemaining:   tr.MakerRemaining,
			MakerFilledAfter: tr.MakerFilledAfter,
			TakerRemaining:   tr.TakerRemaining,
			TakerFilledAfter: tr.TakerFilledAfter,
			SourceOffset:     evt.Source,
		})
	}

	// Emit terminal outcome for the taker.
	switch res.Status {
	case engine.TakerAcceptedOnBook, engine.TakerPartialOnBook:
		w.emit(&Output{
			Kind:           OutputOrderAccepted,
			Symbol:         w.symbol,
			UserID:         o.UserID,
			OrderID:        o.ID,
			Side:           o.Side,
			Price:          o.Price,
			TakerRemaining: o.Remaining,
			SourceOffset:   evt.Source,
		})
	case engine.TakerFilled:
		// Base-driven orders (limit / market sell) reach Filled when the last
		// trade brings Remaining to zero; Counter's settleTaker infers FILLED
		// via filledAfter >= Qty. Quote-driven market buys have Qty == 0, so
		// we need an explicit terminal signal — emit OrderExpired, and Counter
		// will refund residual = FrozenAmount − FrozenSpent (≈0 when budget
		// fully consumed).
		if o.IsQuoteDriven() {
			w.emit(&Output{
				Kind:         OutputOrderExpired,
				Symbol:       w.symbol,
				UserID:       o.UserID,
				OrderID:      o.ID,
				Side:         o.Side,
				FilledQty:    o.Qty.Sub(o.Remaining),
				SourceOffset: evt.Source,
			})
		}
	case engine.TakerExpired:
		w.emit(&Output{
			Kind:         OutputOrderExpired,
			Symbol:       w.symbol,
			UserID:       o.UserID,
			OrderID:      o.ID,
			Side:         o.Side,
			FilledQty:    o.Qty.Sub(o.Remaining),
			SourceOffset: evt.Source,
		})
	case engine.TakerRejected:
		w.emit(&Output{
			Kind:         OutputOrderRejected,
			Symbol:       w.symbol,
			UserID:       o.UserID,
			OrderID:      o.ID,
			Side:         o.Side,
			RejectReason: res.RejectReason,
			SourceOffset: evt.Source,
		})
	default:
		panic(fmt.Sprintf("sequencer: unknown TakerStatus %d", res.Status))
	}
}

func (w *SymbolWorker) handleCancel(evt *Event) {
	o, err := w.book.Cancel(evt.OrderID)
	if err != nil {
		// Order not on the book. This happens when match / counter state has
		// drifted: order already filled / already cancelled / never reached us
		// (dev wipe, snapshot divergence). Still emit OrderCancelled with zero
		// FilledQty so every input produces an output with a match_seq — the
		// invariant downstream dedup + PENDING_CANCEL unfreeze both rely on.
		// Counter's handleCancelled short-circuits if its own view already has
		// the order terminal; otherwise it unfreezes residual from its own
		// state and transitions PENDING_CANCEL → CANCELED.
		w.emit(&Output{
			Kind:         OutputOrderCancelled,
			Symbol:       w.symbol,
			UserID:       evt.UserID,
			OrderID:      evt.OrderID,
			FilledQty:    dec.Zero,
			SourceOffset: evt.Source,
		})
		return
	}
	// Authorization: if UserID is provided and does not match the order owner,
	// log and emit nothing. In production Counter enforces ownership before
	// forwarding; this is defensive.
	if evt.UserID != "" && evt.UserID != o.UserID {
		// Re-insert and reject: this is malformed routing.
		// (In current design counter-journal guarantees no such case; left as
		// a defensive guard.)
		_ = w.book.Insert(o)
		w.emit(&Output{
			Kind:         OutputOrderRejected,
			Symbol:       w.symbol,
			UserID:       evt.UserID,
			OrderID:      evt.OrderID,
			RejectReason: orderbook.RejectInternal,
			SourceOffset: evt.Source,
		})
		return
	}
	filled := o.Qty.Sub(o.Remaining)
	w.emit(&Output{
		Kind:         OutputOrderCancelled,
		Symbol:       w.symbol,
		UserID:       o.UserID,
		OrderID:      o.ID,
		Side:         o.Side,
		FilledQty:    filled,
		SourceOffset: evt.Source,
	})
}

// emit assigns the next match seq and sends the output to the outbox.
func (w *SymbolWorker) emit(out *Output) {
	w.matchSeq++
	out.MatchSeq = w.matchSeq
	if dec.IsZero(out.Qty) && out.Kind != OutputTrade {
		// keep zero values clean
	}
	w.outbox <- out
}

// emitMarketData assigns the next book seq and sends the market-data frame.
// Must be called with w.mu held. ADR-0055: orderbook seq is independent from
// trade-event match_seq_id so per-stream gap detection on the orderbook
// topic works without holes from unrelated trade-event output.
func (w *SymbolWorker) emitMarketData(md *MarketDataOutput) {
	w.bookSeq++
	md.BookSeq = w.bookSeq
	w.mdOutbox <- md
}

// EmitFull captures the current top-N orderbook state and emits an MDKindFull
// frame to the market-data outbox. Takes mu so it can be called from an
// external ticker goroutine alongside Run. topN <= 0 means "no limit". Does
// nothing when mdOutbox is nil. ADR-0055: periodic Full + startup anchor.
func (w *SymbolWorker) EmitFull(topN int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.mdOutbox == nil {
		return
	}
	bids := w.book.TopN(orderbook.Bid, topN)
	asks := w.book.TopN(orderbook.Ask, topN)
	// Discard anything the mutations queued: the Full we're about to send
	// supersedes any pending Delta for the same seq point.
	w.book.DiscardDirty()
	w.emitMarketData(&MarketDataOutput{
		Kind:   MDKindFull,
		Symbol: w.symbol,
		Bids:   bids,
		Asks:   asks,
	})
}

// BookSeq returns the current per-symbol orderbook seq (for snapshot /
// recovery). Takes mu.
func (w *SymbolWorker) BookSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bookSeq
}

// SetBookSeq sets the starting orderbook seq. Must be called before Run.
func (w *SymbolWorker) SetBookSeq(seq uint64) { w.bookSeq = seq }
