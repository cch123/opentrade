package sequencer

import (
	"context"
	"errors"
	"fmt"

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
	Inbox   int           // channel capacity; default 2048
	STPMode engine.STPMode
}

// SymbolWorker serializes all matching for a single symbol.
// It owns a Book and a per-symbol monotonic SeqID counter. It reads Events
// from Inbox and writes Outputs to Outbox.
//
// See ADR-0016 (per-symbol single-thread matching) and ADR-0019 (constant
// goroutine actor model).
type SymbolWorker struct {
	symbol  string
	book    *orderbook.Book
	seqID   uint64
	stp     engine.STPMode

	inbox   chan *Event
	outbox  chan<- *Output

	done    chan struct{}
	started bool
}

// NewSymbolWorker constructs a worker. outbox receives emissions; callers
// must drain it or give it sufficient capacity.
func NewSymbolWorker(cfg Config, outbox chan<- *Output) *SymbolWorker {
	if cfg.Inbox <= 0 {
		cfg.Inbox = 2048
	}
	return &SymbolWorker{
		symbol: cfg.Symbol,
		book:   orderbook.NewBook(cfg.Symbol),
		stp:    cfg.STPMode,
		inbox:  make(chan *Event, cfg.Inbox),
		outbox: outbox,
		done:   make(chan struct{}),
	}
}

// Symbol returns the symbol this worker owns.
func (w *SymbolWorker) Symbol() string { return w.symbol }

// Book returns the underlying orderbook. Callers must NOT mutate it from
// outside the worker goroutine; read-only access from tests is acceptable.
func (w *SymbolWorker) Book() *orderbook.Book { return w.book }

// SeqID returns the current per-symbol sequence id (for snapshot / recovery).
// Must be called before Run starts or after Done() fires.
func (w *SymbolWorker) SeqID() uint64 { return w.seqID }

// SetSeqID sets the starting sequence id — used when restoring from a
// snapshot. Must be called before Run.
func (w *SymbolWorker) SetSeqID(seq uint64) { w.seqID = seq }

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
			return
		case evt, ok := <-w.inbox:
			if !ok {
				return
			}
			w.handle(evt)
		}
	}
}

// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

func (w *SymbolWorker) handle(evt *Event) {
	switch evt.Kind {
	case EventOrderPlaced:
		w.handlePlaced(evt)
	case EventOrderCancel:
		w.handleCancel(evt)
	default:
		// Unknown event kind: drop with no emission.
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
			Kind:           OutputTrade,
			Symbol:         w.symbol,
			UserID:         tr.TakerUserID,
			OrderID:        tr.TakerOrderID,
			Side:           tr.TakerSide,
			Price:          tr.Price,
			Qty:            tr.Qty,
			MakerUserID:    tr.MakerUserID,
			MakerOrderID:   tr.MakerOrderID,
			MakerSide:      tr.MakerSide,
			MakerRemaining: tr.MakerRemaining,
			TakerRemaining: tr.TakerRemaining,
			SourceOffset:   evt.Source,
		})
	}

	// Emit terminal outcome for the taker.
	switch res.Status {
	case engine.TakerAcceptedOnBook, engine.TakerPartialOnBook:
		w.emit(&Output{
			Kind:         OutputOrderAccepted,
			Symbol:       w.symbol,
			UserID:       o.UserID,
			OrderID:      o.ID,
			Side:         o.Side,
			SourceOffset: evt.Source,
		})
	case engine.TakerFilled:
		// Nothing extra — the last Trade carries TakerRemaining=0.
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
		// Order not on the book — nothing to do. Counter's state machine will
		// accept a no-op (order may have been filled / already cancelled).
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

// emit assigns the next seq id and sends the output to the outbox.
func (w *SymbolWorker) emit(out *Output) {
	w.seqID++
	out.SeqID = w.seqID
	if dec.IsZero(out.Qty) && out.Kind != OutputTrade {
		// keep zero values clean
	}
	w.outbox <- out
}
