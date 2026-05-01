package journal

import (
	"errors"
	"sync"

	"github.com/xargin/opentrade/match/internal/sequencer"
)

// ErrUnknownSymbol is returned when an event arrives for a symbol that is not
// owned by this Match instance.
var ErrUnknownSymbol = errors.New("journal: unknown symbol")

// Dispatcher routes sequencer.Events to the correct per-symbol worker. Dispatch
// holds the read lock until the event is actually enqueued, so Unregister cannot
// close a worker inbox after a consumer has picked the worker but before it has
// submitted the event.
type Dispatcher struct {
	mu      sync.RWMutex
	workers map[string]*sequencer.SymbolWorker
}

// NewDispatcher returns an empty dispatcher.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{workers: make(map[string]*sequencer.SymbolWorker)}
}

// Register adds a worker for a symbol. Panics if the symbol is already
// registered — callers should check / reconfigure explicitly.
func (d *Dispatcher) Register(w *sequencer.SymbolWorker) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, dup := d.workers[w.Symbol()]; dup {
		panic("journal.Dispatcher: symbol already registered: " + w.Symbol())
	}
	d.workers[w.Symbol()] = w
}

// Unregister removes a worker for a symbol, returning false if absent.
func (d *Dispatcher) Unregister(symbol string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.workers[symbol]; !ok {
		return false
	}
	delete(d.workers, symbol)
	return true
}

// PickWorker returns the worker for the given symbol, or nil.
func (d *Dispatcher) PickWorker(symbol string) *sequencer.SymbolWorker {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.workers[symbol]
}

// Dispatch routes evt to the correct worker.
func (d *Dispatcher) Dispatch(evt *sequencer.Event) error {
	if evt.Symbol == "" {
		return errors.New("journal: event has no symbol")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	w := d.workers[evt.Symbol]
	if w == nil {
		return ErrUnknownSymbol
	}
	w.Submit(evt)
	return nil
}

// Workers returns a snapshot of all registered workers.
func (d *Dispatcher) Workers() []*sequencer.SymbolWorker {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*sequencer.SymbolWorker, 0, len(d.workers))
	for _, w := range d.workers {
		out = append(out, w)
	}
	return out
}
