package etcdcfg

import (
	"context"
	"sync"
)

// MemorySource is an in-memory Source for tests. It supports List, Watch,
// and programmatic Put/Delete that feed the watch stream.
type MemorySource struct {
	mu        sync.Mutex
	data      map[string]SymbolConfig
	revision  int64
	listeners []memListener
}

type memListener struct {
	ch       chan Event
	ctx      context.Context
	fromRev  int64
	buffered []Event // events with rev < fromRev are replayed on demand
}

// NewMemorySource returns an empty in-memory source.
func NewMemorySource() *MemorySource {
	return &MemorySource{data: make(map[string]SymbolConfig)}
}

// Close is a no-op.
func (s *MemorySource) Close() error { return nil }

// List returns the current state.
func (s *MemorySource) List(_ context.Context) (map[string]SymbolConfig, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[string]SymbolConfig, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}
	return cp, s.revision, nil
}

// Watch returns a channel delivering events with revision > fromRevision.
// Any already-buffered events matching the revision window are replayed
// immediately on the returned channel.
func (s *MemorySource) Watch(ctx context.Context, fromRevision int64) (<-chan Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan Event, 32)
	s.listeners = append(s.listeners, memListener{ch: ch, ctx: ctx, fromRev: fromRevision})
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, l := range s.listeners {
			if l.ch == ch {
				close(ch)
				s.listeners = append(s.listeners[:i], s.listeners[i+1:]...)
				return
			}
		}
	}()
	return ch, nil
}

// Put writes cfg for symbol and notifies watchers.
func (s *MemorySource) Put(symbol string, cfg SymbolConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision++
	s.data[symbol] = cfg
	s.broadcast(Event{Type: EventPut, Symbol: symbol, Config: cfg}, s.revision)
}

// Delete removes symbol and notifies watchers.
func (s *MemorySource) Delete(symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revision++
	if _, ok := s.data[symbol]; !ok {
		return
	}
	delete(s.data, symbol)
	s.broadcast(Event{Type: EventDelete, Symbol: symbol}, s.revision)
}

func (s *MemorySource) broadcast(evt Event, rev int64) {
	for _, l := range s.listeners {
		if rev < l.fromRev {
			continue
		}
		select {
		case l.ch <- evt:
		case <-l.ctx.Done():
		}
	}
}
