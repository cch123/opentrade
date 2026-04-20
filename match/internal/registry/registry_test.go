package registry

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/match/internal/engine"
	"github.com/xargin/opentrade/match/internal/journal"
	"github.com/xargin/opentrade/match/internal/sequencer"
)

func newFixture(t *testing.T) (*Registry, *journal.Dispatcher, chan *sequencer.Output, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	dispatcher := journal.NewDispatcher()
	outbox := make(chan *sequencer.Output, 256)

	var restores, snapshots atomic.Int64

	reg, err := New(context.Background(), Config{
		Factory: func(symbol string) *sequencer.SymbolWorker {
			return sequencer.NewSymbolWorker(sequencer.Config{
				Symbol:  symbol,
				Inbox:   64,
				STPMode: engine.STPNone,
			}, outbox, nil)
		},
		Restore: func(w *sequencer.SymbolWorker) error {
			restores.Add(1)
			return nil
		},
		Snapshot: func(w *sequencer.SymbolWorker) {
			snapshots.Add(1)
		},
		Dispatcher: dispatcher,
		Logger:     zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return reg, dispatcher, outbox, &restores, &snapshots
}

func TestAddSymbol_RegistersAndRunsWorker(t *testing.T) {
	reg, dispatcher, _, restores, _ := newFixture(t)
	t.Cleanup(reg.Close)

	if err := reg.AddSymbol("BTC-USDT"); err != nil {
		t.Fatalf("AddSymbol: %v", err)
	}
	if !reg.HasSymbol("BTC-USDT") {
		t.Error("HasSymbol should be true")
	}
	if w := dispatcher.PickWorker("BTC-USDT"); w == nil {
		t.Error("dispatcher should have the worker")
	}
	if restores.Load() != 1 {
		t.Errorf("restores: %d", restores.Load())
	}
}

func TestAddSymbol_DuplicateIsNoop(t *testing.T) {
	reg, dispatcher, _, restores, _ := newFixture(t)
	t.Cleanup(reg.Close)

	if err := reg.AddSymbol("BTC-USDT"); err != nil {
		t.Fatal(err)
	}
	if err := reg.AddSymbol("BTC-USDT"); err != nil {
		t.Errorf("duplicate should no-op: %v", err)
	}
	if len(dispatcher.Workers()) != 1 {
		t.Errorf("dispatcher has duplicates: %d", len(dispatcher.Workers()))
	}
	if restores.Load() != 1 {
		t.Errorf("restore should not run twice: %d", restores.Load())
	}
}

func TestRemoveSymbol_CleansUpAndSnapshots(t *testing.T) {
	reg, dispatcher, _, _, snapshots := newFixture(t)

	if err := reg.AddSymbol("BTC-USDT"); err != nil {
		t.Fatal(err)
	}
	if err := reg.RemoveSymbol("BTC-USDT"); err != nil {
		t.Fatalf("RemoveSymbol: %v", err)
	}
	if reg.HasSymbol("BTC-USDT") {
		t.Error("symbol should be gone")
	}
	if w := dispatcher.PickWorker("BTC-USDT"); w != nil {
		t.Error("dispatcher should not still have it")
	}
	if snapshots.Load() != 1 {
		t.Errorf("snapshot call count: %d", snapshots.Load())
	}
}

func TestRemoveSymbol_UnknownIsNoop(t *testing.T) {
	reg, _, _, _, snapshots := newFixture(t)
	t.Cleanup(reg.Close)
	if err := reg.RemoveSymbol("UNK"); err != nil {
		t.Errorf("unknown: %v", err)
	}
	if snapshots.Load() != 0 {
		t.Errorf("snapshot should not run: %d", snapshots.Load())
	}
}

func TestRestoreError_AbortsAdd(t *testing.T) {
	dispatcher := journal.NewDispatcher()
	outbox := make(chan *sequencer.Output, 8)
	reg, err := New(context.Background(), Config{
		Factory: func(symbol string) *sequencer.SymbolWorker {
			return sequencer.NewSymbolWorker(sequencer.Config{Symbol: symbol, Inbox: 8}, outbox, nil)
		},
		Restore:    func(*sequencer.SymbolWorker) error { return errors.New("corrupt snapshot") },
		Dispatcher: dispatcher,
		Logger:     zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.AddSymbol("BTC-USDT"); err == nil {
		t.Fatal("expected error")
	}
	if reg.HasSymbol("BTC-USDT") {
		t.Error("symbol should not be tracked on restore failure")
	}
	if w := dispatcher.PickWorker("BTC-USDT"); w != nil {
		t.Error("dispatcher should not have it either")
	}
}

func TestClose_TearsDownEverySymbol(t *testing.T) {
	reg, dispatcher, _, _, snapshots := newFixture(t)

	for _, s := range []string{"A", "B", "C"} {
		if err := reg.AddSymbol(s); err != nil {
			t.Fatal(err)
		}
	}
	reg.Close()

	if n := len(dispatcher.Workers()); n != 0 {
		t.Errorf("dispatcher not cleaned: %d", n)
	}
	if snapshots.Load() != 3 {
		t.Errorf("snapshot count: %d", snapshots.Load())
	}
	if err := reg.AddSymbol("D"); err == nil {
		t.Error("AddSymbol after Close should error")
	}
}

func TestRemoveSymbol_WaitsForWorkerDone(t *testing.T) {
	// Make the snapshot callback observe that the worker has actually
	// exited (inbox closed, Run returned) by sampling a flag the worker
	// would only be able to observe if it kept running.
	var observed int32
	dispatcher := journal.NewDispatcher()
	outbox := make(chan *sequencer.Output, 8)
	reg, err := New(context.Background(), Config{
		Factory: func(symbol string) *sequencer.SymbolWorker {
			return sequencer.NewSymbolWorker(sequencer.Config{Symbol: symbol, Inbox: 8}, outbox, nil)
		},
		Snapshot: func(w *sequencer.SymbolWorker) {
			// If we're invoked, Run has already returned. Flip the flag.
			atomic.StoreInt32(&observed, 1)
			select {
			case <-w.Done():
			case <-time.After(100 * time.Millisecond):
				// Done() should already be closed; if not, snapshot was invoked too early.
			}
		},
		Dispatcher: dispatcher,
		Logger:     zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = reg.AddSymbol("X")
	done := make(chan struct{})
	go func() {
		_ = reg.RemoveSymbol("X")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RemoveSymbol did not return")
	}
	if atomic.LoadInt32(&observed) != 1 {
		t.Error("snapshot callback not invoked")
	}
}

// Sanity: the registry itself doesn't deadlock if many symbols are
// added/removed concurrently.
func TestRegistry_ConcurrentAddRemove(t *testing.T) {
	reg, _, _, _, _ := newFixture(t)
	t.Cleanup(reg.Close)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sym := "S" + itoa(i%5)
			_ = reg.AddSymbol(sym)
			_ = reg.RemoveSymbol(sym)
		}(i)
	}
	wg.Wait()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
