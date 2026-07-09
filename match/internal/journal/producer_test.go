package journal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/pkg/shard"
)

func newPumpTestProducer() *TradeProducer {
	return &TradeProducer{
		cfg: ProducerConfig{
			BatchSize:     32,
			FlushInterval: time.Hour,
		},
		logger:  zap.NewNop(),
		flushCh: make(chan flushReq, 1),
	}
}

// A snapshot flush has its own deadline and may fail while the producer is
// otherwise healthy. The output must stay queued: clearing it here would let a
// later snapshot bind an input offset whose corresponding trade-event was
// never durable.
func TestPump_FlushAndWaitFailureRetainsBatch(t *testing.T) {
	p := newPumpTestProducer()
	wantErr := errors.New("kafka unavailable")

	var (
		mu      sync.Mutex
		batches [][]*sequencer.Output
	)
	p.publishBatchHook = func(_ context.Context, batch []*sequencer.Output) error {
		mu.Lock()
		defer mu.Unlock()
		batches = append(batches, append([]*sequencer.Output(nil), batch...))
		if len(batches) == 1 {
			return wantErr
		}
		return nil
	}

	outbox := make(chan *sequencer.Output, 1)
	want := &sequencer.Output{Kind: sequencer.OutputOrderAccepted, Symbol: "BTC-USDT", MatchSeq: 7}
	outbox <- want
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Pump(context.Background(), outbox)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.FlushAndWait(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("first FlushAndWait error = %v, want %v", err, wantErr)
	}
	if err := p.FlushAndWait(ctx); err != nil {
		t.Fatalf("retry FlushAndWait: %v", err)
	}
	close(outbox)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Pump did not stop after outbox close")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 2 {
		t.Fatalf("publish calls = %d, want 2", len(batches))
	}
	for i, batch := range batches {
		if len(batch) != 1 || batch[0] != want {
			t.Fatalf("batch[%d] = %#v, want retained output %#v", i, batch, want)
		}
	}
}

// Autonomous batch/timer flushes have no caller that can safely recover the
// error. They must fail-stop the process before Match can consume more input or
// write a snapshot containing the advanced orderbook.
func TestPump_BackgroundPublishFailurePanics(t *testing.T) {
	p := newPumpTestProducer()
	p.cfg.BatchSize = 1
	p.publishBatchHook = func(context.Context, []*sequencer.Output) error {
		return errors.New("durability lost")
	}
	outbox := make(chan *sequencer.Output, 1)
	outbox <- &sequencer.Output{Kind: sequencer.OutputTrade, Symbol: "BTC-USDT", MatchSeq: 9}
	close(outbox)

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("Pump returned after a background publish failure; want fail-stop panic")
		}
	}()
	p.Pump(context.Background(), outbox)
}

// TestOutputTargets_NonTrade: every OutputKind other than Trade carries
// exactly one user_id and targets one vshard.
func TestOutputTargets_NonTrade(t *testing.T) {
	const vshards = 256
	cases := []struct {
		name string
		out  *sequencer.Output
	}{
		{"Accepted", &sequencer.Output{Kind: sequencer.OutputOrderAccepted, UserID: 6001}},
		{"Rejected", &sequencer.Output{Kind: sequencer.OutputOrderRejected, UserID: 6002}},
		{"Cancelled", &sequencer.Output{Kind: sequencer.OutputOrderCancelled, UserID: 6003}},
		{"Expired", &sequencer.Output{Kind: sequencer.OutputOrderExpired, UserID: 6004}},
	}
	for _, c := range cases {
		got := outputTargets(c.out, vshards)
		if len(got) != 1 {
			t.Errorf("%s: got %d targets, want 1", c.name, len(got))
			continue
		}
		if got[0].userID != c.out.UserID {
			t.Errorf("%s: userID = %d, want %d", c.name, got[0].userID, c.out.UserID)
		}
		wantPart := shard.Index(c.out.UserID, vshards)
		if got[0].partition != wantPart {
			t.Errorf("%s: partition = %d, want %d", c.name, got[0].partition, wantPart)
		}
	}
}

// TestOutputTargets_Trade_Dual: a normal Trade (maker != taker) emits
// two targets — one for maker, one for taker — each at their own
// vshard.
func TestOutputTargets_Trade_Dual(t *testing.T) {
	const vshards = 256
	out := &sequencer.Output{
		Kind:        sequencer.OutputTrade,
		UserID:      5001, // taker side (see convert.go)
		MakerUserID: 4001,
	}
	got := outputTargets(out, vshards)
	if len(got) != 2 {
		t.Fatalf("got %d targets, want 2: %+v", len(got), got)
	}
	// First entry is maker, second is taker — contract matters for
	// downstream debug / log readers.
	if got[0].userID != 4001 {
		t.Errorf("target[0] = %d, want maker", got[0].userID)
	}
	if got[1].userID != 5001 {
		t.Errorf("target[1] = %d, want taker", got[1].userID)
	}
	if got[0].partition != shard.Index(4001, vshards) {
		t.Errorf("maker partition = %d, want %d",
			got[0].partition, shard.Index(4001, vshards))
	}
	if got[1].partition != shard.Index(5001, vshards) {
		t.Errorf("taker partition = %d, want %d",
			got[1].partition, shard.Index(5001, vshards))
	}
}

// TestOutputTargets_Trade_SelfTrade: maker_user_id == taker_user_id
// collapses to a single emit so Counter doesn't apply a duplicate
// settlement to the same user (the downstream match_seq guard would
// drop the second copy anyway — this avoids the wasted event).
func TestOutputTargets_Trade_SelfTrade(t *testing.T) {
	const vshards = 256
	out := &sequencer.Output{
		Kind:        sequencer.OutputTrade,
		UserID:      6001,
		MakerUserID: 6001,
	}
	got := outputTargets(out, vshards)
	if len(got) != 1 {
		t.Fatalf("got %d targets, want 1: %+v", len(got), got)
	}
	if got[0].userID != 6001 || got[0].partition != shard.Index(6001, vshards) {
		t.Errorf("self-trade = %+v", got[0])
	}
}

// TestOutputTargets_Trade_ZeroMakerCollapses: if maker_user_id is
// zero (shouldn't happen in production but defensively), we fall back
// to single-emit rather than writing to partition for user 0.
func TestOutputTargets_Trade_EmptyMakerCollapses(t *testing.T) {
	out := &sequencer.Output{
		Kind:        sequencer.OutputTrade,
		UserID:      5002,
		MakerUserID: 0,
	}
	got := outputTargets(out, 256)
	if len(got) != 1 {
		t.Errorf("empty maker should collapse to single, got %d targets", len(got))
	}
}
