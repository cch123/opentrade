package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
)

// mockPublisher records every Publish call and can be configured to fail.
type mockPublisher struct {
	mu       sync.Mutex
	events   []*eventpb.CounterJournalEvent
	failNext error
}

func (m *mockPublisher) Publish(_ context.Context, _ string, evt *eventpb.CounterJournalEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext != nil {
		err := m.failNext
		m.failNext = nil
		return err
	}
	m.events = append(m.events, evt)
	return nil
}

func (m *mockPublisher) Events() []*eventpb.CounterJournalEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*eventpb.CounterJournalEvent(nil), m.events...)
}

func newFixture(t *testing.T) (*Service, *engine.ShardState, *mockPublisher) {
	t.Helper()
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &mockPublisher{}
	svc := New(Config{ShardID: 0, ProducerID: "counter-shard-0-main"}, state, seq, dt, pub, zap.NewNop())
	return svc, state, pub
}

func TestTransferConfirms(t *testing.T) {
	svc, state, pub := newFixture(t)

	res, err := svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "tx-1",
		UserID:     "u1",
		Asset:      "USDT",
		Amount:     dec.New("100"),
		Type:       engine.TransferDeposit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != engine.TransferStatusConfirmed {
		t.Fatalf("status = %d, want Confirmed", res.Status)
	}
	if res.BalanceAfter.Available.String() != "100" {
		t.Fatalf("balance_after.available = %s", res.BalanceAfter.Available)
	}
	if state.Balance("u1", "USDT").Available.String() != "100" {
		t.Fatalf("state not committed: %+v", state.Balance("u1", "USDT"))
	}
	if len(pub.Events()) != 1 {
		t.Fatalf("expected 1 Kafka event, got %d", len(pub.Events()))
	}
}

func TestTransferRejectsInsufficient(t *testing.T) {
	svc, state, pub := newFixture(t)
	res, err := svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "tx-1", UserID: "u1", Asset: "USDT",
		Amount: dec.New("10"), Type: engine.TransferWithdraw,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != engine.TransferStatusRejected {
		t.Fatalf("status = %d, want Rejected", res.Status)
	}
	if !state.Balance("u1", "USDT").IsEmpty() {
		t.Fatal("state mutated despite rejection")
	}
	if len(pub.Events()) != 0 {
		t.Fatal("rejection wrote to Kafka")
	}
}

func TestTransferIdempotent(t *testing.T) {
	svc, _, pub := newFixture(t)
	req := engine.TransferRequest{
		TransferID: "tx-1", UserID: "u1", Asset: "USDT",
		Amount: dec.New("10"), Type: engine.TransferDeposit,
	}
	first, err := svc.Transfer(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != engine.TransferStatusConfirmed {
		t.Fatalf("first: status = %d", first.Status)
	}
	second, err := svc.Transfer(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != engine.TransferStatusDuplicated {
		t.Fatalf("second: status = %d, want Duplicated", second.Status)
	}
	// ADR-0048 backlog item 4 方案 A: ring only remembers the id, not the
	// cached response — dedup hits return a bare DUPLICATED with empty
	// balance / counter_seq_id. Callers fetch balance_after via QueryBalance.
	if !second.BalanceAfter.IsEmpty() {
		t.Fatalf("second: expected empty balance, got %+v", second.BalanceAfter)
	}
	if second.CounterSeqID != 0 {
		t.Fatalf("second: expected zero counter_seq_id, got %d", second.CounterSeqID)
	}
	if len(pub.Events()) != 1 {
		t.Fatalf("dedup hit wrote Kafka event: %d total", len(pub.Events()))
	}
}

func TestTransferKafkaFailureRollsBackMemory(t *testing.T) {
	svc, state, pub := newFixture(t)
	pub.mu.Lock()
	pub.failNext = errors.New("kafka down")
	pub.mu.Unlock()

	_, err := svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "tx-1", UserID: "u1", Asset: "USDT",
		Amount: dec.New("10"), Type: engine.TransferDeposit,
	})
	if err == nil {
		t.Fatal("expected publish error")
	}
	if !state.Balance("u1", "USDT").IsEmpty() {
		t.Fatalf("state mutated despite publish failure: %+v", state.Balance("u1", "USDT"))
	}

	// A retry with the same transfer_id should go through cleanly.
	res, err := svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "tx-1", UserID: "u1", Asset: "USDT",
		Amount: dec.New("10"), Type: engine.TransferDeposit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != engine.TransferStatusConfirmed {
		t.Fatalf("retry: status = %d", res.Status)
	}
	if state.Balance("u1", "USDT").Available.String() != "10" {
		t.Fatalf("retry did not commit: %+v", state.Balance("u1", "USDT"))
	}
}

func TestTransferInvalidArgs(t *testing.T) {
	svc, _, _ := newFixture(t)

	cases := []engine.TransferRequest{
		{UserID: "", TransferID: "tx", Asset: "USDT", Amount: dec.New("1"), Type: engine.TransferDeposit},
		{UserID: "u1", TransferID: "", Asset: "USDT", Amount: dec.New("1"), Type: engine.TransferDeposit},
		{UserID: "u1", TransferID: "tx", Asset: "", Amount: dec.New("1"), Type: engine.TransferDeposit},
	}
	for _, c := range cases {
		if _, err := svc.Transfer(context.Background(), c); err == nil {
			t.Fatalf("expected error for %+v", c)
		}
	}
}

func TestConcurrentUsersIndependent(t *testing.T) {
	svc, _, pub := newFixture(t)
	const users, perUser = 20, 50
	var wg sync.WaitGroup
	wg.Add(users)
	for u := 0; u < users; u++ {
		user := userLabel(u)
		go func() {
			defer wg.Done()
			for i := 0; i < perUser; i++ {
				res, err := svc.Transfer(context.Background(), engine.TransferRequest{
					TransferID: user + "-" + itoa(i),
					UserID:     user,
					Asset:      "USDT",
					Amount:     dec.New("1"),
					Type:       engine.TransferDeposit,
				})
				if err != nil || res.Status != engine.TransferStatusConfirmed {
					t.Errorf("%s#%d: err=%v status=%v", user, i, err, res.Status)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := len(pub.Events()); got != users*perUser {
		t.Fatalf("events = %d, want %d", got, users*perUser)
	}
	// All counter_seq_ids must be distinct.
	seen := make(map[uint64]bool, users*perUser)
	for _, e := range pub.Events() {
		if seen[e.CounterSeqId] {
			t.Fatalf("duplicate counter_seq_id %d", e.CounterSeqId)
		}
		seen[e.CounterSeqId] = true
	}
}

// tiny helpers; avoid strconv to keep Go imports minimal in tests.
func userLabel(u int) string { return "u" + itoa(u) }
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
