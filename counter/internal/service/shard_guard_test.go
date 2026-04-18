package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/shard"
)

func newShardedFixture(t *testing.T, shardID, totalShards int) *Service {
	t.Helper()
	state := engine.NewShardState(shardID)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &mockPublisher{}
	return New(Config{ShardID: shardID, TotalShards: totalShards, ProducerID: "counter-shard-X-main"},
		state, seq, dt, pub, zap.NewNop())
}

// Pick a user id that does NOT land on shardID under totalShards.
func userForOtherShard(shardID, totalShards int) string {
	for i := 0; ; i++ {
		u := "probe-" + itoa(i)
		if shard.Index(u, totalShards) != shardID {
			return u
		}
	}
}

func TestOwnsUser_TotalShardsZero_ClaimsEverything(t *testing.T) {
	svc := newShardedFixture(t, 0, 0)
	if !svc.OwnsUser("anyone") {
		t.Error("TotalShards=0 must claim every user")
	}
}

func TestOwnsUser_SelectsByIndex(t *testing.T) {
	total := 10
	for sid := 0; sid < total; sid++ {
		svc := newShardedFixture(t, sid, total)
		owned := "probe-" + itoa(sid*31+1)
		want := shard.Index(owned, total) == sid
		if got := svc.OwnsUser(owned); got != want {
			t.Errorf("shard=%d user=%q OwnsUser=%v want %v", sid, owned, got, want)
		}
	}
}

func TestTransfer_WrongShardReturnsError(t *testing.T) {
	total := 10
	shardID := 0
	svc := newShardedFixture(t, shardID, total)
	otherUser := userForOtherShard(shardID, total)

	_, err := svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "tx-1", UserID: otherUser, Asset: "USDT",
		Amount: dec.New("10"), Type: engine.TransferDeposit,
	})
	if !errors.Is(err, ErrWrongShard) {
		t.Fatalf("got %v, want ErrWrongShard", err)
	}
}

func TestQueryBalance_WrongShardReturnsError(t *testing.T) {
	total := 10
	shardID := 3
	svc := newShardedFixture(t, shardID, total)
	otherUser := userForOtherShard(shardID, total)
	if _, err := svc.QueryBalance(otherUser, "USDT"); !errors.Is(err, ErrWrongShard) {
		t.Errorf("QueryBalance: got %v want ErrWrongShard", err)
	}
	if _, err := svc.QueryAccount(otherUser); !errors.Is(err, ErrWrongShard) {
		t.Errorf("QueryAccount: got %v want ErrWrongShard", err)
	}
	if _, err := svc.QueryOrder(otherUser, 1); !errors.Is(err, ErrWrongShard) {
		t.Errorf("QueryOrder: got %v want ErrWrongShard", err)
	}
}

func TestTransfer_CorrectShardPasses(t *testing.T) {
	total := 10
	shardID := 7
	svc := newShardedFixture(t, shardID, total)
	// Find a user owned by this shard.
	var owned string
	for i := 0; ; i++ {
		u := "mine-" + itoa(i)
		if shard.Index(u, total) == shardID {
			owned = u
			break
		}
	}
	res, err := svc.Transfer(context.Background(), engine.TransferRequest{
		TransferID: "tx-1", UserID: owned, Asset: "USDT",
		Amount: dec.New("50"), Type: engine.TransferDeposit,
	})
	if err != nil {
		t.Fatalf("Transfer failed for owned user: %v", err)
	}
	if res.Status != engine.TransferStatusConfirmed {
		t.Errorf("expected Confirmed, got %d", res.Status)
	}
}

