package main

import (
	"testing"

	"github.com/xargin/opentrade/counter/internal/snapshot"
	"github.com/xargin/opentrade/pkg/shard"
)

// TestReshard_RoutesUsersToNewShard constructs a fake 2-shard layout, asks
// for 5 output shards and asserts each account ends up on the new shard
// dictated by pkg/shard.Index.
func TestReshard_RoutesUsersToNewShard(t *testing.T) {
	users := []string{"alice", "bob", "carol", "dave", "eve", "frank", "gwen"}
	inputs := []*snapshot.ShardSnapshot{
		{
			Version: snapshot.Version, ShardID: 0, CounterSeq: 100,
			Accounts: []snapshot.AccountSnapshot{
				{UserID: users[0], Balances: []snapshot.BalanceSnapshot{{Asset: "USDT", Available: "1"}}},
				{UserID: users[2], Balances: []snapshot.BalanceSnapshot{{Asset: "USDT", Available: "2"}}},
				{UserID: users[4], Balances: []snapshot.BalanceSnapshot{{Asset: "USDT", Available: "3"}}},
			},
			Orders: []snapshot.OrderSnapshot{
				{ID: 1, UserID: users[0], Symbol: "BTC-USDT"},
				{ID: 2, UserID: users[2], Symbol: "BTC-USDT"},
			},
			Dedup: []snapshot.DedupEntrySnapshot{{Key: "tx-1", ExpiresUnix: 1}},
		},
		{
			Version: snapshot.Version, ShardID: 1, CounterSeq: 90,
			Accounts: []snapshot.AccountSnapshot{
				{UserID: users[1], Balances: []snapshot.BalanceSnapshot{{Asset: "USDT", Available: "10"}}},
				{UserID: users[3], Balances: []snapshot.BalanceSnapshot{{Asset: "USDT", Available: "20"}}},
				{UserID: users[5], Balances: []snapshot.BalanceSnapshot{{Asset: "USDT", Available: "30"}}},
				{UserID: users[6], Balances: []snapshot.BalanceSnapshot{{Asset: "USDT", Available: "40"}}},
			},
		},
	}
	outputs, rep := reshard(inputs, 5, 1234)

	if rep.Users != len(users) {
		t.Errorf("report users = %d, want %d", rep.Users, len(users))
	}
	if rep.Orders != 2 {
		t.Errorf("report orders = %d, want 2", rep.Orders)
	}
	if rep.DroppedDedup != 1 {
		t.Errorf("report dropped dedup = %d, want 1", rep.DroppedDedup)
	}
	if rep.MaxCounterSeq != 100 {
		t.Errorf("max counter seq = %d, want 100", rep.MaxCounterSeq)
	}
	for i, o := range outputs {
		if o.ShardID != i {
			t.Errorf("output[%d].ShardID = %d", i, o.ShardID)
		}
		if o.CounterSeq != 100 {
			t.Errorf("output[%d].CounterSeq = %d, want 100", i, o.CounterSeq)
		}
		if o.TimestampMS != 1234 {
			t.Errorf("output[%d].TimestampMS = %d", i, o.TimestampMS)
		}
	}
	// Every user must live on shard.Index(user, 5).
	found := make(map[string]int)
	for i, o := range outputs {
		for _, a := range o.Accounts {
			if _, dup := found[a.UserID]; dup {
				t.Errorf("user %s seen on two shards", a.UserID)
			}
			found[a.UserID] = i
		}
	}
	for _, u := range users {
		want := shard.Index(u, 5)
		got, ok := found[u]
		if !ok {
			t.Errorf("user %s not placed", u)
			continue
		}
		if got != want {
			t.Errorf("user %s placed on shard %d, want %d", u, got, want)
		}
	}
	// Orders must follow their user.
	for _, o := range outputs {
		for _, ord := range o.Orders {
			want := shard.Index(ord.UserID, 5)
			if o.ShardID != want {
				t.Errorf("order %d for user %s on shard %d, want %d", ord.ID, ord.UserID, o.ShardID, want)
			}
		}
	}
}

// TestReshard_IdempotentFromSameTotal: running reshard with from==to should
// be a no-op (same routing), just copied. Useful sanity check if operator
// reruns.
func TestReshard_IdempotentFromSameTotal(t *testing.T) {
	// Two input shards for fromN=2, same hashing for toM=2.
	mk := func(shardID int, users ...string) *snapshot.ShardSnapshot {
		s := &snapshot.ShardSnapshot{Version: snapshot.Version, ShardID: shardID, CounterSeq: uint64(shardID * 10)}
		for _, u := range users {
			s.Accounts = append(s.Accounts, snapshot.AccountSnapshot{UserID: u})
		}
		return s
	}
	// Keep it simple: use user ids the hash actually routes the way we seed.
	var s0, s1 []string
	for i := 0; i < 20; i++ {
		u := "u-" + itoa(i)
		switch shard.Index(u, 2) {
		case 0:
			s0 = append(s0, u)
		case 1:
			s1 = append(s1, u)
		}
	}
	inputs := []*snapshot.ShardSnapshot{mk(0, s0...), mk(1, s1...)}
	outputs, rep := reshard(inputs, 2, 0)

	if rep.Users != 20 {
		t.Errorf("users = %d", rep.Users)
	}
	for i, o := range outputs {
		for _, a := range o.Accounts {
			if shard.Index(a.UserID, 2) != i {
				t.Errorf("routing broken: user %s on shard %d", a.UserID, i)
			}
		}
	}
}

// TestReshard_NilInputsIgnored lets the operator provide a sparse input
// dir when some shards were already empty.
func TestReshard_NilInputsIgnored(t *testing.T) {
	inputs := []*snapshot.ShardSnapshot{
		nil,
		{Version: snapshot.Version, ShardID: 1, Accounts: []snapshot.AccountSnapshot{{UserID: "x"}}},
	}
	outputs, rep := reshard(inputs, 3, 0)
	if rep.Users != 1 {
		t.Errorf("users = %d, want 1", rep.Users)
	}
	found := false
	for i, o := range outputs {
		if len(o.Accounts) == 1 {
			if shard.Index("x", 3) != i {
				t.Errorf("user x on shard %d, want %d", i, shard.Index("x", 3))
			}
			found = true
		}
	}
	if !found {
		t.Error("did not place user x")
	}
}

// itoa avoids strconv just so the helper is self-contained.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	negative := false
	if i < 0 {
		negative = true
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if negative {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
