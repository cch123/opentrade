package reconcile

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/counter/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// putBalance seeds an in-memory account balance via the restore hook (the
// only exported path that bypasses transfer validation).
func putBalance(state *engine.ShardState, userID, asset string, avail, frozen dec.Decimal) {
	state.Account(userID).PutForRestore(asset, engine.Balance{Available: avail, Frozen: frozen})
}

func TestRunOnce_NoMismatches(t *testing.T) {
	state := engine.NewShardState(0)
	putBalance(state, "u1", "USDT", dec.New("100"), dec.New("10"))
	putBalance(state, "u1", "BTC", dec.New("0.5"), dec.Zero)
	putBalance(state, "u2", "USDT", dec.New("200"), dec.Zero)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// RunOnce enumerates Users() and batches them into one IN clause.
	// Order of user ids is non-deterministic, so match the SELECT loosely.
	mock.ExpectQuery(`SELECT user_id, asset, available, frozen FROM accounts WHERE user_id IN`).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "asset", "available", "frozen"}).
			AddRow("u1", "USDT", "100", "10").
			AddRow("u1", "BTC", "0.5", "0").
			AddRow("u2", "USDT", "200", "0"))

	r := New(Config{ShardID: 0, BatchSize: 100}, state, db, zap.NewNop())
	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if rep.UsersChecked != 2 {
		t.Errorf("users checked = %d, want 2", rep.UsersChecked)
	}
	if rep.AssetsChecked != 3 {
		t.Errorf("assets checked = %d, want 3", rep.AssetsChecked)
	}
	if len(rep.Mismatches) != 0 {
		t.Errorf("mismatches = %+v, want none", rep.Mismatches)
	}
}

func TestRunOnce_ValueDiff(t *testing.T) {
	state := engine.NewShardState(0)
	putBalance(state, "u1", "USDT", dec.New("100"), dec.New("10"))

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT user_id, asset, available, frozen FROM accounts WHERE user_id IN`).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "asset", "available", "frozen"}).
			AddRow("u1", "USDT", "99.5", "10"))

	r := New(Config{ShardID: 0}, state, db, zap.NewNop())
	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(rep.Mismatches) != 1 {
		t.Fatalf("mismatches = %+v", rep.Mismatches)
	}
	m := rep.Mismatches[0]
	if m.Kind != MismatchValueDiff {
		t.Errorf("kind = %s, want value_diff", m.Kind)
	}
	if m.UserID != "u1" || m.Asset != "USDT" {
		t.Errorf("target = %s/%s", m.UserID, m.Asset)
	}
	if m.MemAvailable != "100" || m.DBAvailable != "99.5" {
		t.Errorf("avail values: mem=%s db=%s", m.MemAvailable, m.DBAvailable)
	}
}

func TestRunOnce_OnlyInMemory(t *testing.T) {
	state := engine.NewShardState(0)
	putBalance(state, "u1", "USDT", dec.New("100"), dec.Zero)
	putBalance(state, "u1", "BTC", dec.New("1"), dec.Zero)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// DB returns only USDT row — BTC is only in memory (e.g. projection lag).
	mock.ExpectQuery(`SELECT user_id, asset, available, frozen FROM accounts WHERE user_id IN`).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "asset", "available", "frozen"}).
			AddRow("u1", "USDT", "100", "0"))

	r := New(Config{ShardID: 0}, state, db, zap.NewNop())
	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(rep.Mismatches) != 1 {
		t.Fatalf("mismatches = %+v", rep.Mismatches)
	}
	if rep.Mismatches[0].Kind != MismatchOnlyInMemory {
		t.Errorf("kind = %s, want only_in_memory", rep.Mismatches[0].Kind)
	}
	if rep.Mismatches[0].Asset != "BTC" {
		t.Errorf("asset = %s, want BTC", rep.Mismatches[0].Asset)
	}
}

func TestRunOnce_EmptyState(t *testing.T) {
	state := engine.NewShardState(0)
	db, _, err := sqlmock.New() // no expectations — should not query
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := New(Config{ShardID: 0}, state, db, zap.NewNop())
	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if rep.UsersChecked != 0 || len(rep.Mismatches) != 0 {
		t.Errorf("report = %+v, want zero", rep)
	}
}

func TestRunOnce_Batching(t *testing.T) {
	state := engine.NewShardState(0)
	// 5 users, batch size 2 → 3 queries.
	for i := 0; i < 5; i++ {
		putBalance(state, userName(i), "USDT", dec.New("1"), dec.Zero)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for range 3 {
		mock.ExpectQuery(`SELECT user_id, asset, available, frozen FROM accounts WHERE user_id IN`).
			WillReturnRows(sqlmock.NewRows([]string{"user_id", "asset", "available", "frozen"}))
	}
	r := New(Config{ShardID: 0, BatchSize: 2}, state, db, zap.NewNop())
	rep, err := r.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// DB returned no rows, so every (user, asset) is only_in_memory.
	if len(rep.Mismatches) != 5 {
		t.Errorf("mismatches = %d, want 5", len(rep.Mismatches))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
	}
}

func userName(i int) string {
	return "u" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
