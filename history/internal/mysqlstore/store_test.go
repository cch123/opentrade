package mysqlstore

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
)

// ---------------------------------------------------------------------------
// GetOrder
// ---------------------------------------------------------------------------

func TestGetOrder_FoundAndNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	store := NewStoreWithDB(db, time.Second)

	rows := sqlmock.NewRows([]string{
		"order_id", "client_order_id", "user_id", "symbol",
		"side", "order_type", "tif",
		"price", "qty", "filled_qty", "frozen_amt",
		"status", "reject_reason",
		"created_at_ms", "updated_at_ms",
	}).AddRow(
		uint64(42), "cli-1", "u1", "BTC-USDT",
		int8(1), int8(1), int8(1),
		"100", "1", "0.5", "100",
		int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PARTIALLY_FILLED), int8(0),
		int64(1_700_000_000_000), int64(1_700_000_001_000),
	)
	mock.ExpectQuery(regexp.QuoteMeta("FROM orders WHERE order_id = ? AND user_id = ?")).
		WithArgs(uint64(42), "u1").WillReturnRows(rows)

	o, err := store.GetOrder(context.Background(), "u1", 42)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if o.Status != historypb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED {
		t.Fatalf("status = %v want PARTIALLY_FILLED", o.Status)
	}
	if o.Side != eventpb.Side_SIDE_BUY {
		t.Fatalf("side = %v", o.Side)
	}

	// NotFound path.
	mock.ExpectQuery(regexp.QuoteMeta("FROM orders WHERE order_id = ? AND user_id = ?")).
		WithArgs(uint64(99), "u1").WillReturnRows(sqlmock.NewRows(nil))
	if _, err := store.GetOrder(context.Background(), "u1", 99); err != ErrNotFound {
		t.Fatalf("err = %v want ErrNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ListOrders — next_cursor boundary behaviour
// ---------------------------------------------------------------------------

func TestListOrders_ProducesNextCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	store := NewStoreWithDB(db, time.Second)

	// limit=2, return 3 rows so the store pops the extra and emits next cursor.
	cols := []string{
		"order_id", "client_order_id", "user_id", "symbol",
		"side", "order_type", "tif",
		"price", "qty", "filled_qty", "frozen_amt",
		"status", "reject_reason",
		"created_at_ms", "updated_at_ms",
	}
	rows := sqlmock.NewRows(cols).
		AddRow(uint64(3), "", "u1", "BTC-USDT", int8(1), int8(1), int8(1), "100", "1", "0", "100", int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW), int8(0), int64(300), int64(300)).
		AddRow(uint64(2), "", "u1", "BTC-USDT", int8(1), int8(1), int8(1), "100", "1", "0", "100", int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW), int8(0), int64(200), int64(200)).
		AddRow(uint64(1), "", "u1", "BTC-USDT", int8(1), int8(1), int8(1), "100", "1", "0", "100", int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_NEW), int8(0), int64(100), int64(100))

	mock.ExpectQuery("FROM orders").WillReturnRows(rows)

	out, next, err := store.ListOrders(context.Background(), OrdersFilter{UserID: "u1"}, "", 2)
	if err != nil {
		t.Fatalf("ListOrders: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len(out) = %d want 2", len(out))
	}
	if next == "" {
		t.Fatalf("expected non-empty next_cursor")
	}
	// Last returned row was order_id=2 / created_at=200; next_cursor must encode it.
	if out[1].OrderId != 2 {
		t.Fatalf("last order_id = %d want 2", out[1].OrderId)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestListOrders_EndOfStreamEmptyCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	store := NewStoreWithDB(db, time.Second)

	cols := []string{
		"order_id", "client_order_id", "user_id", "symbol",
		"side", "order_type", "tif",
		"price", "qty", "filled_qty", "frozen_amt",
		"status", "reject_reason",
		"created_at_ms", "updated_at_ms",
	}
	// limit=5 but only 2 rows → no next cursor.
	rows := sqlmock.NewRows(cols).
		AddRow(uint64(2), "", "u1", "", int8(1), int8(1), int8(1), "0", "0", "0", "0", int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED), int8(0), int64(200), int64(200)).
		AddRow(uint64(1), "", "u1", "", int8(1), int8(1), int8(1), "0", "0", "0", "0", int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_FILLED), int8(0), int64(100), int64(100))

	mock.ExpectQuery("FROM orders").WillReturnRows(rows)

	out, next, err := store.ListOrders(context.Background(), OrdersFilter{UserID: "u1"}, "", 5)
	if err != nil {
		t.Fatalf("ListOrders: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len(out) = %d", len(out))
	}
	if next != "" {
		t.Fatalf("next_cursor should be empty, got %q", next)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Scope → internal-status fanout
// ---------------------------------------------------------------------------

func TestStatusesForScope(t *testing.T) {
	cases := []struct {
		in     historypb.OrderScope
		expect int
	}{
		{historypb.OrderScope_ORDER_SCOPE_OPEN, 2},
		{historypb.OrderScope_ORDER_SCOPE_TERMINAL, 4},
		{historypb.OrderScope_ORDER_SCOPE_ALL, 0},
		{historypb.OrderScope_ORDER_SCOPE_UNSPECIFIED, 0},
	}
	for _, c := range cases {
		got := StatusesForScope(c.in)
		if len(got) != c.expect {
			t.Errorf("scope %v → len %d want %d", c.in, len(got), c.expect)
		}
	}
}

func TestInternalStatusesForFilters(t *testing.T) {
	// NEW folds PENDING_NEW + NEW (2 internals), PARTIALLY_FILLED folds
	// PARTIALLY_FILLED + PENDING_CANCEL (2). Together, 4 distinct.
	got := InternalStatusesForFilters([]historypb.OrderStatus{
		historypb.OrderStatus_ORDER_STATUS_NEW,
		historypb.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED,
	})
	if len(got) != 4 {
		t.Fatalf("len = %d want 4: %v", len(got), got)
	}
}

// ---------------------------------------------------------------------------
// ListAccountLogs — user-scoped query runs and decodes
// ---------------------------------------------------------------------------

func TestListAccountLogs_Basic(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	store := NewStoreWithDB(db, time.Second)

	cols := []string{
		"shard_id", "seq_id", "asset", "user_id",
		"delta_avail", "delta_frozen", "avail_after", "frozen_after",
		"biz_type", "biz_ref_id", "ts",
	}
	rows := sqlmock.NewRows(cols).
		AddRow(int32(3), uint64(10), "USDT", "u1", "-100", "100", "900", "100", "freeze_place_order", "42", int64(1_700_000_000_000))

	mock.ExpectQuery("FROM account_logs").WillReturnRows(rows)

	out, next, err := store.ListAccountLogs(context.Background(),
		AccountLogsFilter{UserID: "u1"}, "", 10)
	if err != nil {
		t.Fatalf("ListAccountLogs: %v", err)
	}
	if len(out) != 1 || out[0].Asset != "USDT" || out[0].BizType != "freeze_place_order" {
		t.Fatalf("row decode: %+v", out)
	}
	if next != "" {
		t.Fatalf("unexpected next cursor %q", next)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
