package server

import (
	"context"
	"regexp"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/DATA-DOG/go-sqlmock"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/mysqlstore"
)

func TestGetOrder_InvalidArgs(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s := New(mysqlstore.NewStoreWithDB(db, time.Second))

	if _, err := s.GetOrder(context.Background(), connect.NewRequest(&historypb.GetOrderRequest{})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("empty user_id: got %v", err)
	}
	if _, err := s.GetOrder(context.Background(), connect.NewRequest(&historypb.GetOrderRequest{UserId: "u1"})); connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("zero order_id: got %v", err)
	}
}

func TestGetOrder_NotFoundMapsToCode(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	s := New(mysqlstore.NewStoreWithDB(db, time.Second))

	mock.ExpectQuery(regexp.QuoteMeta("FROM orders WHERE order_id = ? AND user_id = ?")).
		WithArgs(uint64(1), "u1").
		WillReturnRows(sqlmock.NewRows(nil))

	_, err = s.GetOrder(context.Background(), connect.NewRequest(&historypb.GetOrderRequest{UserId: "u1", OrderId: 1}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("code = %v want NotFound; err = %v", connect.CodeOf(err), err)
	}
}

func TestListOrders_ScopeTranslatesToStatuses(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	s := New(mysqlstore.NewStoreWithDB(db, time.Second))

	// OPEN scope should expand to 2 external statuses → 4 internal codes
	// (PENDING_NEW, NEW, PARTIALLY_FILLED, PENDING_CANCEL). We assert the
	// placeholder count by matching the prepared SQL shape.
	cols := []string{
		"order_id", "client_order_id", "user_id", "symbol",
		"side", "order_type", "tif",
		"price", "qty", "filled_qty", "frozen_amt",
		"status", "reject_reason",
		"created_at_ms", "updated_at_ms",
	}
	mock.ExpectQuery(`status IN \(\?,\?,\?,\?\)`).
		WillReturnRows(sqlmock.NewRows(cols))

	_, err = s.ListOrders(context.Background(), connect.NewRequest(&historypb.ListOrdersRequest{
		UserId: "u1",
		Scope:  historypb.OrderScope_ORDER_SCOPE_OPEN,
	}))
	if err != nil {
		t.Fatalf("ListOrders: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestListOrders_ExplicitStatusesWinOverScope(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	s := New(mysqlstore.NewStoreWithDB(db, time.Second))

	// FILLED alone → 1 internal code, even though scope=ALL would imply none.
	cols := []string{
		"order_id", "client_order_id", "user_id", "symbol",
		"side", "order_type", "tif",
		"price", "qty", "filled_qty", "frozen_amt",
		"status", "reject_reason",
		"created_at_ms", "updated_at_ms",
	}
	mock.ExpectQuery(`status IN \(\?\)`).
		WillReturnRows(sqlmock.NewRows(cols))

	_, err = s.ListOrders(context.Background(), connect.NewRequest(&historypb.ListOrdersRequest{
		UserId:   "u1",
		Scope:    historypb.OrderScope_ORDER_SCOPE_ALL,
		Statuses: []historypb.OrderStatus{historypb.OrderStatus_ORDER_STATUS_FILLED},
	}))
	if err != nil {
		t.Fatalf("ListOrders: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestListOrders_InvalidCursorMapsToInvalidArgument(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s := New(mysqlstore.NewStoreWithDB(db, time.Second))

	_, err := s.ListOrders(context.Background(), connect.NewRequest(&historypb.ListOrdersRequest{
		UserId: "u1",
		Cursor: "!!!", // not valid base64
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, err = %v", connect.CodeOf(err), err)
	}
}

// Keep the compiler honest: event import must be used.
var _ = eventpb.Side_SIDE_BUY
