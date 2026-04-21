package server

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/mysqlstore"
)

func TestGetOrder_InvalidArgs(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	s := New(mysqlstore.NewStoreWithDB(db, time.Second))

	if _, err := s.GetOrder(context.Background(), &historypb.GetOrderRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty user_id: got %v", err)
	}
	if _, err := s.GetOrder(context.Background(), &historypb.GetOrderRequest{UserId: "u1"}); status.Code(err) != codes.InvalidArgument {
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

	_, err = s.GetOrder(context.Background(), &historypb.GetOrderRequest{UserId: "u1", OrderId: 1})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v want NotFound; err = %v", status.Code(err), err)
	}
}

func TestGetTransfer_NotFoundAndInvalidArgs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	s := New(mysqlstore.NewStoreWithDB(db, time.Second))

	if _, err := s.GetTransfer(context.Background(), &historypb.GetTransferRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty user_id: got %v", err)
	}
	if _, err := s.GetTransfer(context.Background(), &historypb.GetTransferRequest{UserId: "u1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("empty transfer_id: got %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("FROM transfers WHERE transfer_id = ? AND user_id = ?")).
		WithArgs("missing", "u1").
		WillReturnRows(sqlmock.NewRows(nil))
	_, err = s.GetTransfer(context.Background(), &historypb.GetTransferRequest{UserId: "u1", TransferId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v want NotFound", status.Code(err))
	}
}

func TestListTransfers_ScopeFoldsToStates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	s := New(mysqlstore.NewStoreWithDB(db, time.Second))

	// IN_FLIGHT scope expands to 3 states — expect 3 placeholders in the
	// IN clause.
	cols := []string{
		"transfer_id", "user_id", "from_biz", "to_biz", "asset",
		"amount", "state", "reject_reason",
		"created_at_ms", "updated_at_ms",
	}
	mock.ExpectQuery(`state IN \(\?,\?,\?\)`).
		WillReturnRows(sqlmock.NewRows(cols))

	_, err = s.ListTransfers(context.Background(), &historypb.ListTransfersRequest{
		UserId: "u1",
		Scope:  historypb.TransferScope_TRANSFER_SCOPE_IN_FLIGHT,
	})
	if err != nil {
		t.Fatalf("ListTransfers: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations: %v", err)
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

	_, err = s.ListOrders(context.Background(), &historypb.ListOrdersRequest{
		UserId: "u1",
		Scope:  historypb.OrderScope_ORDER_SCOPE_OPEN,
	})
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

	_, err = s.ListOrders(context.Background(), &historypb.ListOrdersRequest{
		UserId:   "u1",
		Scope:    historypb.OrderScope_ORDER_SCOPE_ALL,
		Statuses: []historypb.OrderStatus{historypb.OrderStatus_ORDER_STATUS_FILLED},
	})
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

	_, err := s.ListOrders(context.Background(), &historypb.ListOrdersRequest{
		UserId: "u1",
		Cursor: "!!!", // not valid base64
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, err = %v", status.Code(err), err)
	}
}

// Keep the compiler honest: event import must be used.
var _ = eventpb.Side_SIDE_BUY
