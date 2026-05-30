package mysqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

func TestListPerpPositions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewStoreWithDB(db, time.Second)

	rows := sqlmock.NewRows([]string{
		"user_id", "symbol", "side", "size", "entry_price", "margin", "leverage", "realized_pnl", "updated_at_ms",
	}).AddRow("u1", "BTC-USDT-PERP", int8(eventpb.Side_SIDE_BUY), "1", "100", "10", "10", "0", int64(1748505600000))
	mock.ExpectQuery("FROM perp_positions").WithArgs("u1").WillReturnRows(rows)

	out, err := store.ListPerpPositions(context.Background(), "u1", "")
	if err != nil {
		t.Fatalf("ListPerpPositions: %v", err)
	}
	if len(out) != 1 || out[0].Symbol != "BTC-USDT-PERP" || out[0].Side != eventpb.Side_SIDE_BUY ||
		out[0].Size != "1" || out[0].EntryPrice != "100" {
		t.Fatalf("position = %+v", out)
	}
}

func TestListPerpFunding_ProducesNextCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewStoreWithDB(db, time.Second)

	cols := []string{"perp_seq_id", "symbol", "funding_round_id", "funding_rate", "mark_price", "payment", "ts_unix_ms"}
	rows := sqlmock.NewRows(cols).
		AddRow(uint64(3), "BTC-USDT-PERP", "BTC-USDT-PERP:300", "0.01", "100", "-1", int64(300)).
		AddRow(uint64(2), "BTC-USDT-PERP", "BTC-USDT-PERP:200", "0.01", "100", "-1", int64(200)).
		AddRow(uint64(1), "BTC-USDT-PERP", "BTC-USDT-PERP:100", "0.01", "100", "-1", int64(100))
	mock.ExpectQuery("FROM perp_funding").WillReturnRows(rows)

	out, next, err := store.ListPerpFunding(context.Background(), PerpLedgerFilter{UserID: "u1"}, "", 2)
	if err != nil {
		t.Fatalf("ListPerpFunding: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("want 2 rows (limit), got %d", len(out))
	}
	if next == "" {
		t.Fatal("expected a next cursor when more rows exist")
	}
	if out[0].FundingRoundId != "BTC-USDT-PERP:300" || out[0].Payment != "-1" {
		t.Fatalf("funding row = %+v", out[0])
	}
}

func TestListPerpLiquidations(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewStoreWithDB(db, time.Second)

	cols := []string{"perp_seq_id", "symbol", "liq_order_id", "bankruptcy_price", "mark_price",
		"closed_qty", "realized_pnl", "insurance_delta", "adl_queued", "ts_unix_ms"}
	rows := sqlmock.NewRows(cols).
		AddRow(uint64(9), "BTC-USDT-PERP", uint64(200), "90", "89", "1", "-10", "-2", true, int64(900))
	mock.ExpectQuery("FROM perp_liquidations").WillReturnRows(rows)

	out, _, err := store.ListPerpLiquidations(context.Background(), PerpLedgerFilter{UserID: "u1"}, "", 50)
	if err != nil {
		t.Fatalf("ListPerpLiquidations: %v", err)
	}
	if len(out) != 1 || !out[0].AdlQueued || out[0].InsuranceDelta != "-2" || out[0].LiqOrderId != 200 {
		t.Fatalf("liquidation = %+v", out)
	}
}
