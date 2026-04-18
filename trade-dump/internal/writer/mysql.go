package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLConfig configures the MySQL connection pool.
type MySQLConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration

	// ChunkSize caps the number of rows bundled into a single multi-row
	// INSERT statement. Keeps us under max_allowed_packet and puts an upper
	// bound on MySQL lock duration.
	ChunkSize int
}

// MySQL writes projected rows to MySQL idempotently. Trade inserts use
// INSERT ... ON DUPLICATE KEY UPDATE so reprocessing after a Kafka offset
// rewind is a no-op (ADR-0008).
type MySQL struct {
	db        *sql.DB
	chunkSize int
}

const defaultChunkSize = 500

// NewMySQL opens the DSN, pings it, and returns a writer ready for use.
// The caller owns Close.
func NewMySQL(cfg MySQLConfig) (*MySQL, error) {
	if cfg.DSN == "" {
		return nil, errors.New("writer: MySQL DSN is required")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("sql.Open mysql: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql ping: %w", err)
	}
	return NewMySQLWithDB(db, cfg.ChunkSize), nil
}

// NewMySQLWithDB wraps an existing *sql.DB. Used by tests (sqlmock) and by
// callers that want to share a pool.
func NewMySQLWithDB(db *sql.DB, chunkSize int) *MySQL {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	return &MySQL{db: db, chunkSize: chunkSize}
}

// Close closes the underlying pool.
func (m *MySQL) Close() error { return m.db.Close() }

// Ping verifies connectivity.
func (m *MySQL) Ping(ctx context.Context) error { return m.db.PingContext(ctx) }

// InsertTrades writes rows to the trades table using a single transaction.
// Each chunk is a multi-row INSERT ... ON DUPLICATE KEY UPDATE trade_id =
// trade_id — no-op on conflict keeps the write idempotent when Kafka replays.
// Empty input is a no-op.
func (m *MySQL) InsertTrades(ctx context.Context, rows []TradeRow) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for start := 0; start < len(rows); start += m.chunkSize {
		end := start + m.chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := insertTradeChunk(ctx, tx, rows[start:end]); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit trades tx: %w", err)
	}
	committed = true
	return nil
}

const tradeColumns = "trade_id, symbol, price, qty, maker_user_id, maker_order_id, taker_user_id, taker_order_id, taker_side, ts, symbol_seq_id"

func insertTradeChunk(ctx context.Context, tx *sql.Tx, rows []TradeRow) error {
	placeholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*11)
	for i, r := range rows {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args,
			r.TradeID,
			r.Symbol,
			r.Price,
			r.Qty,
			r.MakerUserID,
			r.MakerOrderID,
			r.TakerUserID,
			r.TakerOrderID,
			r.TakerSide,
			r.TS,
			r.SymbolSeqID,
		)
	}
	query := "INSERT INTO trades (" + tradeColumns + ") VALUES " +
		strings.Join(placeholders, ", ") +
		" ON DUPLICATE KEY UPDATE trade_id = trade_id"
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert trades chunk: %w", err)
	}
	return nil
}
