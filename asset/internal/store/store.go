// Package store persists the funding wallet state owned by asset-service.
//
// ADR-0065 makes MySQL the authority for biz_line=funding balances. The
// funding_accounts table stores the latest balance, while funding_mutations
// provides durable idempotency and an audit trail for TransferOut,
// TransferIn, and CompensateTransferOut.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// Status is the funding holder outcome. Values intentionally mirror
// asset/internal/service.Status and api/rpc/assetholder.TransferStatus.
type Status int

const (
	StatusUnspecified Status = 0
	StatusConfirmed   Status = 1
	StatusRejected    Status = 2
	StatusDuplicated  Status = 3
)

// Operation labels one funding mutation.
type Operation string

const (
	OpTransferOut Operation = "transfer_out"
	OpTransferIn  Operation = "transfer_in"
	OpCompensate  Operation = "compensate"
)

const (
	mutationConfirmed = "CONFIRMED"
	mutationRejected  = "REJECTED"
)

// ErrIdempotencyConflict is returned when the same (transfer_id, op_type)
// is retried with a different request shape.
var ErrIdempotencyConflict = errors.New("funding store: idempotency conflict")

// Request is the store-level shape for a funding holder mutation.
type Request struct {
	UserID          string
	TransferID      string
	Asset           string
	Amount          dec.Decimal
	PeerBiz         string
	Memo            string
	CompensateCause string
}

// Result is the durable outcome of a funding holder mutation.
type Result struct {
	Status         Status
	RejectReason   error
	BalanceAfter   engine.Balance
	FundingVersion uint64
}

// FundingBalance is returned by QueryFundingBalance.
type FundingBalance struct {
	Asset   string
	Balance engine.Balance
}

// Store is a MySQL-backed funding wallet. It is safe for concurrent use.
type Store struct {
	db    *sql.DB
	clock func() time.Time
}

// Config tunes the MySQL pool. Zero-valued pool fields use conservative
// defaults shared with pkg/transferledger.
type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

const (
	defaultMaxOpen         = 16
	defaultMaxIdle         = 4
	defaultConnMaxLifetime = 30 * time.Minute
)

// New opens a Store. The process must have registered a MySQL driver.
func New(cfg Config) (*Store, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("funding store: empty DSN")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("funding store: open: %w", err)
	}
	applyPoolDefaults(db, cfg)
	return &Store{db: db, clock: time.Now}, nil
}

// NewWithDB wraps an existing *sql.DB. Used by tests with sqlmock.
func NewWithDB(db *sql.DB) *Store {
	return &Store{db: db, clock: time.Now}
}

// SetClock overrides the time source. Intended for tests.
func (s *Store) SetClock(c func() time.Time) {
	if c != nil {
		s.clock = c
	}
}

// Close releases the underlying pool.
func (s *Store) Close() error { return s.db.Close() }

// Ping reports connection health.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func applyPoolDefaults(db *sql.DB, cfg Config) {
	open := cfg.MaxOpenConns
	if open <= 0 {
		open = defaultMaxOpen
	}
	idle := cfg.MaxIdleConns
	if idle <= 0 {
		idle = defaultMaxIdle
	}
	life := cfg.ConnMaxLifetime
	if life <= 0 {
		life = defaultConnMaxLifetime
	}
	db.SetMaxOpenConns(open)
	db.SetMaxIdleConns(idle)
	db.SetConnMaxLifetime(life)
}

// TransferOut debits available balance. Insufficient balance is a durable
// REJECTED mutation so retries observe the same terminal outcome.
func (s *Store) TransferOut(ctx context.Context, req Request) (Result, error) {
	return s.apply(ctx, OpTransferOut, req)
}

// TransferIn credits available balance.
func (s *Store) TransferIn(ctx context.Context, req Request) (Result, error) {
	return s.apply(ctx, OpTransferIn, req)
}

// Compensate credits available balance using a distinct op_type so it is
// idempotent independently from the original TransferOut.
func (s *Store) Compensate(ctx context.Context, req Request) (Result, error) {
	return s.apply(ctx, OpCompensate, req)
}

func (s *Store) apply(ctx context.Context, op Operation, req Request) (Result, error) {
	if err := validate(req); err != nil {
		return Result{Status: StatusRejected, RejectReason: err}, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	done := false
	defer func() {
		if !done {
			_ = tx.Rollback()
		}
	}()

	existing, found, err := getMutation(ctx, tx, req.TransferID, op)
	if err != nil {
		return Result{}, err
	}
	if found {
		if !existing.sameRequest(req) {
			return Result{}, fmt.Errorf("%w: transfer_id=%s op_type=%s", ErrIdempotencyConflict, req.TransferID, op)
		}
		if err := tx.Commit(); err != nil {
			return Result{}, err
		}
		done = true
		return existing.toResult()
	}

	now := s.clock().UnixMilli()
	fundingVersion, err := lockFundingUser(ctx, tx, req.UserID, now)
	if err != nil {
		return Result{}, err
	}
	bal, err := lockFundingAccount(ctx, tx, req.UserID, req.Asset, now)
	if err != nil {
		return Result{}, err
	}

	status := mutationConfirmed
	var reject error
	next := bal
	nextFundingVersion := fundingVersion
	if op == OpTransferOut && bal.Available.Cmp(req.Amount) < 0 {
		status = mutationRejected
		reject = engine.ErrInsufficientAvailable
	} else {
		switch op {
		case OpTransferOut:
			next.Available = bal.Available.Sub(req.Amount)
		case OpTransferIn, OpCompensate:
			next.Available = bal.Available.Add(req.Amount)
		default:
			return Result{}, fmt.Errorf("funding store: unknown op %q", op)
		}
		next.Version = bal.Version + 1
		nextFundingVersion = fundingVersion + 1
		if err := updateFundingAccount(ctx, tx, req.UserID, req.Asset, next, now); err != nil {
			return Result{}, err
		}
		if err := updateFundingUser(ctx, tx, req.UserID, nextFundingVersion, now); err != nil {
			return Result{}, err
		}
	}

	m := mutationRow{
		TransferID:      req.TransferID,
		OpType:          op,
		UserID:          req.UserID,
		Asset:           req.Asset,
		Amount:          req.Amount,
		PeerBiz:         req.PeerBiz,
		Memo:            req.Memo,
		Status:          status,
		RejectReason:    rejectString(reject),
		AvailableAfter:  next.Available,
		FrozenAfter:     next.Frozen,
		FundingVersion:  nextFundingVersion,
		BalanceVersion:  next.Version,
		CreatedAtUnixMS: now,
	}
	if err := insertMutation(ctx, tx, m); err != nil {
		if isDuplicateKey(err) {
			_ = tx.Rollback()
			done = true
			return s.apply(ctx, op, req)
		}
		return Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	done = true

	return Result{
		Status:         statusFromMutation(status),
		RejectReason:   reject,
		BalanceAfter:   next,
		FundingVersion: nextFundingVersion,
	}, nil
}

// QueryFundingBalance returns current funding balances. When asset is set
// and the row is missing, it returns a single zero-valued balance for shape
// compatibility with counter's balance query.
func (s *Store) QueryFundingBalance(ctx context.Context, userID, asset string) ([]FundingBalance, error) {
	if userID == "" {
		return nil, engine.ErrMissingUserID
	}
	if asset != "" {
		const q = `SELECT available, frozen, balance_version
			FROM funding_accounts WHERE user_id = ? AND asset = ? LIMIT 1`
		var (
			avail  string
			frozen string
			ver    uint64
		)
		err := s.db.QueryRowContext(ctx, q, userID, asset).Scan(&avail, &frozen, &ver)
		if errors.Is(err, sql.ErrNoRows) {
			return []FundingBalance{{Asset: asset}}, nil
		}
		if err != nil {
			return nil, err
		}
		bal, err := balanceFromStrings(avail, frozen, ver)
		if err != nil {
			return nil, err
		}
		return []FundingBalance{{Asset: asset, Balance: bal}}, nil
	}

	const q = `SELECT asset, available, frozen, balance_version
		FROM funding_accounts WHERE user_id = ? ORDER BY asset`
	rows, err := s.db.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FundingBalance
	for rows.Next() {
		var (
			a      string
			avail  string
			frozen string
			ver    uint64
		)
		if err := rows.Scan(&a, &avail, &frozen, &ver); err != nil {
			return nil, err
		}
		bal, err := balanceFromStrings(avail, frozen, ver)
		if err != nil {
			return nil, err
		}
		out = append(out, FundingBalance{Asset: a, Balance: bal})
	}
	return out, rows.Err()
}

func validate(req Request) error {
	if req.UserID == "" {
		return engine.ErrMissingUserID
	}
	if req.TransferID == "" {
		return engine.ErrMissingTransferID
	}
	if req.Asset == "" {
		return engine.ErrMissingAsset
	}
	if req.Amount.Sign() <= 0 {
		return engine.ErrInvalidAmount
	}
	return nil
}

const selectMutationQ = `SELECT transfer_id, op_type, user_id, asset, amount, peer_biz, memo, status, reject_reason, available_after, frozen_after, funding_version, balance_version, created_at_ms
	FROM funding_mutations WHERE transfer_id = ? AND op_type = ? LIMIT 1 FOR UPDATE`

func getMutation(ctx context.Context, tx *sql.Tx, transferID string, op Operation) (mutationRow, bool, error) {
	row := tx.QueryRowContext(ctx, selectMutationQ, transferID, string(op))
	m, err := scanMutation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return mutationRow{}, false, nil
	}
	if err != nil {
		return mutationRow{}, false, err
	}
	return m, true, nil
}

func lockFundingUser(ctx context.Context, tx *sql.Tx, userID string, now int64) (uint64, error) {
	const insertQ = `INSERT IGNORE INTO funding_users (user_id, funding_version, updated_at_ms)
		VALUES (?, ?, ?)`
	if _, err := tx.ExecContext(ctx, insertQ, userID, uint64(0), now); err != nil {
		return 0, err
	}
	const selectQ = `SELECT funding_version FROM funding_users WHERE user_id = ? FOR UPDATE`
	var version uint64
	if err := tx.QueryRowContext(ctx, selectQ, userID).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func lockFundingAccount(ctx context.Context, tx *sql.Tx, userID, asset string, now int64) (engine.Balance, error) {
	const insertQ = `INSERT IGNORE INTO funding_accounts (user_id, asset, available, frozen, balance_version, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, insertQ, userID, asset, dec.Zero.String(), dec.Zero.String(), uint64(0), now); err != nil {
		return engine.Balance{}, err
	}
	const selectQ = `SELECT available, frozen, balance_version FROM funding_accounts
		WHERE user_id = ? AND asset = ? FOR UPDATE`
	var (
		avail  string
		frozen string
		ver    uint64
	)
	if err := tx.QueryRowContext(ctx, selectQ, userID, asset).Scan(&avail, &frozen, &ver); err != nil {
		return engine.Balance{}, err
	}
	return balanceFromStrings(avail, frozen, ver)
}

func updateFundingAccount(ctx context.Context, tx *sql.Tx, userID, asset string, bal engine.Balance, now int64) error {
	const q = `UPDATE funding_accounts
		SET available = ?, frozen = ?, balance_version = ?, updated_at_ms = ?
		WHERE user_id = ? AND asset = ?`
	_, err := tx.ExecContext(ctx, q, bal.Available.String(), bal.Frozen.String(), bal.Version, now, userID, asset)
	return err
}

func updateFundingUser(ctx context.Context, tx *sql.Tx, userID string, version uint64, now int64) error {
	const q = `UPDATE funding_users
		SET funding_version = ?, updated_at_ms = ?
		WHERE user_id = ?`
	_, err := tx.ExecContext(ctx, q, version, now, userID)
	return err
}

const insertMutationQ = `INSERT INTO funding_mutations
	(transfer_id, op_type, user_id, asset, amount, peer_biz, memo, status, reject_reason, available_after, frozen_after, funding_version, balance_version, created_at_ms)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func insertMutation(ctx context.Context, tx *sql.Tx, m mutationRow) error {
	_, err := tx.ExecContext(ctx, insertMutationQ,
		m.TransferID, string(m.OpType), m.UserID, m.Asset, m.Amount.String(),
		m.PeerBiz, m.Memo, m.Status, m.RejectReason,
		m.AvailableAfter.String(), m.FrozenAfter.String(),
		m.FundingVersion, m.BalanceVersion, m.CreatedAtUnixMS,
	)
	return err
}

type mutationRow struct {
	TransferID      string
	OpType          Operation
	UserID          string
	Asset           string
	Amount          dec.Decimal
	PeerBiz         string
	Memo            string
	Status          string
	RejectReason    string
	AvailableAfter  dec.Decimal
	FrozenAfter     dec.Decimal
	FundingVersion  uint64
	BalanceVersion  uint64
	CreatedAtUnixMS int64
}

func scanMutation(r rowScanner) (mutationRow, error) {
	var (
		m              mutationRow
		op             string
		amount         string
		availableAfter string
		frozenAfter    string
	)
	if err := r.Scan(
		&m.TransferID, &op, &m.UserID, &m.Asset, &amount, &m.PeerBiz,
		&m.Memo, &m.Status, &m.RejectReason, &availableAfter, &frozenAfter,
		&m.FundingVersion, &m.BalanceVersion, &m.CreatedAtUnixMS,
	); err != nil {
		return mutationRow{}, err
	}
	amt, err := dec.Parse(amount)
	if err != nil {
		return mutationRow{}, fmt.Errorf("parse mutation amount: %w", err)
	}
	avail, err := dec.Parse(availableAfter)
	if err != nil {
		return mutationRow{}, fmt.Errorf("parse mutation available_after: %w", err)
	}
	frozen, err := dec.Parse(frozenAfter)
	if err != nil {
		return mutationRow{}, fmt.Errorf("parse mutation frozen_after: %w", err)
	}
	m.OpType = Operation(op)
	m.Amount = amt
	m.AvailableAfter = avail
	m.FrozenAfter = frozen
	return m, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func (m mutationRow) sameRequest(req Request) bool {
	return m.UserID == req.UserID &&
		m.Asset == req.Asset &&
		m.Amount.Cmp(req.Amount) == 0 &&
		m.PeerBiz == req.PeerBiz &&
		m.Memo == req.Memo
}

func (m mutationRow) toResult() (Result, error) {
	bal := engine.Balance{
		Available: m.AvailableAfter,
		Frozen:    m.FrozenAfter,
		Version:   m.BalanceVersion,
	}
	switch m.Status {
	case mutationConfirmed:
		return Result{Status: StatusDuplicated, BalanceAfter: bal, FundingVersion: m.FundingVersion}, nil
	case mutationRejected:
		return Result{
			Status:         StatusRejected,
			RejectReason:   rejectError(m.RejectReason),
			BalanceAfter:   bal,
			FundingVersion: m.FundingVersion,
		}, nil
	default:
		return Result{}, fmt.Errorf("funding store: unknown mutation status %q", m.Status)
	}
}

func statusFromMutation(status string) Status {
	if status == mutationRejected {
		return StatusRejected
	}
	return StatusConfirmed
}

func rejectString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func rejectError(reason string) error {
	switch reason {
	case "":
		return nil
	case engine.ErrInsufficientAvailable.Error():
		return engine.ErrInsufficientAvailable
	case engine.ErrInvalidAmount.Error():
		return engine.ErrInvalidAmount
	default:
		return errors.New(reason)
	}
}

func balanceFromStrings(available, frozen string, version uint64) (engine.Balance, error) {
	avail, err := dec.Parse(available)
	if err != nil {
		return engine.Balance{}, fmt.Errorf("parse available: %w", err)
	}
	frz, err := dec.Parse(frozen)
	if err != nil {
		return engine.Balance{}, fmt.Errorf("parse frozen: %w", err)
	}
	return engine.Balance{Available: avail, Frozen: frz, Version: version}, nil
}

func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") || strings.Contains(msg, "Error 1062")
}
