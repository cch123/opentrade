// Package transferledger persists the asset-service saga state machine
// defined in ADR-0057. It is the authority for "what stage is saga X at";
// the asset-journal Kafka topic is a downstream projection produced by
// asset-service AFTER a ledger transition commits.
//
// The package owns one table, transfer_ledger (schema.sql), and exposes
// four operations the saga driver needs:
//
//   - Create         — insert a new saga in INIT state (with idempotent
//                       read-back on duplicate transfer_id).
//   - Get            — point lookup by transfer_id.
//   - UpdateState    — optimistic-lock transition (WHERE state = from).
//   - ListPending    — recover in-flight sagas on service startup.
//
// pkg/transferledger intentionally does NOT import a MySQL driver. The
// binary linking this package must register one (e.g.
// `import _ "github.com/go-sql-driver/mysql"`).
package transferledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// State is the saga state, stored as a string in MySQL so joins / ad-hoc
// queries stay readable. Values match the SagaStateChangeEvent.old_state
// / new_state strings in api/event/asset_journal.proto and the SagaState
// enum names (minus prefix) in api/rpc/asset/asset.proto.
type State string

// Saga state machine. See ADR-0057 §4.
//
// Transitions:
//
//	INIT          → DEBITED | FAILED
//	DEBITED       → COMPLETED | COMPENSATING
//	COMPENSATING  → COMPENSATED | COMPENSATE_STUCK
//
// Terminal: COMPLETED, FAILED, COMPENSATED, COMPENSATE_STUCK.
const (
	StateInit            State = "INIT"
	StateDebited         State = "DEBITED"
	StateCompleted       State = "COMPLETED"
	StateFailed          State = "FAILED"
	StateCompensating    State = "COMPENSATING"
	StateCompensated     State = "COMPENSATED"
	StateCompensateStuck State = "COMPENSATE_STUCK"
)

// IsTerminal reports whether the state is a saga terminal (no further
// driver work needed beyond operator intervention for STUCK).
func (s State) IsTerminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateCompensated, StateCompensateStuck:
		return true
	}
	return false
}

// Entry mirrors one row in transfer_ledger.
type Entry struct {
	TransferID   string
	UserID       string
	FromBiz      string
	ToBiz        string
	Asset        string
	Amount       string
	State        State
	RejectReason string
	CreatedAtMs  int64
	UpdatedAtMs  int64
}

// Sentinel errors surfaced to callers.
var (
	// ErrAlreadyExists is returned by Create when the transfer_id collides
	// with an existing row. The existing row (read back inside Create) is
	// returned as the first return value so the caller can short-circuit
	// to idempotent behaviour.
	ErrAlreadyExists = errors.New("transferledger: already exists")

	// ErrNotFound is returned by Get / UpdateState when transfer_id is
	// unknown.
	ErrNotFound = errors.New("transferledger: not found")

	// ErrStateMismatch is returned by UpdateState when the row's current
	// state != the caller's `from` argument — i.e. another goroutine /
	// instance advanced the saga first (optimistic lock lost).
	ErrStateMismatch = errors.New("transferledger: state mismatch")
)

// Clock is the pluggable time source; production uses time.Now.
type Clock func() time.Time

// Ledger is the MySQL-backed saga state store. Safe for concurrent use.
type Ledger struct {
	db    *sql.DB
	clock Clock
}

// Config tunes the pool. Zero-valued fields fall back to Default* below.
type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// Defaults. Match history/mysqlstore's posture.
const (
	DefaultMaxOpen         = 16
	DefaultMaxIdle         = 4
	DefaultConnMaxLifetime = 30 * time.Minute
)

// NewLedger opens a new *sql.DB with the given DSN. The caller must
// ensure a MySQL driver is registered in the process.
func NewLedger(cfg Config) (*Ledger, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("transferledger: empty DSN")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("transferledger: open: %w", err)
	}
	applyPoolDefaults(db, cfg)
	return &Ledger{db: db, clock: time.Now}, nil
}

// NewLedgerWithDB wraps a pre-configured *sql.DB. Used by tests with
// sqlmock and by callers that share a pool with other components.
func NewLedgerWithDB(db *sql.DB) *Ledger {
	return &Ledger{db: db, clock: time.Now}
}

// SetClock overrides the time source. Intended for tests.
func (l *Ledger) SetClock(c Clock) {
	if c != nil {
		l.clock = c
	}
}

// Close releases the underlying pool.
func (l *Ledger) Close() error { return l.db.Close() }

// Ping reports connection health.
func (l *Ledger) Ping(ctx context.Context) error { return l.db.PingContext(ctx) }

func applyPoolDefaults(db *sql.DB, cfg Config) {
	open := cfg.MaxOpenConns
	if open <= 0 {
		open = DefaultMaxOpen
	}
	idle := cfg.MaxIdleConns
	if idle <= 0 {
		idle = DefaultMaxIdle
	}
	life := cfg.ConnMaxLifetime
	if life <= 0 {
		life = DefaultConnMaxLifetime
	}
	db.SetMaxOpenConns(open)
	db.SetMaxIdleConns(idle)
	db.SetConnMaxLifetime(life)
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

// Create inserts a fresh saga row in state INIT. The caller-supplied
// Entry.State / CreatedAtMs / UpdatedAtMs fields are ignored — Create
// forces State=INIT and stamps the timestamps from the ledger's clock.
//
// If a row with the same transfer_id already exists (e.g. the BFF is
// retrying a successful request), Create returns (existing, ErrAlready-
// Exists) so the caller can decide whether to treat it as a duplicate
// (return the prior outcome) or as an unexpected collision.
func (l *Ledger) Create(ctx context.Context, e Entry) (Entry, error) {
	now := l.clock().UnixMilli()
	e.State = StateInit
	e.CreatedAtMs = now
	e.UpdatedAtMs = now

	const q = `INSERT INTO transfer_ledger
		(transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := l.db.ExecContext(ctx, q,
		e.TransferID, e.UserID, e.FromBiz, e.ToBiz, e.Asset, e.Amount,
		string(e.State), e.RejectReason, e.CreatedAtMs, e.UpdatedAtMs,
	)
	if err == nil {
		return e, nil
	}
	if isDuplicateKey(err) {
		existing, gErr := l.Get(ctx, e.TransferID)
		if gErr != nil {
			return Entry{}, fmt.Errorf("%w (read-back failed: %v)", ErrAlreadyExists, gErr)
		}
		return existing, ErrAlreadyExists
	}
	return Entry{}, err
}

// isDuplicateKey matches MySQL error 1062 (duplicate key) by message
// inspection so this package stays free of a concrete driver dependency.
// Covers both go-sql-driver/mysql's "Error 1062: Duplicate entry ..."
// and the sqlmock-friendly "Duplicate entry ..." prefix.
func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") || strings.Contains(msg, "Error 1062")
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

// Get returns the entry for transfer_id. Returns ErrNotFound when
// missing.
func (l *Ledger) Get(ctx context.Context, transferID string) (Entry, error) {
	const q = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger WHERE transfer_id = ? LIMIT 1`
	row := l.db.QueryRowContext(ctx, q, transferID)
	e, err := scanEntry(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	return e, nil
}

// ---------------------------------------------------------------------------
// UpdateState
// ---------------------------------------------------------------------------

// UpdateState advances transfer_id from `from` to `to` atomically using
// `WHERE state = from` as the optimistic lock. rejectReason is persisted
// for states that carry a failure or in-flight-failure-cause signal
// (FAILED / COMPENSATE_STUCK / COMPENSATING) and cleared for the
// "happy" states (DEBITED / COMPLETED / COMPENSATED) so stale reasons
// don't leak into forward progress.
//
// Returns:
//   - nil                on success (exactly 1 row updated)
//   - ErrStateMismatch   when the current state != from (lost race)
//   - ErrNotFound        when transfer_id doesn't exist
func (l *Ledger) UpdateState(ctx context.Context, transferID string, from, to State, rejectReason string) error {
	if !stateCarriesReason(to) {
		rejectReason = ""
	}
	now := l.clock().UnixMilli()
	const q = `UPDATE transfer_ledger
		SET state = ?, reject_reason = ?, updated_at_ms = ?
		WHERE transfer_id = ? AND state = ?`
	res, err := l.db.ExecContext(ctx, q, string(to), rejectReason, now, transferID, string(from))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	// n == 0: either the row doesn't exist, or its current state is not
	// `from`. Disambiguate for the caller with a follow-up read.
	if _, gErr := l.Get(ctx, transferID); errors.Is(gErr, ErrNotFound) {
		return ErrNotFound
	}
	return ErrStateMismatch
}

// stateCarriesReason returns true for states where a reject_reason
// value is semantically meaningful and should be persisted. FAILED and
// COMPENSATE_STUCK are terminal failures; COMPENSATING is in-flight but
// MUST carry "why we're compensating" for debugging — otherwise the
// trigger cause is lost by the time it terminates as COMPENSATED.
func stateCarriesReason(s State) bool {
	switch s {
	case StateFailed, StateCompensateStuck, StateCompensating:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// ListPending
// ---------------------------------------------------------------------------

// ListPending returns up to `limit` non-terminal saga rows, ordered by
// updated_at_ms ASC (oldest first) so the driver picks up stuck sagas
// predictably on startup. If limit <= 0 a default cap of 200 applies.
func (l *Ledger) ListPending(ctx context.Context, limit int) ([]Entry, error) {
	if limit <= 0 {
		limit = 200
	}
	const q = `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger
		WHERE state IN ('INIT','DEBITED','COMPENSATING')
		ORDER BY updated_at_ms ASC
		LIMIT ?`
	rows, err := l.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// scan helper
// ---------------------------------------------------------------------------

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEntry(r rowScanner) (Entry, error) {
	var (
		e  Entry
		st string
	)
	if err := r.Scan(
		&e.TransferID, &e.UserID, &e.FromBiz, &e.ToBiz, &e.Asset, &e.Amount,
		&st, &e.RejectReason, &e.CreatedAtMs, &e.UpdatedAtMs,
	); err != nil {
		return Entry{}, err
	}
	e.State = State(st)
	return e, nil
}
