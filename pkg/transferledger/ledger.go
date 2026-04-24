// Package transferledger persists the asset-service saga state machine
// defined in ADR-0057. It is the authority for "what stage is saga X at";
// funding wallet balances are owned separately by ADR-0065's MySQL store.
//
// The package owns one table, transfer_ledger (schema.sql), and exposes
// the operations the saga driver and asset-service gRPC need:
//
//   - Create         — insert a new saga in INIT state (with idempotent
//     read-back on duplicate transfer_id).
//   - Get            — point lookup by transfer_id.
//   - UpdateState    — optimistic-lock transition (WHERE state = from).
//   - ListPending    — recover in-flight sagas on service startup.
//   - List           — user-scoped paged history (BFF ListTransfers, post
//     ADR-0065 this replaced the trade-dump `transfers` projection).
//
// pkg/transferledger intentionally does NOT import a MySQL driver. The
// binary linking this package must register one (e.g.
// `import _ "github.com/go-sql-driver/mysql"`).
package transferledger

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// State is the saga state, stored as a string in MySQL so joins / ad-hoc
// queries stay readable. Values match the SagaState enum names (minus
// prefix) in api/rpc/asset/asset.proto.
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
// CountByState
// ---------------------------------------------------------------------------

// CountByState returns how many ledger rows exist in each State. States
// never observed get a zero entry so callers can reset a Prometheus
// gauge without leaving stale labels. The returned map is always sized
// to len(AllStates) — missing rows in the DB become 0s in the map.
func (l *Ledger) CountByState(ctx context.Context) (map[State]int64, error) {
	const q = `SELECT state, COUNT(*) FROM transfer_ledger GROUP BY state`
	rows, err := l.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[State]int64, len(AllStates))
	for _, s := range AllStates {
		out[s] = 0
	}
	for rows.Next() {
		var (
			st    string
			count int64
		)
		if err := rows.Scan(&st, &count); err != nil {
			return nil, err
		}
		out[State(st)] = count
	}
	return out, rows.Err()
}

// AllStates lists every State value in canonical order. Exported so
// callers (e.g. metrics) can iterate without duplicating the list.
var AllStates = []State{
	StateInit,
	StateDebited,
	StateCompleted,
	StateFailed,
	StateCompensating,
	StateCompensated,
	StateCompensateStuck,
}

// InFlightStates are the non-terminal saga states — what "in flight"
// folds to for ListFilter.States when the caller passes ScopeInFlight.
var InFlightStates = []State{
	StateInit, StateDebited, StateCompensating,
}

// TerminalStates are the terminal saga states.
var TerminalStates = []State{
	StateCompleted, StateFailed, StateCompensated, StateCompensateStuck,
}

// ---------------------------------------------------------------------------
// List (paged user history)
// ---------------------------------------------------------------------------

// ListFilter is the decoded ListTransfersRequest for the store. Empty
// string filters skip the corresponding WHERE clause; zero timestamps
// skip the since/until bounds. States takes raw State values — the
// scope→states fold lives in asset-service's server.
type ListFilter struct {
	UserID  string
	FromBiz string
	ToBiz   string
	Asset   string
	States  []State
	SinceMs int64
	UntilMs int64
}

// Cursor captures the last-seen row for (created_at_ms DESC,
// transfer_id DESC) pagination. transfer_id is the client-supplied
// idempotency key and is globally unique, so the pair is strictly
// monotone.
type Cursor struct {
	CreatedAt  int64  `json:"c"`
	TransferID string `json:"t"`
}

var cursorEnc = base64.RawURLEncoding

// EncodeCursor marshals a Cursor for on-wire use.
func EncodeCursor(c Cursor) (string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return cursorEnc.EncodeToString(raw), nil
}

// DecodeCursor parses a Cursor from the on-wire form. Empty string is
// valid and returns the zero Cursor.
func DecodeCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	raw, err := cursorEnc.DecodeString(s)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	var c Cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	return c, nil
}

// ErrInvalidCursor is returned by DecodeCursor / List when the cursor
// string cannot be parsed.
var ErrInvalidCursor = errors.New("transferledger: invalid cursor")

// DefaultListLimit caps page size when the caller passes <= 0.
const DefaultListLimit = 50

// MaxListLimit is the hard cap per page.
const MaxListLimit = 200

// List returns up to `limit` rows for a user, ordered newest-first by
// (created_at_ms DESC, transfer_id DESC). It over-fetches one row to
// tell the caller whether a next page exists.
//
// When there is a next page, the returned Cursor captures the LAST row
// of the returned slice so the caller can pass it straight back in.
// When exhausted, Cursor is the zero value.
func (l *Ledger) List(ctx context.Context, f ListFilter, raw string, limit int) ([]Entry, Cursor, error) {
	if f.UserID == "" {
		return nil, Cursor{}, fmt.Errorf("transferledger: user_id required")
	}
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}
	cur, err := DecodeCursor(raw)
	if err != nil {
		return nil, Cursor{}, err
	}

	conds := []string{"user_id = ?"}
	args := []any{f.UserID}
	if f.FromBiz != "" {
		conds = append(conds, "from_biz = ?")
		args = append(args, f.FromBiz)
	}
	if f.ToBiz != "" {
		conds = append(conds, "to_biz = ?")
		args = append(args, f.ToBiz)
	}
	if f.Asset != "" {
		conds = append(conds, "asset = ?")
		args = append(args, f.Asset)
	}
	if len(f.States) > 0 {
		placeholders := make([]string, len(f.States))
		for i, st := range f.States {
			placeholders[i] = "?"
			args = append(args, string(st))
		}
		conds = append(conds, "state IN ("+strings.Join(placeholders, ",")+")")
	}
	if f.SinceMs > 0 {
		conds = append(conds, "created_at_ms >= ?")
		args = append(args, f.SinceMs)
	}
	if f.UntilMs > 0 {
		conds = append(conds, "created_at_ms < ?")
		args = append(args, f.UntilMs)
	}
	if raw != "" {
		conds = append(conds,
			"(created_at_ms < ? OR (created_at_ms = ? AND transfer_id < ?))")
		args = append(args, cur.CreatedAt, cur.CreatedAt, cur.TransferID)
	}

	q := `SELECT transfer_id, user_id, from_biz, to_biz, asset, amount, state, reject_reason, created_at_ms, updated_at_ms
		FROM transfer_ledger
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY created_at_ms DESC, transfer_id DESC
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := l.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, Cursor{}, err
	}
	defer rows.Close()

	out := make([]Entry, 0, limit+1)
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, Cursor{}, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, Cursor{}, err
	}

	var next Cursor
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = Cursor{CreatedAt: last.CreatedAtMs, TransferID: last.TransferID}
	}
	return out, next, nil
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
