// Package saga drives the cross-biz_line transfer state machine
// defined in ADR-0057 §4. It reads/writes the transfer_ledger table
// through pkg/transferledger, calls the two legs through holder.Client
// from the biz_line registry, and records state transitions in MySQL.
//
// The driver has one public method: Run. Start (called once from main)
// drives a freshly-created ledger row through to a terminal state;
// Recover replays pending ledger rows on startup.
//
// Concurrency model:
//
//   - Each saga is driven by a single goroutine from INIT to terminal.
//     Concurrency between sagas is unbounded (different transfer_ids
//     never conflict on any shared state beyond per-user holder locks).
//   - Callers hand off ownership of a ledger row to Run and MUST NOT
//     touch it until Run returns. Re-entering Run on the same ledger
//     row from two goroutines is a bug — Recover protects against this
//     by only surfacing pending rows once per restart.
//
// Transport failures on the holder RPCs are retried with exponential
// backoff before the driver decides to FAIL (on the INIT→DEBITED leg,
// since no side effect has landed) or COMPENSATE (on DEBITED→COMPLETED,
// since from-side funds are already debited). Compensation that
// exceeds MaxCompensateRetries enters COMPENSATE_STUCK and must be
// resolved by an operator — the funds sit at the `from` biz_line until
// then.
package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/asset/internal/holder"
	"github.com/xargin/opentrade/asset/internal/metrics"
	"github.com/xargin/opentrade/pkg/transferledger"
)

// Config tunes retry behaviour. Zero-valued fields fall back to the
// Default* constants below so callers can embed Config{} without
// filling every field.
type Config struct {
	// RPCTimeout bounds each individual holder call (TransferOut /
	// TransferIn / CompensateTransferOut).
	RPCTimeout time.Duration

	// ForwardRetries is the retry budget for transport failures on
	// the forward legs (TransferOut / TransferIn). On exhaustion the
	// driver either FAILs (on TransferOut) or COMPENSATEs (on
	// TransferIn).
	ForwardRetries int

	// ForwardBackoff is the per-retry delay on forward legs; the
	// driver multiplies it by attempt number for a simple linear
	// backoff.
	ForwardBackoff time.Duration

	// CompensateRetries bounds retries of CompensateTransferOut
	// before the saga enters COMPENSATE_STUCK.
	CompensateRetries int

	// CompensateBackoff initial delay for compensate retries (doubles
	// per attempt up to a hard cap).
	CompensateBackoff time.Duration
}

// Defaults.
const (
	DefaultRPCTimeout        = 5 * time.Second
	DefaultForwardRetries    = 3
	DefaultForwardBackoff    = 500 * time.Millisecond
	DefaultCompensateRetries = 10
	DefaultCompensateBackoff = 1 * time.Second
	MaxCompensateBackoff     = 60 * time.Second
)

// Driver runs sagas. One process holds one Driver; concurrent Run calls
// on different transfer_ids are safe.
type Driver struct {
	cfg      Config
	ledger   *transferledger.Ledger
	registry *holder.Registry
	logger   *zap.Logger
	metrics  *metrics.Saga
	clock    func() time.Time
	sleep    func(context.Context, time.Duration)
}

// New wires a Driver. metrics may be nil — in that case the driver
// operates with no-op telemetry (useful for tests that don't care).
func New(cfg Config, ledger *transferledger.Ledger, registry *holder.Registry, logger *zap.Logger, m *metrics.Saga) *Driver {
	applyDefaults(&cfg)
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Driver{
		cfg:      cfg,
		ledger:   ledger,
		registry: registry,
		logger:   logger,
		metrics:  m,
		clock:    time.Now,
		sleep:    sleepWithCtx,
	}
}

// SetClock overrides the time source; tests only.
func (d *Driver) SetClock(c func() time.Time) {
	if c != nil {
		d.clock = c
	}
}

// SetSleep overrides the sleep helper; tests only — pass a no-op to
// skip backoff waits.
func (d *Driver) SetSleep(s func(context.Context, time.Duration)) {
	if s != nil {
		d.sleep = s
	}
}

func applyDefaults(cfg *Config) {
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = DefaultRPCTimeout
	}
	if cfg.ForwardRetries <= 0 {
		cfg.ForwardRetries = DefaultForwardRetries
	}
	if cfg.ForwardBackoff <= 0 {
		cfg.ForwardBackoff = DefaultForwardBackoff
	}
	if cfg.CompensateRetries <= 0 {
		cfg.CompensateRetries = DefaultCompensateRetries
	}
	if cfg.CompensateBackoff <= 0 {
		cfg.CompensateBackoff = DefaultCompensateBackoff
	}
}

// Run drives entry from its current state to a terminal state,
// returning the final ledger row. It is the single public entry point
// used by both Transfer (fresh sagas) and Recover (resumed sagas).
//
// On transport / infra errors the driver retries with backoff; only
// unrecoverable errors (e.g. ledger gone, registry missing) surface
// through the returned error. A returned non-terminal State is a bug —
// the driver MUST decide one way or the other.
func (d *Driver) Run(ctx context.Context, entry transferledger.Entry) (transferledger.Entry, error) {
	for !entry.State.IsTerminal() {
		next, err := d.step(ctx, entry)
		if err != nil {
			return entry, err
		}
		entry = next
	}
	return entry, nil
}

// step advances the saga by exactly one state transition.
func (d *Driver) step(ctx context.Context, entry transferledger.Entry) (transferledger.Entry, error) {
	switch entry.State {
	case transferledger.StateInit:
		return d.doDebit(ctx, entry)
	case transferledger.StateDebited:
		return d.doCredit(ctx, entry)
	case transferledger.StateCompensating:
		return d.doCompensate(ctx, entry)
	default:
		return entry, fmt.Errorf("saga: unexpected state %q (transfer_id=%s)", entry.State, entry.TransferID)
	}
}

// ---------------------------------------------------------------------------
// INIT → DEBITED / FAILED
// ---------------------------------------------------------------------------

// doDebit calls from.TransferOut. The "from" side hasn't been touched
// yet, so transport failures can freely turn into FAILED without
// needing compensation.
func (d *Driver) doDebit(ctx context.Context, entry transferledger.Entry) (transferledger.Entry, error) {
	fromHolder, err := d.registry.Get(entry.FromBiz)
	if err != nil {
		return d.transition(ctx, entry, transferledger.StateFailed, "unknown_from_biz")
	}
	req := holder.Request{
		UserID:     entry.UserID,
		TransferID: entry.TransferID,
		Asset:      entry.Asset,
		Amount:     entry.Amount,
		PeerBiz:    entry.ToBiz,
		Memo:       "",
	}
	res, callErr := d.callWithRetry(ctx, d.cfg.ForwardRetries, d.cfg.ForwardBackoff, "TransferOut", func(cctx context.Context) (holder.Result, error) {
		return fromHolder.TransferOut(cctx, req)
	})
	if callErr != nil {
		// Exhausted forward retries with transport failure: the from
		// side may or may not have applied the debit. The safe play
		// is to assume NOT debited (from didn't ack) and fail the
		// saga. If the debit silently landed, the from holder has it
		// recorded by transfer_id and a future saga with the same id
		// would see DUPLICATED, not a second debit.
		d.logger.Warn("saga: TransferOut transport failed; marking FAILED",
			zap.String("transfer_id", entry.TransferID),
			zap.Error(callErr))
		return d.transition(ctx, entry, transferledger.StateFailed, "from_transport_error")
	}
	switch res.Status {
	case holder.StatusConfirmed, holder.StatusDuplicated:
		return d.transition(ctx, entry, transferledger.StateDebited, "")
	case holder.StatusRejected:
		return d.transition(ctx, entry, transferledger.StateFailed, reasonString(res))
	}
	return d.transition(ctx, entry, transferledger.StateFailed, "from_unexpected_status")
}

// ---------------------------------------------------------------------------
// DEBITED → COMPLETED / COMPENSATING
// ---------------------------------------------------------------------------

// doCredit calls to.TransferIn. from side is already debited, so any
// failure path must transition to COMPENSATING (not FAILED) so the
// funds find their way back.
func (d *Driver) doCredit(ctx context.Context, entry transferledger.Entry) (transferledger.Entry, error) {
	toHolder, err := d.registry.Get(entry.ToBiz)
	if err != nil {
		d.logger.Error("saga: to biz_line missing; starting compensation",
			zap.String("transfer_id", entry.TransferID),
			zap.String("to_biz", entry.ToBiz))
		return d.transition(ctx, entry, transferledger.StateCompensating, "unknown_to_biz")
	}
	req := holder.Request{
		UserID:     entry.UserID,
		TransferID: entry.TransferID,
		Asset:      entry.Asset,
		Amount:     entry.Amount,
		PeerBiz:    entry.FromBiz,
	}
	res, callErr := d.callWithRetry(ctx, d.cfg.ForwardRetries, d.cfg.ForwardBackoff, "TransferIn", func(cctx context.Context) (holder.Result, error) {
		return toHolder.TransferIn(cctx, req)
	})
	if callErr != nil {
		d.logger.Warn("saga: TransferIn transport failed; starting compensation",
			zap.String("transfer_id", entry.TransferID),
			zap.Error(callErr))
		return d.transition(ctx, entry, transferledger.StateCompensating, "to_transport_error")
	}
	switch res.Status {
	case holder.StatusConfirmed, holder.StatusDuplicated:
		return d.transition(ctx, entry, transferledger.StateCompleted, "")
	case holder.StatusRejected:
		d.logger.Warn("saga: TransferIn rejected; compensating",
			zap.String("transfer_id", entry.TransferID),
			zap.String("reason", reasonString(res)))
		return d.transition(ctx, entry, transferledger.StateCompensating, reasonString(res))
	}
	return d.transition(ctx, entry, transferledger.StateCompensating, "to_unexpected_status")
}

// ---------------------------------------------------------------------------
// COMPENSATING → COMPENSATED / COMPENSATE_STUCK
// ---------------------------------------------------------------------------

// doCompensate calls from.CompensateTransferOut with doubling backoff
// up to MaxCompensateBackoff, bounded by CompensateRetries. Exhaustion
// → COMPENSATE_STUCK (operator intervention needed).
func (d *Driver) doCompensate(ctx context.Context, entry transferledger.Entry) (transferledger.Entry, error) {
	fromHolder, err := d.registry.Get(entry.FromBiz)
	if err != nil {
		// The from biz_line vanished between DEBIT and COMPENSATE —
		// we cannot return funds without operator action.
		d.logger.Error("saga: from biz_line missing during compensate; STUCK",
			zap.String("transfer_id", entry.TransferID),
			zap.String("from_biz", entry.FromBiz))
		return d.transition(ctx, entry, transferledger.StateCompensateStuck, "unknown_from_biz_during_compensate")
	}
	req := holder.Request{
		UserID:          entry.UserID,
		TransferID:      entry.TransferID,
		Asset:           entry.Asset,
		Amount:          entry.Amount,
		PeerBiz:         entry.ToBiz,
		CompensateCause: entry.RejectReason, // carry forward reason
	}
	backoff := d.cfg.CompensateBackoff
	var lastErr error
	for attempt := 1; attempt <= d.cfg.CompensateRetries; attempt++ {
		if d.metrics != nil {
			d.metrics.CompensateAttempts.Inc()
		}
		res, callErr := d.callWithRetry(ctx, 1, 0, "CompensateTransferOut", func(cctx context.Context) (holder.Result, error) {
			return fromHolder.CompensateTransferOut(cctx, req)
		})
		if callErr == nil {
			switch res.Status {
			case holder.StatusConfirmed, holder.StatusDuplicated:
				return d.transition(ctx, entry, transferledger.StateCompensated, "")
			case holder.StatusRejected:
				// Business rejecting a compensate is very unusual
				// (it should always be a valid credit) but we must
				// treat it as unrecoverable — loop retry cannot fix
				// a semantic rejection.
				d.logger.Error("saga: compensate rejected by from; STUCK",
					zap.String("transfer_id", entry.TransferID),
					zap.String("reason", reasonString(res)))
				return d.transition(ctx, entry, transferledger.StateCompensateStuck, "compensate_rejected:"+reasonString(res))
			}
			lastErr = fmt.Errorf("unexpected status %v", res.Status)
		} else {
			lastErr = callErr
		}
		d.logger.Warn("saga: compensate attempt failed",
			zap.String("transfer_id", entry.TransferID),
			zap.Int("attempt", attempt),
			zap.Duration("next_backoff", backoff),
			zap.Error(lastErr))
		if ctx.Err() != nil {
			return entry, ctx.Err()
		}
		if attempt == d.cfg.CompensateRetries {
			break
		}
		d.sleep(ctx, backoff)
		backoff *= 2
		if backoff > MaxCompensateBackoff {
			backoff = MaxCompensateBackoff
		}
	}
	d.logger.Error("saga: compensate exhausted retries; STUCK",
		zap.String("transfer_id", entry.TransferID),
		zap.Int("retries", d.cfg.CompensateRetries),
		zap.Error(lastErr))
	reason := "compensate_stuck"
	if lastErr != nil {
		reason = "compensate_stuck:" + lastErr.Error()
	}
	return d.transition(ctx, entry, transferledger.StateCompensateStuck, reason)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// transition persists the ledger state change and returns the updated entry
// for the caller. It is called at every saga transition; errors here are
// considered unrecoverable because they indicate ledger breakage.
func (d *Driver) transition(ctx context.Context, entry transferledger.Entry, to transferledger.State, reason string) (transferledger.Entry, error) {
	if err := d.ledger.UpdateState(ctx, entry.TransferID, entry.State, to, reason); err != nil {
		if errors.Is(err, transferledger.ErrStateMismatch) {
			// Another goroutine (e.g. Recover on a duplicate run)
			// advanced the saga. Read back the current row and keep
			// going from there.
			current, gErr := d.ledger.Get(ctx, entry.TransferID)
			if gErr != nil {
				return entry, fmt.Errorf("saga: state mismatch then read back failed: %w", gErr)
			}
			d.logger.Info("saga: state mismatch reconciled; continuing from observed state",
				zap.String("transfer_id", entry.TransferID),
				zap.String("observed", string(current.State)))
			return current, nil
		}
		return entry, fmt.Errorf("saga: update ledger: %w", err)
	}
	if d.metrics != nil {
		d.metrics.TransitionsTotal.WithLabelValues(string(entry.State), string(to)).Inc()
		if to == transferledger.StateCompensateStuck {
			d.metrics.StuckTotal.Inc()
		}
	}
	entry.State = to
	entry.RejectReason = reason
	entry.UpdatedAtMs = d.clock().UnixMilli()
	return entry, nil
}

// callWithRetry retries call up to `attempts` times with a linear
// backoff of `baseDelay`*attempt. Returns the last holder.Result on
// success (first successful call) or the last error on exhaustion.
// attempts <= 1 disables retry.
func (d *Driver) callWithRetry(ctx context.Context, attempts int, baseDelay time.Duration, label string, call func(context.Context) (holder.Result, error)) (holder.Result, error) {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 1; i <= attempts; i++ {
		cctx, cancel := context.WithTimeout(ctx, d.cfg.RPCTimeout)
		res, err := call(cctx)
		cancel()
		if err == nil {
			return res, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return holder.Result{}, ctx.Err()
		}
		if i == attempts {
			break
		}
		wait := baseDelay * time.Duration(i)
		d.logger.Warn("saga: holder RPC transport failed; retrying",
			zap.String("label", label),
			zap.Int("attempt", i),
			zap.Duration("wait", wait),
			zap.Error(err))
		d.sleep(ctx, wait)
	}
	return holder.Result{}, lastErr
}

func reasonString(res holder.Result) string {
	if res.ReasonDetail != "" {
		return res.ReasonDetail
	}
	return res.Reason.String()
}

// sleepWithCtx is the default sleep used by the driver; context-aware
// so shutdown can interrupt.
func sleepWithCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
