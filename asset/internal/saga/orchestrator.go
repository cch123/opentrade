package saga

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/asset/internal/metrics"
	"github.com/xargin/opentrade/pkg/transferledger"
)

// Orchestrator is the user-facing wrapper around Driver. It owns a
// ledger, a biz_line registry, and a Driver, and exposes three
// higher-level operations:
//
//   - Transfer:       called by AssetService.Transfer (BFF entry).
//   - Query:          called by AssetService.QueryTransfer.
//   - Recover:        called once at startup to resume pending sagas.
//
// Transfer is synchronous: it drives the saga all the way to a terminal
// state before returning, so the BFF's gRPC caller can observe the
// outcome on the same request. Recover runs asynchronously in its own
// goroutine pool.
type Orchestrator struct {
	ledger  *transferledger.Ledger
	driver  *Driver
	logger  *zap.Logger
	metrics *metrics.Saga

	// inflight tracks transfer_ids currently being driven so Recover
	// doesn't race the primary Transfer path on the same row.
	inflight   sync.Map
	recoverSem chan struct{} // bounds Recover parallelism
}

// OrchestratorConfig tunes Recover parallelism. A sane default (16) is
// applied when RecoverParallelism <= 0.
type OrchestratorConfig struct {
	RecoverParallelism int
}

// NewOrchestrator wires an Orchestrator. m may be nil — tests or
// deployments without a metrics endpoint pass nil and the orchestrator
// silently skips observations.
func NewOrchestrator(cfg OrchestratorConfig, ledger *transferledger.Ledger, driver *Driver, logger *zap.Logger, m *metrics.Saga) *Orchestrator {
	if logger == nil {
		logger = zap.NewNop()
	}
	par := cfg.RecoverParallelism
	if par <= 0 {
		par = 16
	}
	return &Orchestrator{
		ledger:     ledger,
		driver:     driver,
		logger:     logger,
		metrics:    m,
		recoverSem: make(chan struct{}, par),
	}
}

// TransferInput is the Transfer RPC shape in internal types.
type TransferInput struct {
	UserID     string
	TransferID string
	FromBiz    string
	ToBiz      string
	Asset      string
	Amount     string // decimal string; validated upstream (server layer)
	Memo       string
}

// TransferOutput is what AssetService.Transfer returns.
type TransferOutput struct {
	TransferID string
	State      transferledger.State
	Reason     string
	Terminal   bool
}

// Errors surfaced directly (non-saga errors).
var (
	ErrInvalidRequest = errors.New("saga: invalid request")
	ErrSameBiz        = errors.New("saga: from_biz and to_biz must differ")
)

// Transfer creates the ledger row (idempotently; ErrAlreadyExists falls
// through to "return the current row") and drives it to a terminal
// state.
func (o *Orchestrator) Transfer(ctx context.Context, in TransferInput) (TransferOutput, error) {
	if err := validateTransfer(in); err != nil {
		return TransferOutput{}, err
	}
	entry := transferledger.Entry{
		TransferID: in.TransferID,
		UserID:     in.UserID,
		FromBiz:    in.FromBiz,
		ToBiz:      in.ToBiz,
		Asset:      in.Asset,
		Amount:     in.Amount,
	}
	created, err := o.ledger.Create(ctx, entry)
	if err != nil && !errors.Is(err, transferledger.ErrAlreadyExists) {
		return TransferOutput{}, fmt.Errorf("saga: create ledger: %w", err)
	}
	if errors.Is(err, transferledger.ErrAlreadyExists) {
		// Idempotent replay. If the existing saga conflicts on shape
		// (different from/to/amount) we surface a clear error rather
		// than silently running a misaligned saga.
		if created.UserID != in.UserID || created.FromBiz != in.FromBiz ||
			created.ToBiz != in.ToBiz || created.Asset != in.Asset || created.Amount != in.Amount {
			return TransferOutput{}, fmt.Errorf("%w: transfer_id %q reused with different shape", ErrInvalidRequest, in.TransferID)
		}
		// If the existing saga is already terminal, return it.
		if created.State.IsTerminal() {
			return toOutput(created), nil
		}
		// Otherwise continue driving it (or wait for an in-flight
		// run to finish). The inflight map prevents double drivers.
	}

	return o.driveOwned(ctx, created)
}

// Query returns the current ledger row for transfer_id as a trimmed
// TransferOutput. Callers that need the full ledger shape (user_id,
// asset, amount, timestamps, ...) should use QueryEntry instead.
func (o *Orchestrator) Query(ctx context.Context, transferID string) (TransferOutput, error) {
	e, err := o.ledger.Get(ctx, transferID)
	if err != nil {
		return TransferOutput{}, err
	}
	return toOutput(e), nil
}

// QueryEntry returns the full ledger row.
func (o *Orchestrator) QueryEntry(ctx context.Context, transferID string) (transferledger.Entry, error) {
	return o.ledger.Get(ctx, transferID)
}

// Recover loads all non-terminal ledger rows and drives each to
// completion using a bounded goroutine pool. It returns after all
// recovered sagas finish (or ctx is done). Intended to be called once
// from main at startup, before the gRPC server starts accepting
// traffic, so no concurrent Transfer can race.
func (o *Orchestrator) Recover(ctx context.Context) error {
	const batch = 500
	pending, err := o.ledger.ListPending(ctx, batch)
	if err != nil {
		return fmt.Errorf("saga: list pending: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}
	o.logger.Info("saga: recovering pending transfers", zap.Int("count", len(pending)))

	var wg sync.WaitGroup
	for _, p := range pending {
		p := p
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case o.recoverSem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-o.recoverSem }()
			if _, err := o.driveOwned(ctx, p); err != nil {
				o.logger.Warn("saga: recover drive failed",
					zap.String("transfer_id", p.TransferID),
					zap.Error(err))
			}
		}()
	}
	wg.Wait()
	return nil
}

// driveOwned takes a ledger row, claims exclusive ownership via the
// inflight map, and drives it to terminal. Competing callers (e.g. a
// Recover pass racing a fresh Transfer with the same id) see a
// fast-path "already in flight" path that waits for the current driver
// to finish and then reads the ledger.
func (o *Orchestrator) driveOwned(ctx context.Context, entry transferledger.Entry) (TransferOutput, error) {
	waiter := make(chan struct{})
	existingWaiter, loaded := o.inflight.LoadOrStore(entry.TransferID, waiter)
	if loaded {
		// Someone else is driving this saga. Wait for them to
		// finish, then return the latest ledger state.
		select {
		case <-existingWaiter.(chan struct{}):
		case <-ctx.Done():
			return TransferOutput{}, ctx.Err()
		}
		fresh, err := o.ledger.Get(ctx, entry.TransferID)
		if err != nil {
			return TransferOutput{}, err
		}
		return toOutput(fresh), nil
	}
	defer func() {
		o.inflight.Delete(entry.TransferID)
		close(waiter)
	}()

	final, err := o.driver.Run(ctx, entry)
	if err != nil {
		return TransferOutput{}, err
	}
	o.observeTerminalDuration(entry, final)
	return toOutput(final), nil
}

// observeTerminalDuration records how long the saga took from creation
// to terminal, labelled by final state. Non-terminal runs and missing
// metrics handle are skipped. CreatedAtMs is 0 for ledger rows we never
// re-read after transition() bumped them, but the Run contract
// guarantees `final` came through the ledger so the timestamp is set.
func (o *Orchestrator) observeTerminalDuration(initial, final transferledger.Entry) {
	if o.metrics == nil {
		return
	}
	if !final.State.IsTerminal() {
		return
	}
	if initial.CreatedAtMs <= 0 || final.UpdatedAtMs <= 0 {
		return
	}
	elapsed := time.Duration(final.UpdatedAtMs-initial.CreatedAtMs) * time.Millisecond
	if elapsed < 0 {
		return
	}
	o.metrics.TerminalDurationSec.WithLabelValues(string(final.State)).Observe(elapsed.Seconds())
}

func validateTransfer(in TransferInput) error {
	if in.UserID == "" {
		return fmt.Errorf("%w: user_id required", ErrInvalidRequest)
	}
	if in.TransferID == "" {
		return fmt.Errorf("%w: transfer_id required", ErrInvalidRequest)
	}
	if in.FromBiz == "" || in.ToBiz == "" {
		return fmt.Errorf("%w: from_biz and to_biz required", ErrInvalidRequest)
	}
	if in.FromBiz == in.ToBiz {
		return ErrSameBiz
	}
	if in.Asset == "" {
		return fmt.Errorf("%w: asset required", ErrInvalidRequest)
	}
	if in.Amount == "" {
		return fmt.Errorf("%w: amount required", ErrInvalidRequest)
	}
	return nil
}

func toOutput(e transferledger.Entry) TransferOutput {
	return TransferOutput{
		TransferID: e.TransferID,
		State:      e.State,
		Reason:     e.RejectReason,
		Terminal:   e.State.IsTerminal(),
	}
}
