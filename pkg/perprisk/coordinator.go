// Package perprisk contains the pure state machine for ADR-0071's global risk
// coordinator. The package intentionally stops at deterministic accounting and
// task planning: Kafka consumption, HA election, and RPC/task transport can wrap
// this state without duplicating fund, quota, or ADL correctness logic.
package perprisk

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

var zero = dec.FromInt(0)

// InsuranceDelta is the coordinator's fold input. perp-counter shards emit the
// delta in perp-journal; the coordinator folds it by settlement coin instead of
// trusting any shard-local fund balance.
type InsuranceDelta struct {
	Coin             string
	Symbol           string
	UserID           string
	RefID            string
	Delta            dec.Decimal
	Price            dec.Decimal
	Backstop         bool
	TakeoverNotional dec.Decimal
}

// QuotaPolicy limits how much working capital one symbol can borrow from the
// global coin fund during a day. Fraction and AbsoluteCap are combined with min
// when both are set; zero means "not configured" for that dimension.
type QuotaPolicy struct {
	Fraction    dec.Decimal
	AbsoluteCap dec.Decimal
}

// BorrowRequest asks the global fund to lend working capital for takeover
// inventory unwinding. Day is an explicit key (for example UTC YYYY-MM-DD) so
// tests and callers do not smuggle wall-clock assumptions into the pure state.
type BorrowRequest struct {
	Coin   string
	Symbol string
	Day    string
	RefID  string
	Amount dec.Decimal
}

// BorrowResult explains the three-way min ADR-0071 requires: requested amount,
// remaining per-symbol daily quota, and global coin availability.
type BorrowResult struct {
	Requested       dec.Decimal
	Borrowed        dec.Decimal
	GlobalAvailable dec.Decimal
	SymbolLimit     dec.Decimal
	SymbolUsed      dec.Decimal
}

// ADLCandidate is a shard-reported or projection-derived profitable position.
// PosSeq and PositionVersion are the read-view stamps observed by the
// coordinator. Execution must reject the task if either stamp, or the observed
// side, differs by the time the command enters the owning user's sequencer.
type ADLCandidate struct {
	UserID          string
	Symbol          string
	Side            perpstate.Side
	Size            dec.Decimal
	Score           dec.Decimal
	SacrificePerQty dec.Decimal
	PosSeq          uint64
	PositionVersion uint64
}

// ADLTask is the version-stamped command a coordinator may dispatch to a
// perp-counter shard. The coordinator only decides; the owning shard performs
// all position mutation after checking the observed position stamps and
// AdlRound idempotency.
type ADLTask struct {
	UserID          string
	Symbol          string
	Side            perpstate.Side
	Qty             dec.Decimal
	Price           dec.Decimal
	PosSeq          uint64
	PositionVersion uint64
	AdlRound        uint64
}

// Snapshot is the coordinator recovery image. Offsets are next-to-consume
// offsets for perp-journal partitions, matching ADR-0048's snapshot+offset
// binding shape.
type Snapshot struct {
	Funds        []FundSnap   `json:"funds"`
	Quotas       []QuotaSnap  `json:"quotas"`
	Loans        []LoanSnap   `json:"loans"`
	Offsets      []OffsetSnap `json:"offsets"`
	NextAdlRound uint64       `json:"next_adl_round"`
}

type FundSnap struct {
	Coin   string `json:"coin"`
	Amount string `json:"amount"`
}

type QuotaSnap struct {
	Symbol string `json:"symbol"`
	Day    string `json:"day"`
	Used   string `json:"used"`
}

type LoanSnap struct {
	RefID     string `json:"ref_id"`
	Coin      string `json:"coin"`
	Symbol    string `json:"symbol"`
	Day       string `json:"day"`
	Principal string `json:"principal"`
	Repaid    string `json:"repaid"`
}

type OffsetSnap struct {
	Partition int32 `json:"partition"`
	Next      int64 `json:"next"`
}

type quotaState struct {
	day  string
	used dec.Decimal
}

type loanState struct {
	coin      string
	symbol    string
	day       string
	principal dec.Decimal
	repaid    dec.Decimal
}

// Coordinator owns the authoritative global fund state for ADR-0071. It is
// safe for concurrent callers; transport layers can process multiple Kafka
// partitions while keeping the fold and quota updates serialized here.
type Coordinator struct {
	mu sync.Mutex

	funds        map[string]dec.Decimal
	policies     map[string]QuotaPolicy
	quotas       map[string]quotaState
	loans        map[string]loanState
	offsets      map[int32]int64
	nextAdlRound uint64
}

func New() *Coordinator {
	return &Coordinator{
		funds:    map[string]dec.Decimal{},
		policies: map[string]QuotaPolicy{},
		quotas:   map[string]quotaState{},
		loans:    map[string]loanState{},
		offsets:  map[int32]int64{},
	}
}

// SetQuotaPolicy installs the daily borrow rule for one symbol. Existing daily
// usage is kept because config changes should not erase already-consumed risk
// capacity during the same business day.
func (c *Coordinator) SetQuotaPolicy(symbol string, p QuotaPolicy) error {
	if symbol == "" {
		return errors.New("perprisk: quota symbol required")
	}
	if p.Fraction.Sign() < 0 || p.AbsoluteCap.Sign() < 0 {
		return errors.New("perprisk: quota policy must be non-negative")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.policies[symbol] = p
	return nil
}

// ApplyDelta folds one InsuranceDelta into the global coin fund. Positive
// values are credits, negative values are deficits paid by the fund.
func (c *Coordinator) ApplyDelta(d InsuranceDelta) error {
	if d.Coin == "" {
		return errors.New("perprisk: insurance delta coin required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.funds[d.Coin] = c.funds[d.Coin].Add(d.Delta)
	return nil
}

// ApplyJournalEvent folds the InsuranceDelta carried by a perp-journal event.
// Events without an insurance effect are ignored; malformed decimal payloads are
// errors so a coordinator does not silently skip money movement.
func (c *Coordinator) ApplyJournalEvent(evt *eventpb.PerpJournalEvent) (bool, error) {
	d, ok, err := DeltaFromJournalEvent(evt)
	if err != nil || !ok {
		return ok, err
	}
	return true, c.ApplyDelta(d)
}

// ApplyJournalEventAt folds an event and advances the partition's next offset
// only after the fold succeeds. This is the unit a Kafka adapter should place
// under its snapshot barrier.
func (c *Coordinator) ApplyJournalEventAt(evt *eventpb.PerpJournalEvent, partition int32, offset int64) (bool, error) {
	_, applied, err := c.ApplyJournalEventAtResult(evt, partition, offset)
	return applied, err
}

// ApplyJournalEventAtResult is the richer form used by perp-risk's service
// loop: it atomically folds the event and offset, then returns the delta so the
// coordinator can decide whether this record should trigger ADL planning.
func (c *Coordinator) ApplyJournalEventAtResult(evt *eventpb.PerpJournalEvent, partition int32, offset int64) (InsuranceDelta, bool, error) {
	d, applied, err := DeltaFromJournalEvent(evt)
	if err != nil {
		return InsuranceDelta{}, false, err
	}
	c.mu.Lock()
	if applied {
		c.funds[d.Coin] = c.funds[d.Coin].Add(d.Delta)
	}
	if next := offset + 1; next > c.offsets[partition] {
		c.offsets[partition] = next
	}
	c.mu.Unlock()
	return d, applied, nil
}

// DeltaFromJournalEvent extracts the insurance movement from liquidation and
// ADL journal records. The settlement coin is derived from the linear perp
// symbol (BTC-USDT-PERP -> USDT) until a future proto carries coin explicitly.
func DeltaFromJournalEvent(evt *eventpb.PerpJournalEvent) (InsuranceDelta, bool, error) {
	if evt == nil {
		return InsuranceDelta{}, false, nil
	}
	switch p := evt.GetPayload().(type) {
	case *eventpb.PerpJournalEvent_Liquidation:
		l := p.Liquidation
		if l == nil {
			return InsuranceDelta{}, false, nil
		}
		delta, err := dec.Parse(l.GetInsuranceDelta())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: liquidation insurance_delta: %w", err)
		}
		price, err := dec.Parse(l.GetBankruptcyPrice())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: liquidation bankruptcy_price: %w", err)
		}
		closedQty, err := dec.Parse(l.GetClosedQty())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: liquidation closed_qty: %w", err)
		}
		return InsuranceDelta{
			Coin: coinFromLinearPerp(l.GetSymbol()), Symbol: l.GetSymbol(),
			UserID: l.GetUserId(), RefID: strconv.FormatUint(l.GetLiqOrderId(), 10),
			Delta: delta, Price: price, Backstop: l.GetBackstop(),
			TakeoverNotional: closedQty.Mul(price),
		}, true, nil
	case *eventpb.PerpJournalEvent_Takeover:
		t := p.Takeover
		if t == nil {
			return InsuranceDelta{}, false, nil
		}
		delta, err := dec.Parse(t.GetInsuranceDelta())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: takeover insurance_delta: %w", err)
		}
		price, err := dec.Parse(t.GetBankruptcyPrice())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: takeover bankruptcy_price: %w", err)
		}
		closedQty, err := dec.Parse(t.GetClosedQty())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: takeover closed_qty: %w", err)
		}
		takeoverNotional, err := dec.Parse(t.GetTakeoverNotional())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: takeover takeover_notional: %w", err)
		}
		if takeoverNotional.Sign() <= 0 {
			// Older fixtures may not carry takeover_notional. Recomputing it from
			// the deterministic position transfer keeps replay behavior stable
			// while the coordinator remains the sole authority for actual loans.
			takeoverNotional = closedQty.Mul(price)
		}
		return InsuranceDelta{
			Coin: coinFromLinearPerp(t.GetSymbol()), Symbol: t.GetSymbol(),
			UserID: t.GetUserId(), RefID: strconv.FormatUint(t.GetLiqOrderId(), 10),
			Delta: delta, Price: price, Backstop: true,
			TakeoverNotional: takeoverNotional,
		}, true, nil
	case *eventpb.PerpJournalEvent_Adl:
		a := p.Adl
		if a == nil {
			return InsuranceDelta{}, false, nil
		}
		delta, err := dec.Parse(a.GetInsuranceDelta())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: adl insurance_delta: %w", err)
		}
		price, err := dec.Parse(a.GetPrice())
		if err != nil {
			return InsuranceDelta{}, true, fmt.Errorf("perprisk: adl price: %w", err)
		}
		return InsuranceDelta{
			Coin: coinFromLinearPerp(a.GetSymbol()), Symbol: a.GetSymbol(),
			UserID: a.GetUserId(), RefID: strconv.FormatUint(a.GetAdlRound(), 10),
			Delta: delta, Price: price,
		}, true, nil
	default:
		return InsuranceDelta{}, false, nil
	}
}

// BorrowWorkingCapital debits the global fund by the allowed amount and records
// daily symbol usage. Repayments do not lower usage: the quota caps gross daily
// risk taken by a symbol, which is the failure-containment property ADR-0071
// needs during a liquidation storm.
func (c *Coordinator) BorrowWorkingCapital(req BorrowRequest) (BorrowResult, error) {
	if req.Coin == "" || req.Symbol == "" || req.Day == "" {
		return BorrowResult{}, errors.New("perprisk: borrow coin, symbol, and day are required")
	}
	if req.Amount.Sign() <= 0 {
		return BorrowResult{}, errors.New("perprisk: borrow amount must be positive")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if req.RefID != "" {
		if existing, ok := c.loans[req.RefID]; ok {
			// Borrow refs are idempotency keys from the takeover event. Returning
			// zero borrowed on replay avoids double-debiting the global fund while
			// still exposing the loan that was already consuming symbol quota.
			return BorrowResult{
				Requested: req.Amount, Borrowed: zero, GlobalAvailable: dec.Max(c.funds[req.Coin], zero),
				SymbolLimit: c.symbolLimitLocked(req.Symbol, dec.Max(c.funds[req.Coin], zero)),
				SymbolUsed:  existing.principal,
			}, nil
		}
	}

	available := dec.Max(c.funds[req.Coin], zero)
	st := c.quotas[req.Symbol]
	if st.day != req.Day {
		// Quotas reset on the caller-supplied business day, not on wall clock in
		// this pure package. That keeps replay deterministic and lets deployment
		// policy decide whether the day is UTC or exchange-local.
		st = quotaState{day: req.Day, used: zero}
	}
	limit := c.symbolLimitLocked(req.Symbol, available)
	remainingQuota := dec.Max(limit.Sub(st.used), zero)
	borrowed := dec.Min(req.Amount, dec.Min(available, remainingQuota))
	if borrowed.Sign() > 0 {
		c.funds[req.Coin] = c.funds[req.Coin].Sub(borrowed)
		st.used = st.used.Add(borrowed)
		c.quotas[req.Symbol] = st
		if req.RefID != "" {
			c.loans[req.RefID] = loanState{
				coin: req.Coin, symbol: req.Symbol, day: req.Day,
				principal: borrowed, repaid: zero,
			}
		}
	}
	return BorrowResult{
		Requested: req.Amount, Borrowed: borrowed, GlobalAvailable: available,
		SymbolLimit: limit, SymbolUsed: st.used,
	}, nil
}

// RepayWorkingCapital credits the global fund when takeover inventory unwinds.
func (c *Coordinator) RepayWorkingCapital(coin string, amount dec.Decimal) error {
	if coin == "" {
		return errors.New("perprisk: repay coin required")
	}
	if amount.Sign() <= 0 {
		return errors.New("perprisk: repay amount must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.funds[coin] = c.funds[coin].Add(amount)
	return nil
}

func (c *Coordinator) RepayWorkingCapitalRef(refID string, amount dec.Decimal) error {
	if refID == "" {
		return errors.New("perprisk: repay ref_id required")
	}
	if amount.Sign() <= 0 {
		return errors.New("perprisk: repay amount must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	loan, ok := c.loans[refID]
	if !ok {
		return errors.New("perprisk: working-capital loan not found")
	}
	remaining := loan.principal.Sub(loan.repaid)
	// Over-repayment is clamped instead of rejected so external inventory
	// unwinders can be at-least-once without accidentally minting fund balance.
	move := dec.Min(amount, remaining)
	if move.Sign() <= 0 {
		return nil
	}
	loan.repaid = loan.repaid.Add(move)
	c.loans[refID] = loan
	c.funds[loan.coin] = c.funds[loan.coin].Add(move)
	return nil
}

func (c *Coordinator) symbolLimitLocked(symbol string, available dec.Decimal) dec.Decimal {
	p := c.policies[symbol]
	limit := available
	if p.Fraction.Sign() > 0 {
		limit = available.Mul(p.Fraction)
	}
	if p.AbsoluteCap.Sign() > 0 {
		limit = dec.Min(limit, p.AbsoluteCap)
	}
	return dec.Max(limit, zero)
}

func (c *Coordinator) Fund(coin string) dec.Decimal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.funds[coin]
}

func (c *Coordinator) Offset(partition int32) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsets[partition]
}

func (c *Coordinator) Offsets() map[int32]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[int32]int64, len(c.offsets))
	for part, next := range c.offsets {
		out[part] = next
	}
	return out
}

// ReserveAdlRound allocates a coordinator-scoped ADL round. Shards use it as
// the replay guard in addition to the position sequence stamp.
func (c *Coordinator) ReserveAdlRound() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextAdlRound++
	return c.nextAdlRound
}

// PlanADL converts a deficit and ranked candidates into version-stamped tasks.
// Candidate data may be stale; correctness relies on the owning shard checking
// task's observed stamps before mutation, so this planner is free to use
// eventually consistent shard reports or MySQL projections.
func PlanADL(deficit, price dec.Decimal, adlRound uint64, candidates []ADLCandidate) []ADLTask {
	if deficit.Sign() <= 0 {
		return nil
	}
	ordered := append([]ADLCandidate(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if c := ordered[i].Score.Cmp(ordered[j].Score); c != 0 {
			return c > 0
		}
		return ordered[i].UserID < ordered[j].UserID
	})
	remaining := deficit
	tasks := make([]ADLTask, 0, len(ordered))
	for _, cand := range ordered {
		if remaining.Sign() <= 0 {
			break
		}
		if cand.Size.Sign() <= 0 || cand.SacrificePerQty.Sign() <= 0 {
			continue
		}
		// SacrificePerQty is the insurance improvement from closing one base
		// unit at price. Dividing remaining deficit by that value minimizes user
		// impact while preserving deterministic task sizing.
		qty := dec.Min(cand.Size, remaining.Div(cand.SacrificePerQty))
		if qty.Sign() <= 0 {
			continue
		}
		tasks = append(tasks, ADLTask{
			UserID: cand.UserID, Symbol: cand.Symbol, Side: cand.Side,
			Qty: qty, Price: price, PosSeq: cand.PosSeq,
			PositionVersion: cand.PositionVersion, AdlRound: adlRound,
		})
		remaining = remaining.Sub(qty.Mul(cand.SacrificePerQty))
	}
	return tasks
}

func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Snapshot{NextAdlRound: c.nextAdlRound}
	for coin, amount := range c.funds {
		s.Funds = append(s.Funds, FundSnap{Coin: coin, Amount: amount.String()})
	}
	sort.Slice(s.Funds, func(i, j int) bool { return s.Funds[i].Coin < s.Funds[j].Coin })
	for symbol, q := range c.quotas {
		s.Quotas = append(s.Quotas, QuotaSnap{Symbol: symbol, Day: q.day, Used: q.used.String()})
	}
	sort.Slice(s.Quotas, func(i, j int) bool { return s.Quotas[i].Symbol < s.Quotas[j].Symbol })
	for ref, loan := range c.loans {
		s.Loans = append(s.Loans, LoanSnap{
			RefID: ref, Coin: loan.coin, Symbol: loan.symbol, Day: loan.day,
			Principal: loan.principal.String(), Repaid: loan.repaid.String(),
		})
	}
	sort.Slice(s.Loans, func(i, j int) bool { return s.Loans[i].RefID < s.Loans[j].RefID })
	for part, next := range c.offsets {
		s.Offsets = append(s.Offsets, OffsetSnap{Partition: part, Next: next})
	}
	sort.Slice(s.Offsets, func(i, j int) bool { return s.Offsets[i].Partition < s.Offsets[j].Partition })
	return s
}

func (c *Coordinator) Restore(s Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.funds = map[string]dec.Decimal{}
	c.policies = map[string]QuotaPolicy{}
	c.quotas = map[string]quotaState{}
	c.loans = map[string]loanState{}
	c.offsets = map[int32]int64{}
	c.nextAdlRound = s.NextAdlRound
	for _, f := range s.Funds {
		c.funds[f.Coin] = dec.New(f.Amount)
	}
	for _, q := range s.Quotas {
		c.quotas[q.Symbol] = quotaState{day: q.Day, used: dec.New(q.Used)}
	}
	for _, l := range s.Loans {
		c.loans[l.RefID] = loanState{
			coin: l.Coin, symbol: l.Symbol, day: l.Day,
			principal: dec.New(l.Principal), repaid: dec.New(l.Repaid),
		}
	}
	for _, o := range s.Offsets {
		c.offsets[o.Partition] = o.Next
	}
}

func coinFromLinearPerp(symbol string) string {
	parts := strings.Split(symbol, "-")
	if len(parts) >= 2 && parts[1] != "" {
		return parts[1]
	}
	return "USDT"
}
