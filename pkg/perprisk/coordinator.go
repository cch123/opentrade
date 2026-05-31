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

// InsuranceDelta is kept for explicit seed/legacy market-liquidation folds.
// ADR-0073 takeover inventory and ADL no longer use it as authoritative fund
// input; completed lots must settle through RiskPoolSettlement.
type InsuranceDelta struct {
	Coin             string
	Symbol           string
	UserID           uint64
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
	UserID          uint64
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
	LotID           string
	UserID          uint64
	Symbol          string
	Side            perpstate.Side
	Qty             dec.Decimal
	Price           dec.Decimal
	PosSeq          uint64
	PositionVersion uint64
	AdlRound        uint64
}

type JournalKind string

const (
	JournalKindNone               JournalKind = ""
	JournalKindLegacyDelta        JournalKind = "legacy_delta"
	JournalKindTakeoverLot        JournalKind = "takeover_lot"
	JournalKindLotADL             JournalKind = "lot_adl"
	JournalKindRiskPoolSettlement JournalKind = "risk_pool_settlement"
)

// JournalResult is the coordinator-facing effect of one perp-journal record.
// User-position events and fund movements are intentionally separated: ADR-0073
// requires ADL to consume lot inventory, while only settlement changes the fund.
type JournalResult struct {
	Kind         JournalKind
	Applied      bool
	Coin         string
	Symbol       string
	UserID       uint64
	Price        dec.Decimal
	LotID        string
	Lot          TakenOverLot
	AdlRound     uint64
	BorrowRef    string
	BorrowAmount dec.Decimal
	Settlement   RiskPoolSettlement
}

// Snapshot is the coordinator recovery image. Offsets are next-to-consume
// offsets for perp-journal partitions, matching ADR-0048's snapshot+offset
// binding shape.
type Snapshot struct {
	Funds        []FundSnap               `json:"funds"`
	Quotas       []QuotaSnap              `json:"quotas"`
	Loans        []LoanSnap               `json:"loans"`
	Lots         []LotSnap                `json:"lots"`
	Settlements  []RiskPoolSettlementSnap `json:"settlements"`
	InFlightADL  []InFlightADLSnap        `json:"in_flight_adl"`
	Offsets      []OffsetSnap             `json:"offsets"`
	NextAdlRound uint64                   `json:"next_adl_round"`
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

type LotSnap struct {
	LotID             string    `json:"lot_id"`
	UserID            uint64    `json:"user_id"`
	Symbol            string    `json:"symbol"`
	Side              uint8     `json:"side"`
	TotalQty          string    `json:"total_qty"`
	LeavesQty         string    `json:"leaves_qty"`
	TakeoverPrice     string    `json:"takeover_price"`
	TriggerMarkPrice  string    `json:"trigger_mark_price"`
	TakenOverBalance  string    `json:"taken_over_balance"`
	PositionVersion   uint64    `json:"position_version"`
	Status            LotStatus `json:"status"`
	RealizedPnL       string    `json:"realized_pnl"`
	CumFee            string    `json:"cum_fee"`
	WorkingCapitalRef string    `json:"working_capital_ref"`
	WorkingCapital    string    `json:"working_capital"`
	CreatedUnixMs     int64     `json:"created_unix_ms"`
	UpdatedUnixMs     int64     `json:"updated_unix_ms"`
}

type RiskPoolSettlementSnap struct {
	LotID               string `json:"lot_id"`
	Symbol              string `json:"symbol"`
	Coin                string `json:"coin"`
	WorkingCapitalRef   string `json:"working_capital_ref"`
	TakenOverBalance    string `json:"taken_over_balance"`
	LiqAdlRealizedPnL   string `json:"liq_adl_realized_pnl"`
	CumFee              string `json:"cum_fee"`
	WorkingCapitalDrawn string `json:"working_capital_drawn"`
	BorrowedBalance     string `json:"borrowed_balance"`
	FinalPoolDelta      string `json:"final_pool_delta"`
}

type InFlightADLSnap struct {
	LotID           string `json:"lot_id"`
	UserID          uint64 `json:"user_id"`
	Symbol          string `json:"symbol"`
	Side            uint8  `json:"side"`
	Qty             string `json:"qty"`
	Price           string `json:"price"`
	PosSeq          uint64 `json:"pos_seq"`
	PositionVersion uint64 `json:"position_version"`
	AdlRound        uint64 `json:"adl_round"`
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
	lots         map[string]TakenOverLot
	settlements  map[string]RiskPoolSettlement
	inFlightADL  map[string]ADLTask
	offsets      map[int32]int64
	nextAdlRound uint64
}

func New() *Coordinator {
	return &Coordinator{
		funds:       map[string]dec.Decimal{},
		policies:    map[string]QuotaPolicy{},
		quotas:      map[string]quotaState{},
		loans:       map[string]loanState{},
		lots:        map[string]TakenOverLot{},
		settlements: map[string]RiskPoolSettlement{},
		inFlightADL: map[string]ADLTask{},
		offsets:     map[int32]int64{},
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

// ApplyDelta folds an explicit legacy/seed delta into the global coin fund.
// New takeover inventory code should prefer SettleLot/RiskPoolSettlement.
func (c *Coordinator) ApplyDelta(d InsuranceDelta) error {
	if d.Coin == "" {
		return errors.New("perprisk: insurance delta coin required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.funds[d.Coin] = c.funds[d.Coin].Add(d.Delta)
	return nil
}

// ApplyJournalEvent folds the risk effect carried by a perp-journal event.
// Events without a RiskPool effect are ignored; malformed decimal payloads are
// errors so a coordinator does not silently skip inventory or money movement.
func (c *Coordinator) ApplyJournalEvent(evt *eventpb.PerpJournalEvent) (bool, error) {
	result, err := c.applyJournalEventAtResult(evt, nil, 0)
	return result.Applied, err
}

// ApplyJournalEventAt folds an event and advances the partition's next offset
// only after the fold succeeds. This is the unit a Kafka adapter should place
// under its snapshot barrier.
func (c *Coordinator) ApplyJournalEventAt(evt *eventpb.PerpJournalEvent, partition int32, offset int64) (bool, error) {
	result, err := c.ApplyJournalEventAtResult(evt, partition, offset)
	return result.Applied, err
}

// ApplyJournalEventAtResult is the richer form used by perp-risk's service
// loop: it atomically folds the event and offset, then returns the lot/fund
// effect so the coordinator service can dispatch inventory-consuming work.
func (c *Coordinator) ApplyJournalEventAtResult(evt *eventpb.PerpJournalEvent, partition int32, offset int64) (JournalResult, error) {
	return c.applyJournalEventAtResult(evt, &partition, offset)
}

func (c *Coordinator) applyJournalEventAtResult(evt *eventpb.PerpJournalEvent, partition *int32, offset int64) (JournalResult, error) {
	result, err := JournalResultFromEvent(evt)
	if err != nil {
		return JournalResult{}, err
	}
	c.mu.Lock()
	if partition != nil && offset < c.offsets[*partition] {
		c.mu.Unlock()
		return JournalResult{}, nil
	}
	if result.Applied {
		if err := c.applyJournalResultLocked(&result); err != nil {
			c.mu.Unlock()
			return JournalResult{}, err
		}
	}
	if partition != nil {
		if next := offset + 1; next > c.offsets[*partition] {
			c.offsets[*partition] = next
		}
	}
	if result.LotID != "" {
		result.Lot = c.lots[result.LotID]
	}
	c.mu.Unlock()
	return result, nil
}

func (c *Coordinator) applyJournalResultLocked(result *JournalResult) error {
	switch result.Kind {
	case JournalKindLegacyDelta:
		c.funds[result.Coin] = c.funds[result.Coin].Add(result.Settlement.FinalPoolDelta)
	case JournalKindTakeoverLot:
		if _, ok := c.lots[result.LotID]; ok {
			return nil
		}
		lot, err := NewTakenOverLot(result.Lot)
		if err != nil {
			return err
		}
		c.lots[lot.LotID] = lot
	case JournalKindLotADL:
		lot, ok := c.lots[result.LotID]
		if !ok {
			return errors.New("perprisk: ADL result for unknown lot")
		}
		next, err := ApplyLotADL(lot, result.BorrowAmount, result.Settlement.LiqAdlRealizedPnL)
		if err != nil {
			return err
		}
		c.lots[result.LotID] = next
		delete(c.inFlightADL, adlTaskKey(result.LotID, result.UserID, result.AdlRound))
	case JournalKindRiskPoolSettlement:
		return c.applyRiskPoolSettlementLocked(result.Settlement)
	}
	return nil
}

// JournalResultFromEvent extracts the risk-coordinator effect from a
// perp-journal event. ADR-0073 deliberately ignores ADL insurance_delta: ADL is
// inventory consumption, not a fund credit. Legacy liquidation deltas are kept
// only for non-takeover market fills until a dedicated settlement producer
// exists for that path.
func JournalResultFromEvent(evt *eventpb.PerpJournalEvent) (JournalResult, error) {
	if evt == nil {
		return JournalResult{}, nil
	}
	switch p := evt.GetPayload().(type) {
	case *eventpb.PerpJournalEvent_Liquidation:
		l := p.Liquidation
		if l == nil {
			return JournalResult{}, nil
		}
		delta, err := dec.Parse(l.GetInsuranceDelta())
		if err != nil {
			return JournalResult{}, fmt.Errorf("perprisk: liquidation insurance_delta: %w", err)
		}
		price, err := dec.Parse(l.GetBankruptcyPrice())
		if err != nil {
			return JournalResult{}, fmt.Errorf("perprisk: liquidation bankruptcy_price: %w", err)
		}
		coin := coinFromLinearPerp(l.GetSymbol())
		return JournalResult{
			Kind: JournalKindLegacyDelta, Applied: true, Coin: coin,
			Symbol: l.GetSymbol(), UserID: l.GetUserId(), Price: price,
			Settlement: RiskPoolSettlement{Symbol: l.GetSymbol(), Coin: coin, FinalPoolDelta: delta},
		}, nil
	case *eventpb.PerpJournalEvent_Takeover:
		t := p.Takeover
		if t == nil {
			return JournalResult{}, nil
		}
		lot, borrowAmount, borrowRef, err := lotFromTakeoverEvent(t, tsFromEventMeta(evt.GetMeta()))
		if err != nil {
			return JournalResult{}, err
		}
		return JournalResult{
			Kind: JournalKindTakeoverLot, Applied: true, Coin: coinFromLinearPerp(lot.Symbol),
			Symbol: lot.Symbol, UserID: lot.UserID, Price: lot.TakeoverPrice,
			LotID: lot.LotID, Lot: lot, BorrowRef: borrowRef, BorrowAmount: borrowAmount,
		}, nil
	case *eventpb.PerpJournalEvent_Adl:
		a := p.Adl
		if a == nil {
			return JournalResult{}, nil
		}
		if a.GetLotId() == "" {
			return JournalResult{}, nil
		}
		factQty, err := dec.Parse(a.GetFactQty())
		if err != nil {
			return JournalResult{}, fmt.Errorf("perprisk: adl fact_qty: %w", err)
		}
		if factQty.Sign() == 0 {
			factQty, err = dec.Parse(a.GetClosedQty())
			if err != nil {
				return JournalResult{}, fmt.Errorf("perprisk: adl closed_qty: %w", err)
			}
		}
		realized, err := dec.Parse(a.GetRealizedPnl())
		if err != nil {
			return JournalResult{}, fmt.Errorf("perprisk: adl realized_pnl: %w", err)
		}
		price, err := dec.Parse(a.GetPrice())
		if err != nil {
			return JournalResult{}, fmt.Errorf("perprisk: adl price: %w", err)
		}
		return JournalResult{
			Kind: JournalKindLotADL, Applied: true, Coin: coinFromLinearPerp(a.GetSymbol()),
			Symbol: a.GetSymbol(), UserID: a.GetUserId(), Price: price, LotID: a.GetLotId(),
			AdlRound: a.GetAdlRound(), BorrowAmount: factQty,
			Settlement: RiskPoolSettlement{LiqAdlRealizedPnL: realized},
		}, nil
	case *eventpb.PerpJournalEvent_RiskPoolSettlement:
		s := p.RiskPoolSettlement
		if s == nil {
			return JournalResult{}, nil
		}
		settlement, err := settlementFromEvent(s)
		if err != nil {
			return JournalResult{}, err
		}
		return JournalResult{
			Kind: JournalKindRiskPoolSettlement, Applied: true, Coin: settlement.Coin,
			Symbol: settlement.Symbol, LotID: settlement.LotID, Settlement: settlement,
		}, nil
	default:
		return JournalResult{}, nil
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

func (c *Coordinator) MarkLotWorkingCapital(lotID, ref string, amount dec.Decimal) error {
	if lotID == "" {
		return errors.New("perprisk: lot_id required")
	}
	if amount.Sign() < 0 {
		return errors.New("perprisk: working capital must be non-negative")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lot, ok := c.lots[lotID]
	if !ok {
		return errors.New("perprisk: lot not found")
	}
	c.lots[lotID] = lot.WithWorkingCapital(ref, amount)
	return nil
}

func (c *Coordinator) ApplyLotADLResult(lotID string, factQty, realizedPnL dec.Decimal) (TakenOverLot, error) {
	if lotID == "" {
		return TakenOverLot{}, errors.New("perprisk: lot_id required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lot, ok := c.lots[lotID]
	if !ok {
		return TakenOverLot{}, errors.New("perprisk: lot not found")
	}
	next, err := ApplyLotADL(lot, factQty, realizedPnL)
	if err != nil {
		return TakenOverLot{}, err
	}
	c.lots[lotID] = next
	return next, nil
}

func (c *Coordinator) RegisterInFlightADL(task ADLTask) error {
	if task.LotID == "" || task.UserID == 0 || task.Symbol == "" || task.AdlRound == 0 {
		return errors.New("perprisk: in-flight ADL requires lot_id, user_id, symbol, and adl_round")
	}
	if task.Qty.Sign() <= 0 {
		return errors.New("perprisk: in-flight ADL qty must be positive")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlightADL[adlTaskKey(task.LotID, task.UserID, task.AdlRound)] = task
	return nil
}

func (c *Coordinator) CompleteInFlightADL(lotID string, userID uint64, adlRound uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.inFlightADL, adlTaskKey(lotID, userID, adlRound))
}

func (c *Coordinator) SettleLot(lotID string) (RiskPoolSettlement, bool, error) {
	if lotID == "" {
		return RiskPoolSettlement{}, false, errors.New("perprisk: lot_id required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.settlements[lotID]; ok {
		return existing, false, nil
	}
	lot, ok := c.lots[lotID]
	if !ok {
		return RiskPoolSettlement{}, false, errors.New("perprisk: lot not found")
	}
	next, settlement, applied, err := SettleTakenOverLot(lot, coinFromLinearPerp(lot.Symbol))
	if err != nil || !applied {
		return settlement, applied, err
	}
	c.lots[lotID] = next
	if err := c.applyRiskPoolSettlementLocked(settlement); err != nil {
		return RiskPoolSettlement{}, false, err
	}
	return settlement, true, nil
}

func (c *Coordinator) Lot(lotID string) (TakenOverLot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lot, ok := c.lots[lotID]
	return lot, ok
}

func (c *Coordinator) LotForADLPlanning(lotID string) (TakenOverLot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lot, ok := c.lots[lotID]
	if !ok {
		return TakenOverLot{}, false
	}
	reserved := zero
	for _, task := range c.inFlightADL {
		if task.LotID == lotID {
			reserved = reserved.Add(task.Qty)
		}
	}
	// In-flight task qty is only a reservation, not a fill. It is subtracted for
	// planning so replay or a partial ADL result does not dispatch more commands
	// than the lot can absorb; leaves_qty is still reduced only by fact_qty.
	lot.LeavesQty = dec.Max(lot.LeavesQty.Sub(reserved), zero)
	return lot, true
}

func (c *Coordinator) OpenLots() []TakenOverLot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]TakenOverLot, 0, len(c.lots))
	for _, lot := range c.lots {
		if lot.Status != LotStatusDone {
			out = append(out, lot)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LotID < out[j].LotID })
	return out
}

func (c *Coordinator) applyRiskPoolSettlementLocked(s RiskPoolSettlement) error {
	if s.LotID == "" {
		return errors.New("perprisk: settlement lot_id required")
	}
	if s.Coin == "" {
		s.Coin = coinFromLinearPerp(s.Symbol)
	}
	if _, ok := c.settlements[s.LotID]; ok {
		return nil
	}
	c.settlements[s.LotID] = s
	c.funds[s.Coin] = c.funds[s.Coin].Add(s.FinalPoolDelta)
	if lot, ok := c.lots[s.LotID]; ok {
		lot.Status = LotStatusDone
		c.lots[s.LotID] = lot
	}
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

// PlanADL converts a taken-over lot's remaining inventory into version-stamped
// tasks. ADR-0073 stops ADL at leaves_qty == 0; sacrifice_per_qty is only a
// profitability filter/ranking input, not a fund-repair sizing denominator.
func PlanADL(lot TakenOverLot, price dec.Decimal, adlRound uint64, candidates []ADLCandidate) []ADLTask {
	if lot.LotID == "" || lot.LeavesQty.Sign() <= 0 {
		return nil
	}
	ordered := append([]ADLCandidate(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if c := ordered[i].Score.Cmp(ordered[j].Score); c != 0 {
			return c > 0
		}
		return ordered[i].UserID < ordered[j].UserID
	})
	remaining := lot.LeavesQty
	tasks := make([]ADLTask, 0, len(ordered))
	for _, cand := range ordered {
		if remaining.Sign() <= 0 {
			break
		}
		if cand.Size.Sign() <= 0 || cand.SacrificePerQty.Sign() <= 0 {
			continue
		}
		qty := dec.Min(cand.Size, remaining)
		if qty.Sign() <= 0 {
			continue
		}
		tasks = append(tasks, ADLTask{
			LotID: lot.LotID, UserID: cand.UserID, Symbol: cand.Symbol, Side: cand.Side,
			Qty: qty, Price: price, PosSeq: cand.PosSeq,
			PositionVersion: cand.PositionVersion, AdlRound: adlRound,
		})
		remaining = remaining.Sub(qty)
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
	for _, lot := range c.lots {
		s.Lots = append(s.Lots, LotSnap{
			LotID: lot.LotID, UserID: lot.UserID, Symbol: lot.Symbol, Side: uint8(lot.Side),
			TotalQty: lot.TotalQty.String(), LeavesQty: lot.LeavesQty.String(),
			TakeoverPrice: lot.TakeoverPrice.String(), TriggerMarkPrice: lot.TriggerMarkPrice.String(),
			TakenOverBalance: lot.TakenOverBalance.String(), PositionVersion: lot.PositionVersion,
			Status: lot.Status, RealizedPnL: lot.RealizedPnL.String(), CumFee: lot.CumFee.String(),
			WorkingCapitalRef: lot.WorkingCapitalRef, WorkingCapital: lot.WorkingCapital.String(),
			CreatedUnixMs: lot.CreatedUnixMs, UpdatedUnixMs: lot.UpdatedUnixMs,
		})
	}
	sort.Slice(s.Lots, func(i, j int) bool { return s.Lots[i].LotID < s.Lots[j].LotID })
	for _, settlement := range c.settlements {
		s.Settlements = append(s.Settlements, settlementToSnap(settlement))
	}
	sort.Slice(s.Settlements, func(i, j int) bool { return s.Settlements[i].LotID < s.Settlements[j].LotID })
	for _, task := range c.inFlightADL {
		s.InFlightADL = append(s.InFlightADL, InFlightADLSnap{
			LotID: task.LotID, UserID: task.UserID, Symbol: task.Symbol, Side: uint8(task.Side),
			Qty: task.Qty.String(), Price: task.Price.String(), PosSeq: task.PosSeq,
			PositionVersion: task.PositionVersion, AdlRound: task.AdlRound,
		})
	}
	sort.Slice(s.InFlightADL, func(i, j int) bool {
		if s.InFlightADL[i].LotID != s.InFlightADL[j].LotID {
			return s.InFlightADL[i].LotID < s.InFlightADL[j].LotID
		}
		if s.InFlightADL[i].AdlRound != s.InFlightADL[j].AdlRound {
			return s.InFlightADL[i].AdlRound < s.InFlightADL[j].AdlRound
		}
		return s.InFlightADL[i].UserID < s.InFlightADL[j].UserID
	})
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
	c.lots = map[string]TakenOverLot{}
	c.settlements = map[string]RiskPoolSettlement{}
	c.inFlightADL = map[string]ADLTask{}
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
	for _, l := range s.Lots {
		c.lots[l.LotID] = TakenOverLot{
			LotID: l.LotID, UserID: l.UserID, Symbol: l.Symbol, Side: perpstate.Side(l.Side),
			TotalQty: dec.New(l.TotalQty), LeavesQty: dec.New(l.LeavesQty),
			TakeoverPrice: dec.New(l.TakeoverPrice), TriggerMarkPrice: dec.New(l.TriggerMarkPrice),
			TakenOverBalance: dec.New(l.TakenOverBalance), PositionVersion: l.PositionVersion,
			Status: l.Status, RealizedPnL: dec.New(l.RealizedPnL), CumFee: dec.New(l.CumFee),
			WorkingCapitalRef: l.WorkingCapitalRef, WorkingCapital: dec.New(l.WorkingCapital),
			CreatedUnixMs: l.CreatedUnixMs, UpdatedUnixMs: l.UpdatedUnixMs,
		}
	}
	for _, s := range s.Settlements {
		c.settlements[s.LotID] = settlementFromSnap(s)
	}
	for _, t := range s.InFlightADL {
		task := ADLTask{
			LotID: t.LotID, UserID: t.UserID, Symbol: t.Symbol, Side: perpstate.Side(t.Side),
			Qty: dec.New(t.Qty), Price: dec.New(t.Price), PosSeq: t.PosSeq,
			PositionVersion: t.PositionVersion, AdlRound: t.AdlRound,
		}
		c.inFlightADL[adlTaskKey(task.LotID, task.UserID, task.AdlRound)] = task
	}
	for _, o := range s.Offsets {
		c.offsets[o.Partition] = o.Next
	}
}

func lotFromTakeoverEvent(t *eventpb.PerpTakeoverEvent, ts int64) (TakenOverLot, dec.Decimal, string, error) {
	lotID := t.GetLotId()
	if lotID == "" {
		lotID = t.GetSymbol() + ":" + strconv.FormatUint(t.GetLiqOrderId(), 10)
	}
	qtyText := t.GetTakenOverQty()
	if qtyText == "" {
		qtyText = t.GetClosedQty()
	}
	qty, err := dec.Parse(qtyText)
	if err != nil {
		return TakenOverLot{}, zero, "", fmt.Errorf("perprisk: takeover taken_over_qty: %w", err)
	}
	priceText := t.GetTakeoverPrice()
	if priceText == "" {
		priceText = t.GetBankruptcyPrice()
	}
	price, err := dec.Parse(priceText)
	if err != nil {
		return TakenOverLot{}, zero, "", fmt.Errorf("perprisk: takeover takeover_price: %w", err)
	}
	mark, err := dec.Parse(t.GetMarkPrice())
	if err != nil {
		return TakenOverLot{}, zero, "", fmt.Errorf("perprisk: takeover mark_price: %w", err)
	}
	balanceText := t.GetTakenOverBalance()
	if balanceText == "" {
		// Legacy takeover events only carried insurance_delta. In ADR-0073 terms
		// that value is the user-side balance transferred into the lot, not an
		// immediate fund movement.
		balanceText = t.GetInsuranceDelta()
	}
	balance, err := dec.Parse(balanceText)
	if err != nil {
		return TakenOverLot{}, zero, "", fmt.Errorf("perprisk: takeover taken_over_balance: %w", err)
	}
	borrowAmount, err := dec.Parse(t.GetTakeoverNotional())
	if err != nil {
		return TakenOverLot{}, zero, "", fmt.Errorf("perprisk: takeover takeover_notional: %w", err)
	}
	if borrowAmount.Sign() <= 0 {
		borrowAmount = qty.Mul(price)
	}
	version := t.GetPositionVersion()
	if version == 0 && t.GetPositionAfter() != nil {
		version = t.GetPositionAfter().GetVersion()
	}
	lot, err := NewTakenOverLot(TakenOverLot{
		LotID: lotID, UserID: t.GetUserId(), Symbol: t.GetSymbol(),
		Side:     eventSideToPerp(t.GetInventorySide()),
		TotalQty: qty, LeavesQty: qty, TakeoverPrice: price,
		TriggerMarkPrice: mark, TakenOverBalance: balance,
		PositionVersion: version, Status: LotStatusInit,
		CreatedUnixMs: ts, UpdatedUnixMs: ts,
	})
	if err != nil {
		return TakenOverLot{}, zero, "", err
	}
	return lot, borrowAmount, "takeover:" + lotID, nil
}

func settlementFromEvent(e *eventpb.RiskPoolSettlementEvent) (RiskPoolSettlement, error) {
	takenOverBalance, err := dec.Parse(e.GetTakenOverBalance())
	if err != nil {
		return RiskPoolSettlement{}, fmt.Errorf("perprisk: settlement taken_over_balance: %w", err)
	}
	realized, err := dec.Parse(e.GetLiqAdlRealisedPnl())
	if err != nil {
		return RiskPoolSettlement{}, fmt.Errorf("perprisk: settlement liq_adl_realised_pnl: %w", err)
	}
	fee, err := dec.Parse(e.GetCumFee())
	if err != nil {
		return RiskPoolSettlement{}, fmt.Errorf("perprisk: settlement cum_fee: %w", err)
	}
	workingCapital, err := dec.Parse(e.GetWorkingCapitalDrawn())
	if err != nil {
		return RiskPoolSettlement{}, fmt.Errorf("perprisk: settlement working_capital_drawn: %w", err)
	}
	borrowed, err := dec.Parse(e.GetBorrowedBalance())
	if err != nil {
		return RiskPoolSettlement{}, fmt.Errorf("perprisk: settlement borrowed_balance: %w", err)
	}
	delta, err := dec.Parse(e.GetFinalPoolDelta())
	if err != nil {
		return RiskPoolSettlement{}, fmt.Errorf("perprisk: settlement final_pool_delta: %w", err)
	}
	return RiskPoolSettlement{
		LotID: e.GetLotId(), Symbol: e.GetSymbol(), Coin: e.GetCoin(),
		WorkingCapitalRef: e.GetWorkingCapitalRef(),
		TakenOverBalance:  takenOverBalance, LiqAdlRealizedPnL: realized,
		CumFee: fee, WorkingCapitalDrawn: workingCapital,
		BorrowedBalance: borrowed, FinalPoolDelta: delta,
	}, nil
}

func settlementToSnap(s RiskPoolSettlement) RiskPoolSettlementSnap {
	return RiskPoolSettlementSnap{
		LotID: s.LotID, Symbol: s.Symbol, Coin: s.Coin,
		WorkingCapitalRef:   s.WorkingCapitalRef,
		TakenOverBalance:    s.TakenOverBalance.String(),
		LiqAdlRealizedPnL:   s.LiqAdlRealizedPnL.String(),
		CumFee:              s.CumFee.String(),
		WorkingCapitalDrawn: s.WorkingCapitalDrawn.String(),
		BorrowedBalance:     s.BorrowedBalance.String(),
		FinalPoolDelta:      s.FinalPoolDelta.String(),
	}
}

func settlementFromSnap(s RiskPoolSettlementSnap) RiskPoolSettlement {
	return RiskPoolSettlement{
		LotID: s.LotID, Symbol: s.Symbol, Coin: s.Coin,
		WorkingCapitalRef:   s.WorkingCapitalRef,
		TakenOverBalance:    dec.New(s.TakenOverBalance),
		LiqAdlRealizedPnL:   dec.New(s.LiqAdlRealizedPnL),
		CumFee:              dec.New(s.CumFee),
		WorkingCapitalDrawn: dec.New(s.WorkingCapitalDrawn),
		BorrowedBalance:     dec.New(s.BorrowedBalance),
		FinalPoolDelta:      dec.New(s.FinalPoolDelta),
	}
}

func tsFromEventMeta(meta *eventpb.EventMeta) int64 {
	if meta == nil {
		return 0
	}
	return meta.GetTsUnixMs()
}

func eventSideToPerp(side eventpb.Side) perpstate.Side {
	switch side {
	case eventpb.Side_SIDE_BUY:
		return perpstate.SideBuy
	case eventpb.Side_SIDE_SELL:
		return perpstate.SideSell
	default:
		return 0
	}
}

func adlTaskKey(lotID string, userID uint64, round uint64) string {
	return lotID + "|" + strconv.FormatUint(userID, 10) + "|" + strconv.FormatUint(round, 10)
}

func coinFromLinearPerp(symbol string) string {
	parts := strings.Split(symbol, "-")
	if len(parts) >= 2 && parts[1] != "" {
		return parts[1]
	}
	return "USDT"
}
