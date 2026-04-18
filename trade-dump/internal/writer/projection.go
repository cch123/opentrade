package writer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	eventpb "github.com/xargin/opentrade/api/gen/event"
)

// -----------------------------------------------------------------------------
// Row types — direct 1:1 projection onto deploy/docker/mysql-init/01-schema.sql.
// -----------------------------------------------------------------------------

// OrderRow mirrors the `orders` table. Inserted on FreezeEvent, updated by
// OrderStatusEvent. frozen_amt is populated from FreezeEvent once; later
// events leave it as-is (InsertOnConflictIgnore semantics on re-insert).
type OrderRow struct {
	OrderID       uint64
	ClientOrderID string
	UserID        string
	Symbol        string
	Side          int8
	OrderType     int8
	TIF           int8
	Price         string
	Qty           string
	FrozenAmt     string
	// Status / FilledQty / RejectReason are always set; for FreezeEvent
	// they reflect the initial PENDING_NEW state.
	Status       int8
	FilledQty    string
	RejectReason int8
	// CreatedAtMs is filled for FreezeEvent rows (which always insert).
	// UpdatedAtMs is filled for every event.
	CreatedAtMs int64
	UpdatedAtMs int64
	// Kind distinguishes INSERT-new (FreezeEvent) from UPDATE-existing
	// (OrderStatusEvent). The writer issues different SQL for each.
	Kind OrderRowKind
}

// OrderRowKind tells the writer which SQL form to use.
type OrderRowKind uint8

const (
	OrderRowInsert OrderRowKind = 1 // Freeze = new PENDING_NEW row
	OrderRowUpdate OrderRowKind = 2 // OrderStatus = status / filled_qty change
)

// AccountRow mirrors the `accounts` table. Upserted on every event whose
// CounterJournalEvent payload carries a BalanceSnapshot. seq_id is the
// shard-monotonic id at event emission; writer uses it to guard against
// out-of-order replays.
type AccountRow struct {
	UserID    string
	Asset     string
	Available string
	Frozen    string
	SeqID     uint64
}

// AccountLogRow mirrors the `account_logs` table — one row per (shard, seq,
// asset). Inserted with ON DUPLICATE KEY UPDATE no-op so replays are
// idempotent.
type AccountLogRow struct {
	ShardID      int32
	SeqID        uint64
	Asset        string
	UserID       string
	DeltaAvail   string
	DeltaFrozen  string
	AvailAfter   string
	FrozenAfter  string
	BizType      string
	BizRefID     string
	TsUnixMs     int64
}

// JournalBatch is the aggregate projection of a consumed slice of
// CounterJournalEvents. Writer applies all three slices inside a single
// MySQL transaction.
type JournalBatch struct {
	Orders      []OrderRow
	Accounts    []AccountRow
	AccountLogs []AccountLogRow
}

// IsEmpty reports whether the batch has nothing to write.
func (b *JournalBatch) IsEmpty() bool {
	return len(b.Orders) == 0 && len(b.Accounts) == 0 && len(b.AccountLogs) == 0
}

// JournalWriter is the contract the consumer uses against MySQL. Real impl
// in mysql_journal.go; tests can fake it.
type JournalWriter interface {
	ApplyJournalBatch(ctx context.Context, batch JournalBatch) error
}

// -----------------------------------------------------------------------------
// Pure builder: CounterJournalEvent → JournalBatch
// -----------------------------------------------------------------------------

// BuildJournalBatch projects a slice of events into a MySQL write batch. It
// is a pure function with no I/O. Unknown or malformed events are skipped
// (with a returned-error-but-partial-batch signal lost on purpose — batch
// writing must be all-or-nothing within the consumed Kafka records, and a
// single bad event should never block progress).
//
// The shardID is parsed from EventMeta.producer_id (e.g.
// "counter-shard-3-main" → 3). Events without a parseable shard id are
// skipped from account_logs (the other tables don't need shard_id).
func BuildJournalBatch(events []*eventpb.CounterJournalEvent) JournalBatch {
	var batch JournalBatch
	for _, evt := range events {
		if evt == nil {
			continue
		}
		seq, ts := metaFor(evt.Meta)
		shardID, hasShard := shardIDFromProducer(evt.Meta.GetProducerId())

		switch p := evt.Payload.(type) {
		case *eventpb.CounterJournalEvent_Freeze:
			appendFromFreeze(&batch, p.Freeze, seq, ts, shardID, hasShard)
		case *eventpb.CounterJournalEvent_Unfreeze:
			appendFromUnfreeze(&batch, p.Unfreeze, seq, ts, shardID, hasShard)
		case *eventpb.CounterJournalEvent_Settlement:
			appendFromSettlement(&batch, p.Settlement, seq, ts, shardID, hasShard)
		case *eventpb.CounterJournalEvent_Transfer:
			appendFromTransfer(&batch, p.Transfer, seq, ts, shardID, hasShard)
		case *eventpb.CounterJournalEvent_OrderStatus:
			appendFromOrderStatus(&batch, p.OrderStatus, ts)
		case *eventpb.CounterJournalEvent_CancelReq:
			// CancelRequested is purely informational for trade-dump — the
			// pending-cancel state also arrives via OrderStatusEvent. Skip.
		}
	}
	return batch
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func metaFor(meta *eventpb.EventMeta) (seqID uint64, tsMs int64) {
	if meta == nil {
		return 0, time.Now().UnixMilli()
	}
	if meta.TsUnixMs == 0 {
		return meta.SeqId, time.Now().UnixMilli()
	}
	return meta.SeqId, meta.TsUnixMs
}

// shardIDFromProducer extracts N from "counter-shard-N-<role>". Returns
// (0, false) if the format does not match.
func shardIDFromProducer(producerID string) (int32, bool) {
	const prefix = "counter-shard-"
	if !strings.HasPrefix(producerID, prefix) {
		return 0, false
	}
	rest := producerID[len(prefix):]
	// rest = "<N>-<role>"
	dash := strings.IndexByte(rest, '-')
	if dash < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(rest[:dash])
	if err != nil || n < 0 {
		return 0, false
	}
	return int32(n), true
}

func appendFromFreeze(b *JournalBatch, e *eventpb.FreezeEvent, seq uint64, ts int64, shardID int32, hasShard bool) {
	if e == nil {
		return
	}
	b.Orders = append(b.Orders, OrderRow{
		OrderID:       e.OrderId,
		ClientOrderID: e.ClientOrderId,
		UserID:        e.UserId,
		Symbol:        e.Symbol,
		Side:          int8(e.Side),
		OrderType:     int8(e.OrderType),
		TIF:           int8(e.Tif),
		Price:         e.Price,
		Qty:           e.Qty,
		FrozenAmt:     e.FreezeAmount,
		Status:        int8(eventpb.InternalOrderStatus_INTERNAL_ORDER_STATUS_PENDING_NEW),
		FilledQty:     "0",
		CreatedAtMs:   ts,
		UpdatedAtMs:   ts,
		Kind:          OrderRowInsert,
	})
	if snap := e.BalanceAfter; snap != nil {
		b.Accounts = append(b.Accounts, accountRowFromSnap(snap, seq))
		if hasShard {
			b.AccountLogs = append(b.AccountLogs, AccountLogRow{
				ShardID:     shardID,
				SeqID:       seq,
				Asset:       snap.Asset,
				UserID:      e.UserId,
				DeltaAvail:  "-" + trimDecimal(e.FreezeAmount),
				DeltaFrozen: trimDecimal(e.FreezeAmount),
				AvailAfter:  snap.Available,
				FrozenAfter: snap.Frozen,
				BizType:     "freeze_place_order",
				BizRefID:    fmt.Sprintf("%d", e.OrderId),
				TsUnixMs:    ts,
			})
		}
	}
}

func appendFromUnfreeze(b *JournalBatch, e *eventpb.UnfreezeEvent, seq uint64, ts int64, shardID int32, hasShard bool) {
	if e == nil {
		return
	}
	if snap := e.BalanceAfter; snap != nil {
		b.Accounts = append(b.Accounts, accountRowFromSnap(snap, seq))
		if hasShard {
			b.AccountLogs = append(b.AccountLogs, AccountLogRow{
				ShardID:     shardID,
				SeqID:       seq,
				Asset:       snap.Asset,
				UserID:      e.UserId,
				DeltaAvail:  trimDecimal(e.Amount),
				DeltaFrozen: "-" + trimDecimal(e.Amount),
				AvailAfter:  snap.Available,
				FrozenAfter: snap.Frozen,
				BizType:     "unfreeze",
				BizRefID:    fmt.Sprintf("%d", e.OrderId),
				TsUnixMs:    ts,
			})
		}
	}
}

func appendFromSettlement(b *JournalBatch, e *eventpb.SettlementEvent, seq uint64, ts int64, shardID int32, hasShard bool) {
	if e == nil {
		return
	}
	// base leg
	if snap := e.BaseBalanceAfter; snap != nil {
		b.Accounts = append(b.Accounts, accountRowFromSnap(snap, seq))
		if hasShard {
			b.AccountLogs = append(b.AccountLogs, AccountLogRow{
				ShardID:     shardID,
				SeqID:       seq,
				Asset:       snap.Asset,
				UserID:      e.UserId,
				DeltaAvail:  trimDecimal(e.DeltaBase),
				DeltaFrozen: negateDecimal(e.UnfreezeBase),
				AvailAfter:  snap.Available,
				FrozenAfter: snap.Frozen,
				BizType:     "settlement",
				BizRefID:    e.TradeId,
				TsUnixMs:    ts,
			})
		}
	}
	// quote leg
	if snap := e.QuoteBalanceAfter; snap != nil {
		b.Accounts = append(b.Accounts, accountRowFromSnap(snap, seq))
		if hasShard {
			b.AccountLogs = append(b.AccountLogs, AccountLogRow{
				ShardID:     shardID,
				SeqID:       seq,
				Asset:       snap.Asset,
				UserID:      e.UserId,
				DeltaAvail:  trimDecimal(e.DeltaQuote),
				DeltaFrozen: negateDecimal(e.UnfreezeQuote),
				AvailAfter:  snap.Available,
				FrozenAfter: snap.Frozen,
				BizType:     "settlement",
				BizRefID:    e.TradeId,
				TsUnixMs:    ts,
			})
		}
	}
}

func appendFromTransfer(b *JournalBatch, e *eventpb.TransferEvent, seq uint64, ts int64, shardID int32, hasShard bool) {
	if e == nil {
		return
	}
	snap := e.BalanceAfter
	if snap == nil {
		return
	}
	b.Accounts = append(b.Accounts, accountRowFromSnap(snap, seq))
	if !hasShard {
		return
	}
	deltaAvail, deltaFrozen := transferDeltas(e.Type, e.Amount)
	b.AccountLogs = append(b.AccountLogs, AccountLogRow{
		ShardID:     shardID,
		SeqID:       seq,
		Asset:       snap.Asset,
		UserID:      e.UserId,
		DeltaAvail:  deltaAvail,
		DeltaFrozen: deltaFrozen,
		AvailAfter:  snap.Available,
		FrozenAfter: snap.Frozen,
		BizType:     transferBizType(e.Type),
		BizRefID:    firstNonEmpty(e.BizRefId, e.TransferId),
		TsUnixMs:    ts,
	})
}

func appendFromOrderStatus(b *JournalBatch, e *eventpb.OrderStatusEvent, ts int64) {
	if e == nil {
		return
	}
	b.Orders = append(b.Orders, OrderRow{
		OrderID:      e.OrderId,
		UserID:       e.UserId,
		Status:       int8(e.NewStatus),
		FilledQty:    defaultZero(e.FilledQty),
		RejectReason: int8(e.RejectReason),
		UpdatedAtMs:  ts,
		Kind:         OrderRowUpdate,
	})
}

func accountRowFromSnap(snap *eventpb.BalanceSnapshot, seq uint64) AccountRow {
	return AccountRow{
		UserID:    snap.UserId,
		Asset:     snap.Asset,
		Available: snap.Available,
		Frozen:    snap.Frozen,
		SeqID:     seq,
	}
}

// transferDeltas produces (delta_avail, delta_frozen) wire-form strings for
// the journal. deposit / withdraw touch available only; freeze / unfreeze
// shuffle between available and frozen.
func transferDeltas(t eventpb.TransferEvent_TransferType, amount string) (string, string) {
	switch t {
	case eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT:
		return trimDecimal(amount), "0"
	case eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW:
		return negateDecimal(amount), "0"
	case eventpb.TransferEvent_TRANSFER_TYPE_FREEZE:
		return negateDecimal(amount), trimDecimal(amount)
	case eventpb.TransferEvent_TRANSFER_TYPE_UNFREEZE:
		return trimDecimal(amount), negateDecimal(amount)
	}
	return "0", "0"
}

func transferBizType(t eventpb.TransferEvent_TransferType) string {
	switch t {
	case eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT:
		return "deposit"
	case eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW:
		return "withdraw"
	case eventpb.TransferEvent_TRANSFER_TYPE_FREEZE:
		return "freeze"
	case eventpb.TransferEvent_TRANSFER_TYPE_UNFREEZE:
		return "unfreeze"
	}
	return "transfer_unknown"
}

func trimDecimal(s string) string {
	if s == "" {
		return "0"
	}
	return s
}

// negateDecimal returns -s as a decimal string. Inputs are always produced
// by Counter via shopspring/decimal.String(), so they're free of leading
// whitespace or explicit '+'.
func negateDecimal(s string) string {
	if s == "" || s == "0" {
		return "0"
	}
	if strings.HasPrefix(s, "-") {
		return s[1:]
	}
	return "-" + s
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func defaultZero(s string) string {
	if s == "" {
		return "0"
	}
	return s
}
