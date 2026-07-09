package engine

import (
	"fmt"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/pkg/dec"
)

// ApplyJournalEvent applies one trigger-event envelope during startup
// recovery. TriggerUpdate carries a complete post-change record, so replay is
// an in-memory upsert and must not call Counter, release reservations, or emit
// another journal record. A market checkpoint only advances the two price-feed
// cursors; this mirrors trade-dump's shadow apply for checkpoints that landed
// after the restored snapshot.
func (e *Engine) ApplyJournalEvent(evt *eventpb.TriggerEvent) error {
	if evt == nil {
		return fmt.Errorf("trigger: nil journal event")
	}
	switch payload := evt.Payload.(type) {
	case *eventpb.TriggerEvent_Update:
		return e.applyJournalUpdate(payload.Update)
	case *eventpb.TriggerEvent_MarketCheckpoint:
		return e.applyJournalCheckpoint(payload.MarketCheckpoint)
	default:
		return fmt.Errorf("trigger: unknown journal payload %T", evt.Payload)
	}
}

func (e *Engine) applyJournalCheckpoint(checkpoint *eventpb.TriggerMarketCheckpointEvent) error {
	if checkpoint == nil {
		return fmt.Errorf("trigger: nil TriggerMarketCheckpointEvent")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for partition, offset := range checkpoint.MarketOffsets {
		if current, ok := e.offsets[partition]; !ok || offset > current {
			e.offsets[partition] = offset
		}
	}
	for partition, offset := range checkpoint.PerpPriceOffsets {
		if current, ok := e.perpOffsets[partition]; !ok || offset > current {
			e.perpOffsets[partition] = offset
		}
	}
	return nil
}

func (e *Engine) applyJournalUpdate(update *eventpb.TriggerUpdate) error {
	c, err := triggerFromJournalUpdate(update)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if c.OCOGroupID != "" {
		e.knownOCOGroups[c.OCOGroupID] = struct{}{}
	}

	if isJournalTerminal(c.Status) {
		e.applyJournalTerminalLocked(c)
		e.cascadeJournalOCOLocked(c)
		return nil
	}

	// Terminal is irreversible. Ignoring a stale/duplicate PENDING update
	// protects against replay mistakes resurrecting a canceled or fired order.
	if _, terminal := e.terminals[c.ID]; terminal {
		return nil
	}
	if old, exists := e.pending[c.ID]; exists {
		if old.UserID != c.UserID || old.Symbol != c.Symbol {
			e.decActiveTriggerLocked(old.UserID, old.Symbol)
			e.incActiveTriggerLocked(c.UserID, c.Symbol)
		}
		e.removeJournalClientKeyLocked(old, c.ClientTriggerID)
	} else {
		e.incActiveTriggerLocked(c.UserID, c.Symbol)
	}
	e.pending[c.ID] = c
	if c.ClientTriggerID != "" {
		e.byClient[triggerClientKey(c.UserID, c.ClientTriggerID)] = c.ID
	}
	return nil
}

// cascadeJournalOCOLocked reconstructs the atomic OCO invariant if a crash
// left only the first terminal update durable. Normal operation emits the
// sibling CANCELED updates immediately afterward; replaying those later is
// idempotent through applyJournalTerminalLocked.
func (e *Engine) cascadeJournalOCOLocked(primary *Trigger) {
	if primary.OCOGroupID == "" {
		return
	}
	for id, sibling := range e.pending {
		if id == primary.ID || sibling.OCOGroupID != primary.OCOGroupID {
			continue
		}
		canceled := *sibling
		canceled.Status = condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED
		canceled.TriggeredAtMs = primary.TriggeredAtMs
		if canceled.RejectReason == "" {
			canceled.RejectReason = "sibling OCO leg terminated during recovery"
		}
		e.applyJournalTerminalLocked(&canceled)
	}
}

func (e *Engine) applyJournalTerminalLocked(c *Trigger) {
	if old, exists := e.terminals[c.ID]; exists {
		// Re-applying the same terminal record must not append another FIFO
		// entry; otherwise retries would evict unrelated terminal history.
		e.removeJournalClientKeyLocked(old, c.ClientTriggerID)
		e.terminals[c.ID] = c
		if c.ClientTriggerID != "" {
			e.byClient[triggerClientKey(c.UserID, c.ClientTriggerID)] = c.ID
		}
		return
	}

	if old, exists := e.pending[c.ID]; exists {
		e.decActiveTriggerLocked(old.UserID, old.Symbol)
		e.removeJournalClientKeyLocked(old, c.ClientTriggerID)
		delete(e.pending, c.ID)
	}
	e.terminals[c.ID] = c
	e.termOrder = append(e.termOrder, c.ID)
	if c.ClientTriggerID != "" {
		e.byClient[triggerClientKey(c.UserID, c.ClientTriggerID)] = c.ID
	}
	if e.cfg.TerminalHistoryLimit <= 0 {
		return
	}
	for len(e.termOrder) > e.cfg.TerminalHistoryLimit {
		drop := e.termOrder[0]
		e.termOrder = e.termOrder[1:]
		if terminal, ok := e.terminals[drop]; ok {
			if terminal.ClientTriggerID != "" {
				delete(e.byClient, triggerClientKey(terminal.UserID, terminal.ClientTriggerID))
			}
			delete(e.terminals, drop)
		}
	}
}

func (e *Engine) removeJournalClientKeyLocked(old *Trigger, replacement string) {
	if old == nil || old.ClientTriggerID == "" || old.ClientTriggerID == replacement {
		return
	}
	key := triggerClientKey(old.UserID, old.ClientTriggerID)
	if id, ok := e.byClient[key]; ok && id == old.ID {
		delete(e.byClient, key)
	}
}

func triggerFromJournalUpdate(update *eventpb.TriggerUpdate) (*Trigger, error) {
	if update == nil {
		return nil, fmt.Errorf("trigger: nil TriggerUpdate")
	}
	if update.Id == 0 {
		return nil, fmt.Errorf("trigger: TriggerUpdate.id is zero")
	}
	if update.UserId == 0 || update.Symbol == "" {
		return nil, fmt.Errorf("trigger %d: user_id and symbol required", update.Id)
	}
	status := condrpc.TriggerStatus(update.Status)
	if status != condrpc.TriggerStatus_TRIGGER_STATUS_PENDING && !isJournalTerminal(status) {
		return nil, fmt.Errorf("trigger %d: invalid journal status %d", update.Id, update.Status)
	}
	typ := condrpc.TriggerType(update.Type)
	switch typ {
	case condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS,
		condrpc.TriggerType_TRIGGER_TYPE_STOP_LOSS_LIMIT,
		condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT,
		condrpc.TriggerType_TRIGGER_TYPE_TAKE_PROFIT_LIMIT,
		condrpc.TriggerType_TRIGGER_TYPE_TRAILING_STOP_LOSS:
	default:
		return nil, fmt.Errorf("trigger %d: invalid journal type %d", update.Id, update.Type)
	}
	if update.Side != eventpb.Side_SIDE_BUY && update.Side != eventpb.Side_SIDE_SELL {
		return nil, fmt.Errorf("trigger %d: invalid journal side %d", update.Id, update.Side)
	}
	parse := func(field, value string) (dec.Decimal, error) {
		d, err := dec.Parse(value)
		if err != nil {
			return dec.Decimal{}, fmt.Errorf("trigger %d %s: %w", update.Id, field, err)
		}
		return d, nil
	}
	stop, err := parse("stop_price", update.StopPrice)
	if err != nil {
		return nil, err
	}
	limit, err := parse("limit_price", update.LimitPrice)
	if err != nil {
		return nil, err
	}
	qty, err := parse("qty", update.Qty)
	if err != nil {
		return nil, err
	}
	quoteQty, err := parse("quote_qty", update.QuoteQty)
	if err != nil {
		return nil, err
	}
	activation, err := parse("activation_price", update.ActivationPrice)
	if err != nil {
		return nil, err
	}
	watermark, err := parse("trailing_watermark", update.TrailingWatermark)
	if err != nil {
		return nil, err
	}

	return &Trigger{
		ID:                update.Id,
		ClientTriggerID:   update.ClientTriggerId,
		UserID:            update.UserId,
		Symbol:            update.Symbol,
		Side:              update.Side,
		Type:              typ,
		StopPrice:         stop,
		LimitPrice:        limit,
		Qty:               qty,
		QuoteQty:          quoteQty,
		TIF:               update.Tif,
		Status:            status,
		CreatedAtMs:       update.CreatedAtUnixMs,
		TriggeredAtMs:     update.TriggeredAtUnixMs,
		PlacedOrderID:     update.PlacedOrderId,
		RejectReason:      update.RejectReason,
		ExpiresAtMs:       update.ExpiresAtUnixMs,
		OCOGroupID:        update.OcoGroupId,
		TrailingDeltaBps:  update.TrailingDeltaBps,
		ActivationPrice:   activation,
		TrailingWatermark: watermark,
		TrailingActive:    update.TrailingActive,
		Perp:              update.Perp,
		PositionIdx:       update.PositionIdx,
		CloseOnTrigger:    update.CloseOnTrigger,
		SlippageBps:       update.SlippageBps,
	}, nil
}

func isJournalTerminal(status condrpc.TriggerStatus) bool {
	switch status {
	case condrpc.TriggerStatus_TRIGGER_STATUS_TRIGGERED,
		condrpc.TriggerStatus_TRIGGER_STATUS_CANCELED,
		condrpc.TriggerStatus_TRIGGER_STATUS_REJECTED,
		condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED,
		condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED_IN_MATCH,
		condrpc.TriggerStatus_TRIGGER_STATUS_EXPIRED_POSITION_GONE:
		return true
	default:
		return false
	}
}
