// Package cursor contains opaque cursor codecs for HistoryService paging.
//
// Cursors are returned verbatim from List* responses and fed back unmodified
// into the next request. Clients MUST treat them as opaque — the on-wire
// encoding is base64(JSON) today but may change. Each RPC has its own struct
// so the MySQL WHERE clause can include a unique, strictly-monotone tail of
// columns (ts + unique-PK suffix) to avoid duplicates on cursor boundaries.
package cursor

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

// OrdersCursor paginates ListOrders. `orders` table orders by
// (created_at DESC, order_id DESC). The cursor captures the last row seen.
type OrdersCursor struct {
	CreatedAt int64  `json:"c"`
	OrderID   uint64 `json:"o"`
}

// TradesCursor paginates ListTrades. `trades` table orders by
// (ts DESC, trade_id DESC).
type TradesCursor struct {
	Ts      int64  `json:"t"`
	TradeID string `json:"i"`
}

// AccountLogsCursor paginates ListAccountLogs. `account_logs` orders by
// (ts DESC, vshard_id DESC, counter_seq_id DESC, asset DESC) — a unique key
// to ensure strict monotonicity on the cursor boundary. ADR-0058 renamed
// shard_id → vshard_id; JSON tag kept as "s" to avoid a cursor-format
// break.
type AccountLogsCursor struct {
	Ts           int64  `json:"t"`
	VShardID     int32  `json:"s"`
	CounterSeqID uint64 `json:"q"`
	Asset        string `json:"a"`
}

// TriggersCursor paginates ListTriggers. `triggers` orders by
// (created_at DESC, id DESC). The id column is a monotonic snowflake so
// (created_at, id) is unique in practice.
type TriggersCursor struct {
	CreatedAt int64  `json:"c"`
	ID        uint64 `json:"i"`
}

// TransfersCursor paginates ListTransfers. `transfers` orders by
// (created_at_ms DESC, transfer_id DESC). transfer_id is a client-
// generated unique string so the pair is strictly monotone in practice.
type TransfersCursor struct {
	CreatedAt  int64  `json:"c"`
	TransferID string `json:"t"`
}

var enc = base64.RawURLEncoding

// ErrInvalid is returned when a cursor string fails to decode. Callers
// should surface this as InvalidArgument at the RPC boundary.
var ErrInvalid = errors.New("invalid cursor")

// Encode marshals any cursor value to the on-wire representation. Empty
// struct is valid and encodes to a non-empty string; callers that want
// "no cursor" should pass "" on the wire instead.
func Encode(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return enc.EncodeToString(raw), nil
}

// Decode unmarshals a wire cursor into dst. Empty input leaves dst at its
// zero value and returns nil — callers can just pass the user's cursor
// without a "if empty skip" guard.
func Decode(s string, dst any) error {
	if s == "" {
		return nil
	}
	raw, err := enc.DecodeString(s)
	if err != nil {
		return ErrInvalid
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return ErrInvalid
	}
	return nil
}
