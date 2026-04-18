package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// Client wraps the BFF REST API. Auth is the dev-mode X-User-Id header (ADR-0039
// header scheme) — no token needed, tui rotates it per active user.
type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// ---------------------------------------------------------------------------
// Request/response types
// ---------------------------------------------------------------------------

type TransferReq struct {
	TransferID string `json:"transfer_id"`
	Asset      string `json:"asset"`
	Amount     string `json:"amount"`
	Type       string `json:"type"` // deposit / withdraw / freeze / unfreeze
}

type TransferResp struct {
	TransferID     string `json:"transfer_id"`
	Status         string `json:"status"`
	RejectReason   string `json:"reject_reason"`
	AvailableAfter string `json:"available_after"`
	FrozenAfter    string `json:"frozen_after"`
}

type PlaceOrderReq struct {
	ClientOrderID string `json:"client_order_id,omitempty"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	OrderType     string `json:"order_type"`
	TIF           string `json:"tif,omitempty"`
	Price         string `json:"price,omitempty"`
	Qty           string `json:"qty,omitempty"`
	QuoteQty      string `json:"quote_qty,omitempty"`
}

type PlaceOrderResp struct {
	OrderID          uint64 `json:"order_id"`
	ClientOrderID    string `json:"client_order_id"`
	Status           string `json:"status"`
	Accepted         bool   `json:"accepted"`
	ReceivedTsUnixMs int64  `json:"received_ts_unix_ms"`
}

type CancelOrderResp struct {
	OrderID  uint64 `json:"order_id"`
	Accepted bool   `json:"accepted"`
}

type Balance struct {
	Asset     string `json:"asset"`
	Available string `json:"available"`
	Frozen    string `json:"frozen"`
}

type BalancesResp struct {
	Balances []Balance `json:"balances"`
}

type OrderRow struct {
	OrderID       uint64 `json:"order_id"`
	ClientOrderID string `json:"client_order_id"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	OrderType     string `json:"order_type"`
	TIF           string `json:"tif"`
	Price         string `json:"price"`
	Qty           string `json:"qty"`
	FilledQty     string `json:"filled_qty"`
	Status        string `json:"status"`
	CreatedAt     int64  `json:"created_at"`
	UpdatedAt     int64  `json:"updated_at"`
}

type ListOrdersResp struct {
	Orders     []OrderRow `json:"orders"`
	NextCursor string     `json:"next_cursor"`
}

type TradeRow struct {
	TradeID uint64 `json:"trade_id"`
	Symbol  string `json:"symbol"`
	Price   string `json:"price"`
	Qty     string `json:"qty"`
	Role    string `json:"role"`
	OrderID uint64 `json:"order_id"`
	Side    string `json:"side"`
	Ts      int64  `json:"ts"`
}

type ListTradesResp struct {
	Trades     []TradeRow `json:"trades"`
	NextCursor string     `json:"next_cursor"`
}

// ---------------------------------------------------------------------------
// REST helpers
// ---------------------------------------------------------------------------

type apiError struct {
	status int
	body   string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("http %d: %s", e.status, strings.TrimSpace(e.body))
}

func (c *Client) do(ctx context.Context, method, path, user string, body, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if user != "" {
		req.Header.Set("X-User-Id", user)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		// try to extract {"error": "..."} for friendlier output
		var env struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &env) == nil && env.Error != "" {
			return &apiError{status: resp.StatusCode, body: env.Error}
		}
		return &apiError{status: resp.StatusCode, body: string(raw)}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (c *Client) Transfer(ctx context.Context, user string, req TransferReq) (*TransferResp, error) {
	var out TransferResp
	if err := c.do(ctx, http.MethodPost, "/v1/transfer", user, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) PlaceOrder(ctx context.Context, user string, req PlaceOrderReq) (*PlaceOrderResp, error) {
	var out PlaceOrderResp
	if err := c.do(ctx, http.MethodPost, "/v1/order", user, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) CancelOrder(ctx context.Context, user string, orderID uint64) (*CancelOrderResp, error) {
	var out CancelOrderResp
	path := fmt.Sprintf("/v1/order/%d", orderID)
	if err := c.do(ctx, http.MethodDelete, path, user, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) Balances(ctx context.Context, user string) (*BalancesResp, error) {
	var out BalancesResp
	if err := c.do(ctx, http.MethodGet, "/v1/account", user, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) OpenOrders(ctx context.Context, user, symbol string) (*ListOrdersResp, error) {
	q := url.Values{}
	q.Set("scope", "open")
	if symbol != "" {
		q.Set("symbol", symbol)
	}
	q.Set("limit", "50")
	var out ListOrdersResp
	if err := c.do(ctx, http.MethodGet, "/v1/orders?"+q.Encode(), user, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) RecentTrades(ctx context.Context, user, symbol string) (*ListTradesResp, error) {
	q := url.Values{}
	if symbol != "" {
		q.Set("symbol", symbol)
	}
	q.Set("limit", "20")
	var out ListTradesResp
	if err := c.do(ctx, http.MethodGet, "/v1/trades?"+q.Encode(), user, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

// WSFrame is a decoded server-to-client frame.
type WSFrame struct {
	Stream  string          // "user" / "publictrade@SYM" / control op
	Data    json.RawMessage // body for data frames
	Control string          // non-empty for control (pong/ack/error/subscribed)
	Raw     []byte          // full payload
}

// dataFrame / controlFrame shapes match push/internal/ws/protocol.go.
type dataFrame struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type controlFrame struct {
	Op      string   `json:"op"`
	Streams []string `json:"streams,omitempty"`
	Message string   `json:"message,omitempty"`
}

// DialWS opens a WS connection to wsURL (e.g. ws://localhost:8080/ws) with
// X-User-Id set to user. Returns the open conn ready for Read/Write.
func DialWS(ctx context.Context, wsURL, user string) (*websocket.Conn, error) {
	header := http.Header{}
	if user != "" {
		header.Set("X-User-Id", user)
	}
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: header,
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Subscribe sends `{op:"subscribe", streams:[...]}`.
func Subscribe(ctx context.Context, conn *websocket.Conn, streams []string) error {
	msg := controlFrame{Op: "subscribe", Streams: streams}
	b, _ := json.Marshal(msg)
	return conn.Write(ctx, websocket.MessageText, b)
}

// ReadFrame reads one frame and decodes it into WSFrame. Returns io.EOF-like
// error when the peer closes.
func ReadFrame(ctx context.Context, conn *websocket.Conn) (*WSFrame, error) {
	_, raw, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	// Try data frame first.
	var d dataFrame
	if err := json.Unmarshal(raw, &d); err == nil && d.Stream != "" {
		return &WSFrame{Stream: d.Stream, Data: d.Data, Raw: raw}, nil
	}
	// Then control.
	var c controlFrame
	if err := json.Unmarshal(raw, &c); err == nil && c.Op != "" {
		return &WSFrame{Control: c.Op, Raw: raw}, nil
	}
	return nil, errors.New("unrecognized ws frame")
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// RandID returns a short random hex id, used for client_order_id / transfer_id.
func RandID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// PrettyJSON renders v as compact one-line JSON for log display.
func PrettyJSON(v json.RawMessage) string {
	var any interface{}
	if err := json.Unmarshal(v, &any); err != nil {
		return string(v)
	}
	b, err := json.Marshal(any)
	if err != nil {
		return string(v)
	}
	return string(b)
}
