package index

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/coder/websocket"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/dec"
)

const reconnectDelay = 5 * time.Second

// RunExternalSources starts best-effort public market-data adapters for
// non-self sources. The composite algorithm owns safety through max-age and
// quorum, so an adapter may fail or reconnect without mutating any global
// health state beyond its latest quote aging out.
func RunExternalSources(ctx context.Context, book *SourceBook, sources []SourceConfig, logger *zap.Logger) {
	for _, src := range sources {
		if src.Self {
			continue
		}
		venue, symbol, ok := splitSourceName(src.Name)
		if !ok {
			logger.Warn("index source name must be venue:symbol", zap.String("source", src.Name))
			continue
		}
		switch venue {
		case "binance":
			go runReconnecting(ctx, src.Name, logger, func(ctx context.Context) error {
				return runBinance(ctx, book, src.Name, symbol)
			})
		case "okx":
			go runReconnecting(ctx, src.Name, logger, func(ctx context.Context) error {
				return runOKX(ctx, book, src.Name, symbol)
			})
		case "bybit":
			go runReconnecting(ctx, src.Name, logger, func(ctx context.Context) error {
				return runBybit(ctx, book, src.Name, symbol)
			})
		default:
			logger.Warn("unsupported external index source venue",
				zap.String("source", src.Name), zap.String("venue", venue))
		}
	}
}

func runReconnecting(ctx context.Context, name string, logger *zap.Logger, run func(context.Context) error) {
	for ctx.Err() == nil {
		if err := run(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("index source disconnected; reconnecting",
				zap.String("source", name), zap.Error(err), zap.Duration("delay", reconnectDelay))
		}
		timer := time.NewTimer(reconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func splitSourceName(name string) (venue, symbol string, ok bool) {
	parts := strings.SplitN(name, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return strings.ToLower(parts[0]), parts[1], true
}

func runBinance(ctx context.Context, book *SourceBook, name, symbol string) error {
	// Binance raw streams require lowercase symbols and expose best bid/ask on
	// <symbol>@bookTicker. This adapter computes a mid so the composite layer
	// remains exchange-agnostic.
	url := "wss://stream.binance.com:9443/ws/" + strings.ToLower(symbol) + "@bookTicker"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if string(payload) == "ping" {
			_ = conn.Write(ctx, websocket.MessageText, []byte("pong"))
			continue
		}
		var msg struct {
			Bid string `json:"b"`
			Ask string `json:"a"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		upsertMid(book, name, msg.Bid, msg.Ask, time.Now().UnixMilli())
	}
}

func runOKX(ctx context.Context, book *SourceBook, name, symbol string) error {
	conn, _, err := websocket.Dial(ctx, "wss://ws.okx.com:8443/ws/v5/public", nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")
	sub := map[string]any{
		"op":   "subscribe",
		"args": []map[string]string{{"channel": "tickers", "instId": symbol}},
	}
	body, _ := json.Marshal(sub)
	if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
		return err
	}
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var msg struct {
			Data []struct {
				Bid string `json:"bidPx"`
				Ask string `json:"askPx"`
				Ts  string `json:"ts"`
			} `json:"data"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil || len(msg.Data) == 0 {
			continue
		}
		ts := parseMs(msg.Data[0].Ts)
		upsertMid(book, name, msg.Data[0].Bid, msg.Data[0].Ask, ts)
	}
}

func runBybit(ctx context.Context, book *SourceBook, name, symbol string) error {
	conn, _, err := websocket.Dial(ctx, "wss://stream.bybit.com/v5/public/linear", nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")
	// Bybit v5 linear public streams use explicit subscribe messages. Tickers
	// provide best bid/ask without maintaining a depth book locally, which is
	// enough for the composite index's exchange-agnostic mid-price input.
	sub := map[string]any{"op": "subscribe", "args": []string{"tickers." + symbol}}
	body, _ := json.Marshal(sub)
	if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
		return err
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go bybitHeartbeat(heartbeatCtx, conn)

	var lastBid, lastAsk string
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var msg struct {
			Topic string `json:"topic"`
			Ts    int64  `json:"ts"`
			Data  struct {
				Bid string `json:"bid1Price"`
				Ask string `json:"ask1Price"`
			} `json:"data"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil || msg.Topic != "tickers."+symbol {
			continue
		}
		// The ticker stream can send delta messages that omit unchanged fields.
		// Carry the last non-empty side so a one-sided delta still refreshes the
		// mid once both sides have been observed.
		if msg.Data.Bid != "" {
			lastBid = msg.Data.Bid
		}
		if msg.Data.Ask != "" {
			lastAsk = msg.Data.Ask
		}
		upsertMid(book, name, lastBid, lastAsk, msg.Ts)
	}
}

func bybitHeartbeat(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			body, _ := json.Marshal(map[string]string{"op": "ping"})
			if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
				return
			}
		}
	}
}

func upsertMid(book *SourceBook, name, bidRaw, askRaw string, tsMs int64) {
	bid, errBid := dec.Parse(bidRaw)
	ask, errAsk := dec.Parse(askRaw)
	if errBid != nil || errAsk != nil || bid.Sign() <= 0 || ask.Sign() <= 0 {
		return
	}
	if tsMs <= 0 {
		tsMs = time.Now().UnixMilli()
	}
	book.Upsert(name, bid.Add(ask).Div(dec.FromInt(2)), tsMs)
}

func parseMs(s string) int64 {
	if s == "" {
		return time.Now().UnixMilli()
	}
	var v int64
	if _, err := fmt.Sscan(s, &v); err != nil || v <= 0 {
		return time.Now().UnixMilli()
	}
	return v
}
