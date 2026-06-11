package server

// perp_ops.go is the ADR-0078 admin plane over perp-counter: §7
// ForceAdjustPosition and §8 BlockTrade. Both audit BEFORE the HTTP
// response is written (mirrors the catalog handlers) and carry the
// mandatory reason/ticket/operator triple; the operator defaults to the
// authenticated admin id when omitted.

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"connectrpc.com/connect"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/auth"
	"github.com/xargin/opentrade/pkg/shard"
)

// parseSideOptional maps an optional "buy"/"sell" body field; empty stays
// UNSPECIFIED (SUB derives the side from the leg).
func parseSideOptional(v string) (eventpb.Side, error) {
	switch v {
	case "":
		return eventpb.Side_SIDE_UNSPECIFIED, nil
	case "buy":
		return eventpb.Side_SIDE_BUY, nil
	case "sell":
		return eventpb.Side_SIDE_SELL, nil
	default:
		return eventpb.Side_SIDE_UNSPECIFIED, errors.New("side must be buy or sell")
	}
}

// PerpAdminOps is the slice of the perp-counter RPC surface the ADR-0078
// admin ops call. The generated perprpcconnect.PerpServiceClient satisfies
// it (same clients Config.PerpCounters carries for the dry-run).
type PerpAdminOps interface {
	ForceAdjustPosition(ctx context.Context, req *connect.Request[perprpc.ForceAdjustPositionRequest]) (*connect.Response[perprpc.ForceAdjustPositionResponse], error)
	BlockTrade(ctx context.Context, req *connect.Request[perprpc.BlockTradeRequest]) (*connect.Response[perprpc.BlockTradeResponse], error)
}

// perpOpsRoutes mounts the ADR-0078 admin endpoints.
func (s *Server) perpOpsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /admin/perp/positions/force-adjust", s.handlePerpForceAdjust)
	mux.HandleFunc("POST /admin/perp/block-trade", s.handlePerpBlockTrade)
}

// perpAdminFor routes a user-scoped admin op to its perp-counter shard
// (xxhash by user id — the same routing every other caller uses). nil when
// no perp counters are configured.
func (s *Server) perpAdminFor(userID uint64) PerpAdminOps {
	if len(s.perpAdmin) == 0 {
		return nil
	}
	return s.perpAdmin[shard.Index(userID, len(s.perpAdmin))]
}

// operatorOr resolves the audit operator: an explicit body value wins,
// otherwise the authenticated admin id.
func operatorOr(r *http.Request, explicit string) string {
	if explicit != "" {
		return explicit
	}
	if uid, err := auth.UserID(r.Context()); err == nil {
		return strconv.FormatUint(uid, 10)
	}
	return ""
}

type forceAdjustBody struct {
	UserID      uint64 `json:"user_id"`
	Symbol      string `json:"symbol"`
	PositionIdx uint32 `json:"position_idx,omitempty"`
	Sub         bool   `json:"sub"`            // false = ADD, true = SUB
	Side        string `json:"side,omitempty"` // ADD: "buy"/"sell" (flat leg defines side)
	Qty         string `json:"qty"`
	Price       string `json:"price"`
	Reason      string `json:"reason"`
	Ticket      string `json:"ticket"`
	Operator    string `json:"operator,omitempty"` // default: authenticated admin id
	ClientOpID  string `json:"client_op_id"`
}

func (s *Server) handlePerpForceAdjust(w http.ResponseWriter, r *http.Request) {
	var body forceAdjustBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cli := s.perpAdminFor(body.UserID)
	if cli == nil {
		writeError(w, http.StatusServiceUnavailable, "perp-counter not configured")
		return
	}
	side, err := parseSideOptional(body.Side)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	operator := operatorOr(r, body.Operator)
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	resp, rpcErr := cli.ForceAdjustPosition(ctx, connect.NewRequest(&perprpc.ForceAdjustPositionRequest{
		UserId: body.UserID, Symbol: body.Symbol, PositionIdx: body.PositionIdx,
		Sub: body.Sub, Side: side, Qty: body.Qty, Price: body.Price,
		Reason: body.Reason, Ticket: body.Ticket, Operator: operator,
		ClientOpId: body.ClientOpID,
	}))

	op := "admin.perp.force_adjust.add"
	if body.Sub {
		op = "admin.perp.force_adjust.sub"
	}
	entry := adminaudit.Entry{
		Op:     op,
		Target: body.Symbol,
		Params: map[string]any{
			"user_id": body.UserID, "position_idx": body.PositionIdx,
			"qty": body.Qty, "price": body.Price,
			"reason": body.Reason, "ticket": body.Ticket, "operator": operator,
			"client_op_id": body.ClientOpID,
		},
		Status: statusFromErr(rpcErr),
		Error:  errString(rpcErr),
	}
	if resp != nil && !resp.Msg.Accepted {
		entry.Status = "rejected"
		entry.Error = resp.Msg.RejectReason
	}
	if err := s.writeAudit(r, entry); err != nil {
		writeError(w, http.StatusInternalServerError, "audit write failed: "+err.Error())
		return
	}
	if rpcErr != nil {
		writeError(w, http.StatusBadGateway, rpcErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":           resp.Msg.Accepted,
		"reject_reason":      resp.Msg.RejectReason,
		"position_size":      resp.Msg.PositionSize,
		"entry_price":        resp.Msg.EntryPrice,
		"margin":             resp.Msg.Margin,
		"realized_pnl":       resp.Msg.RealizedPnl,
		"free_balance_after": resp.Msg.FreeBalanceAfter,
	})
}

type blockTradeLegBody struct {
	UserID      uint64 `json:"user_id"`
	PositionIdx uint32 `json:"position_idx,omitempty"`
	ReduceOnly  bool   `json:"reduce_only,omitempty"`
}

type blockTradeBody struct {
	BlockTradeID string            `json:"block_trade_id"`
	Symbol       string            `json:"symbol"`
	Price        string            `json:"price"`
	Qty          string            `json:"qty"`
	Buyer        blockTradeLegBody `json:"buyer"`
	Seller       blockTradeLegBody `json:"seller"`
	Reason       string            `json:"reason"`
	Ticket       string            `json:"ticket"`
	Operator     string            `json:"operator,omitempty"`
}

func (s *Server) handlePerpBlockTrade(w http.ResponseWriter, r *http.Request) {
	var body blockTradeBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(s.perpAdmin) == 0 {
		writeError(w, http.StatusServiceUnavailable, "perp-counter not configured")
		return
	}
	// V1 block trades execute inside ONE perp-counter process (ADR-0078
	// 修订 #9); with multiple shards both legs must land on the same one —
	// the cross-shard coordinator arrives with real sharding.
	buyerShard := shard.Index(body.Buyer.UserID, len(s.perpAdmin))
	sellerShard := shard.Index(body.Seller.UserID, len(s.perpAdmin))
	if buyerShard != sellerShard {
		writeError(w, http.StatusBadRequest, "cross-shard block trade unsupported (ADR-0078 修订 #9)")
		return
	}
	operator := operatorOr(r, body.Operator)
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()
	resp, rpcErr := s.perpAdmin[buyerShard].BlockTrade(ctx, connect.NewRequest(&perprpc.BlockTradeRequest{
		BlockTradeId: body.BlockTradeID, Symbol: body.Symbol,
		Price: body.Price, Qty: body.Qty,
		Buyer:  &perprpc.BlockTradeLeg{UserId: body.Buyer.UserID, PositionIdx: body.Buyer.PositionIdx, ReduceOnly: body.Buyer.ReduceOnly},
		Seller: &perprpc.BlockTradeLeg{UserId: body.Seller.UserID, PositionIdx: body.Seller.PositionIdx, ReduceOnly: body.Seller.ReduceOnly},
		Reason: body.Reason, Ticket: body.Ticket, Operator: operator,
	}))

	entry := adminaudit.Entry{
		Op:     "admin.perp.block_trade",
		Target: body.Symbol,
		Params: map[string]any{
			"block_trade_id": body.BlockTradeID,
			"price":          body.Price, "qty": body.Qty,
			"buyer_user_id": body.Buyer.UserID, "seller_user_id": body.Seller.UserID,
			"buyer_reduce_only": body.Buyer.ReduceOnly, "seller_reduce_only": body.Seller.ReduceOnly,
			"reason": body.Reason, "ticket": body.Ticket, "operator": operator,
		},
		Status: statusFromErr(rpcErr),
		Error:  errString(rpcErr),
	}
	if resp != nil && !resp.Msg.Accepted {
		entry.Status = "rejected"
		entry.Error = resp.Msg.RejectReason
	}
	if err := s.writeAudit(r, entry); err != nil {
		writeError(w, http.StatusInternalServerError, "audit write failed: "+err.Error())
		return
	}
	if rpcErr != nil {
		writeError(w, http.StatusBadGateway, rpcErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":      resp.Msg.Accepted,
		"reject_reason": resp.Msg.RejectReason,
		"trade_id":      resp.Msg.TradeId,
	})
}
