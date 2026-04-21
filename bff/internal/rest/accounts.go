package rest

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	assetrpc "github.com/xargin/opentrade/api/gen/rpc/asset"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/pkg/auth"
)

// transferBody is the ADR-0057 request body for POST /v1/transfer. It is
// a saga initiator: the user moves `amount` of `asset` from biz_line
// `from_biz` to `to_biz`. Valid biz_line values are driven by
// asset-service's registry (MVP: "funding" / "spot"; future: "futures" /
// "earn" / "margin").
type transferBody struct {
	TransferID string `json:"transfer_id"`
	FromBiz    string `json:"from_biz"`
	ToBiz      string `json:"to_biz"`
	Asset      string `json:"asset"`
	Amount     string `json:"amount"`
	Memo       string `json:"memo,omitempty"`
}

// handleTransfer drives a cross-biz_line saga via asset-service
// (ADR-0057 M4). The call is synchronous but may return terminal=false
// if the saga hasn't reached a terminal state before the server-side
// deadline — the client can poll GET /v1/transfer/{transfer_id}.
func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if s.asset == nil {
		writeError(w, http.StatusServiceUnavailable, "asset service unavailable")
		return
	}
	var body transferBody
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := s.asset.Transfer(r.Context(), &assetrpc.TransferRequest{
		UserId:     userID,
		TransferId: body.TransferID,
		FromBiz:    body.FromBiz,
		ToBiz:      body.ToBiz,
		Asset:      body.Asset,
		Amount:     body.Amount,
		Memo:       body.Memo,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"transfer_id":   resp.TransferId,
		"state":         sagaStateToString(resp.State),
		"reject_reason": resp.RejectReason,
		"terminal":      resp.Terminal,
	})
}

// handleQueryTransfer exposes asset-service's QueryTransfer as the
// realtime polling endpoint (reads transfer_ledger directly). For
// long-term history browsing use GET /v1/transfers which reads the
// trade-dump MySQL projection via history-service (ADR-0057 M5).
func (s *Server) handleQueryTransfer(w http.ResponseWriter, r *http.Request) {
	if _, err := auth.UserID(r.Context()); err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if s.asset == nil {
		writeError(w, http.StatusServiceUnavailable, "asset service unavailable")
		return
	}
	id := r.PathValue("transfer_id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "transfer_id required")
		return
	}
	resp, err := s.asset.QueryTransfer(r.Context(), &assetrpc.QueryTransferRequest{TransferId: id})
	if err != nil {
		if se, ok := status.FromError(err); ok && se.Code() == codes.NotFound {
			writeError(w, http.StatusNotFound, se.Message())
			return
		}
		writeGRPCError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"transfer_id":        resp.TransferId,
		"user_id":            resp.UserId,
		"from_biz":           resp.FromBiz,
		"to_biz":             resp.ToBiz,
		"asset":              resp.Asset,
		"amount":             resp.Amount,
		"state":              sagaStateToString(resp.State),
		"reject_reason":      resp.RejectReason,
		"created_at_unix_ms": resp.CreatedAtUnixMs,
		"updated_at_unix_ms": resp.UpdatedAtUnixMs,
	})
}

// handleQueryFundingBalance proxies asset-service's QueryFundingBalance
// for the authenticated user. Mirrors handleQueryBalance's shape but
// sourced from the funding-wallet account book.
func (s *Server) handleQueryFundingBalance(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	if s.asset == nil {
		writeError(w, http.StatusServiceUnavailable, "asset service unavailable")
		return
	}
	asset := r.URL.Query().Get("asset")
	resp, err := s.asset.QueryFundingBalance(r.Context(), &assetrpc.QueryFundingBalanceRequest{
		UserId: userID,
		Asset:  asset,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	balances := make([]map[string]any, 0, len(resp.Balances))
	for _, b := range resp.Balances {
		balances = append(balances, map[string]any{
			"asset":     b.Asset,
			"available": b.Available,
			"frozen":    b.Frozen,
			"version":   b.Version,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"balances": balances})
}

// handleQueryBalance returns the spot-wallet balances from counter.
// Counter only has one account book (spot) today; future biz_lines will
// each own a separate read path.
func (s *Server) handleQueryBalance(w http.ResponseWriter, r *http.Request) {
	userID, err := auth.UserID(r.Context())
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	asset := r.URL.Query().Get("asset")
	resp, err := s.counter.QueryBalance(r.Context(), &counterrpc.QueryBalanceRequest{
		UserId: userID,
		Asset:  asset,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	balances := make([]map[string]string, 0, len(resp.Balances))
	for _, b := range resp.Balances {
		balances = append(balances, map[string]string{
			"asset":     b.Asset,
			"available": b.Available,
			"frozen":    b.Frozen,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"balances": balances,
	})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// sagaStateToString produces the stable wire-level string for saga
// states. Mirrors transferledger.State values so downstream consumers
// can parse the same alphabet whether they read via BFF or directly via
// the MySQL projection.
func sagaStateToString(s assetrpc.SagaState) string {
	switch s {
	case assetrpc.SagaState_SAGA_STATE_INIT:
		return "INIT"
	case assetrpc.SagaState_SAGA_STATE_DEBITED:
		return "DEBITED"
	case assetrpc.SagaState_SAGA_STATE_COMPLETED:
		return "COMPLETED"
	case assetrpc.SagaState_SAGA_STATE_FAILED:
		return "FAILED"
	case assetrpc.SagaState_SAGA_STATE_COMPENSATING:
		return "COMPENSATING"
	case assetrpc.SagaState_SAGA_STATE_COMPENSATED:
		return "COMPENSATED"
	case assetrpc.SagaState_SAGA_STATE_COMPENSATE_STUCK:
		return "COMPENSATE_STUCK"
	}
	return "UNSPECIFIED"
}

