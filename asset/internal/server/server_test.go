package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	assetrpc "github.com/xargin/opentrade/api/gen/rpc/asset"
	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/asset/internal/store"
)

type fakeFundingStore struct {
	state *engine.State
}

func newFakeFundingStore() *fakeFundingStore {
	return &fakeFundingStore{state: engine.NewState()}
}

func (f *fakeFundingStore) TransferOut(_ context.Context, req store.Request) (store.Result, error) {
	res, err := f.state.ApplyTransferOut(toEngineReq(req))
	if err != nil {
		return store.Result{Status: store.StatusRejected, RejectReason: err}, nil
	}
	return fromEngineResult(res), nil
}

func (f *fakeFundingStore) TransferIn(_ context.Context, req store.Request) (store.Result, error) {
	res, err := f.state.ApplyTransferIn(toEngineReq(req))
	if err != nil {
		return store.Result{Status: store.StatusRejected, RejectReason: err}, nil
	}
	return fromEngineResult(res), nil
}

func (f *fakeFundingStore) Compensate(ctx context.Context, req store.Request) (store.Result, error) {
	return f.TransferIn(ctx, req)
}

func (f *fakeFundingStore) QueryFundingBalance(_ context.Context, userID, asset string) ([]store.FundingBalance, error) {
	acc := f.state.Account(userID)
	if asset != "" {
		return []store.FundingBalance{{Asset: asset, Balance: acc.Balance(asset)}}, nil
	}
	all := acc.Copy()
	out := make([]store.FundingBalance, 0, len(all))
	for a, b := range all {
		out = append(out, store.FundingBalance{Asset: a, Balance: b})
	}
	return out, nil
}

func toEngineReq(req store.Request) engine.TransferRequest {
	return engine.TransferRequest{
		UserID:     req.UserID,
		TransferID: req.TransferID,
		Asset:      req.Asset,
		Amount:     req.Amount,
	}
}

func fromEngineResult(res engine.Result) store.Result {
	status := store.StatusConfirmed
	if res.Duplicated {
		status = store.StatusDuplicated
	}
	return store.Result{
		Status:         status,
		BalanceAfter:   res.BalanceAfter,
		FundingVersion: res.FundingVersion,
	}
}

func newServers(t *testing.T) (*AssetHolderServer, *AssetServer) {
	t.Helper()
	svc := service.New(newFakeFundingStore(), nil)
	return NewAssetHolderServer(svc), NewAssetServer(svc, nil)
}

func TestHolder_TransferIn_Confirmed(t *testing.T) {
	h, _ := newServers(t)
	resp, err := h.TransferIn(context.Background(), &assetholderrpc.TransferInRequest{
		UserId: "u1", TransferId: "t1", Asset: "USDT", Amount: "100", PeerBiz: "spot",
	})
	if err != nil {
		t.Fatalf("in: %v", err)
	}
	if resp.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Errorf("status = %v", resp.Status)
	}
	if resp.AvailableAfter != "100" {
		t.Errorf("available_after = %q", resp.AvailableAfter)
	}
}

func TestHolder_TransferOut_InsufficientBalance(t *testing.T) {
	h, _ := newServers(t)
	resp, err := h.TransferOut(context.Background(), &assetholderrpc.TransferOutRequest{
		UserId: "u1", TransferId: "t1", Asset: "USDT", Amount: "50", PeerBiz: "spot",
	})
	if err != nil {
		t.Fatalf("out: %v", err)
	}
	if resp.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED {
		t.Fatalf("status = %v", resp.Status)
	}
	if resp.RejectReason != assetholderrpc.RejectReason_REJECT_REASON_INSUFFICIENT_BALANCE {
		t.Errorf("reject = %v", resp.RejectReason)
	}
}

func TestHolder_Idempotent(t *testing.T) {
	h, _ := newServers(t)
	req := &assetholderrpc.TransferInRequest{
		UserId: "u1", TransferId: "t1", Asset: "USDT", Amount: "40", PeerBiz: "spot",
	}
	first, err := h.TransferIn(context.Background(), req)
	if err != nil || first.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("first: %v / %v", err, first)
	}
	second, err := h.TransferIn(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED {
		t.Errorf("status = %v, want DUPLICATED", second.Status)
	}
}

func TestHolder_Compensate(t *testing.T) {
	h, _ := newServers(t)
	resp, err := h.CompensateTransferOut(context.Background(), &assetholderrpc.CompensateTransferOutRequest{
		UserId: "u1", TransferId: "t-comp", Asset: "USDT", Amount: "20",
		PeerBiz: "spot", CompensateCause: "peer_in_timeout",
	})
	if err != nil {
		t.Fatalf("compensate: %v", err)
	}
	if resp.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Errorf("status = %v", resp.Status)
	}
}

func TestHolder_InvalidArgument(t *testing.T) {
	h, _ := newServers(t)
	bad := []struct {
		name string
		req  *assetholderrpc.TransferInRequest
	}{
		{"nil_user", &assetholderrpc.TransferInRequest{TransferId: "t", Asset: "USDT", Amount: "1"}},
		{"nil_tx", &assetholderrpc.TransferInRequest{UserId: "u1", Asset: "USDT", Amount: "1"}},
		{"nil_asset", &assetholderrpc.TransferInRequest{UserId: "u1", TransferId: "t", Amount: "1"}},
		{"bad_amount", &assetholderrpc.TransferInRequest{UserId: "u1", TransferId: "t", Asset: "USDT", Amount: "abc"}},
		{"zero_amount", &assetholderrpc.TransferInRequest{UserId: "u1", TransferId: "t", Asset: "USDT", Amount: "0"}},
		{"neg_amount", &assetholderrpc.TransferInRequest{UserId: "u1", TransferId: "t", Asset: "USDT", Amount: "-5"}},
	}
	for _, b := range bad {
		_, err := h.TransferIn(context.Background(), b.req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: code = %v, want InvalidArgument", b.name, status.Code(err))
		}
	}
}

func TestHolder_NilRequest(t *testing.T) {
	h, _ := newServers(t)
	_, err := h.TransferIn(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("in nil: code = %v", status.Code(err))
	}
	_, err = h.TransferOut(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("out nil: code = %v", status.Code(err))
	}
	_, err = h.CompensateTransferOut(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("compensate nil: code = %v", status.Code(err))
	}
}

// -----------------------------------------------------------------------------
// AssetService.QueryFundingBalance
// -----------------------------------------------------------------------------

func TestAsset_QueryFundingBalance_All(t *testing.T) {
	h, a := newServers(t)
	ctx := context.Background()
	if _, err := h.TransferIn(ctx, &assetholderrpc.TransferInRequest{
		UserId: "u1", TransferId: "t-usdt", Asset: "USDT", Amount: "100", PeerBiz: "spot",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.TransferIn(ctx, &assetholderrpc.TransferInRequest{
		UserId: "u1", TransferId: "t-btc", Asset: "BTC", Amount: "0.5", PeerBiz: "spot",
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := a.QueryFundingBalance(ctx, &assetrpc.QueryFundingBalanceRequest{UserId: "u1"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Balances) != 2 {
		t.Errorf("balances = %d, want 2", len(resp.Balances))
	}
}

func TestAsset_QueryFundingBalance_Single(t *testing.T) {
	h, a := newServers(t)
	ctx := context.Background()
	if _, err := h.TransferIn(ctx, &assetholderrpc.TransferInRequest{
		UserId: "u1", TransferId: "t-usdt", Asset: "USDT", Amount: "75", PeerBiz: "spot",
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := a.QueryFundingBalance(ctx, &assetrpc.QueryFundingBalanceRequest{
		UserId: "u1", Asset: "USDT",
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.Balances) != 1 || resp.Balances[0].Available != "75" {
		t.Errorf("balances = %+v", resp.Balances)
	}
}

func TestAsset_QueryFundingBalance_MissingUser(t *testing.T) {
	_, a := newServers(t)
	_, err := a.QueryFundingBalance(context.Background(), &assetrpc.QueryFundingBalanceRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}
