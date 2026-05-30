package main

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"

	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/api/gen/rpc/assetholder/assetholderrpcconnect"
	"github.com/xargin/opentrade/pkg/connectx"
)

// faucet.go credits dev balances directly through the AssetHolder contract
// (ADR-0057) — the same uniform interface the transfer saga uses to move
// money between biz_lines. There is no public deposit REST endpoint (a saga
// only *moves* funds between wallets), so to make trading work end-to-end we
// seed a wallet's balance here:
//
//   - target "spot"    → Counter's AssetHolder (:8081): credits the tradeable
//                         spot balance directly, so an order can be placed
//                         immediately.
//   - target "funding" → asset-service's AssetHolder (:19000): credits the
//                         funding wallet, so the funding→spot transfer saga
//                         can then be exercised from the UI.
//
// peer_biz is informational only (the holder records but doesn't validate it),
// so a synthetic "faucet" counterparty is fine.

type faucet struct {
	spot    assetholderrpcconnect.AssetHolderClient // counter
	funding assetholderrpcconnect.AssetHolderClient // asset-service
}

func newFaucet() *faucet {
	hc := connectx.NewH2CClient()
	return &faucet{
		spot:    assetholderrpcconnect.NewAssetHolderClient(hc, connectx.BaseURL(fmt.Sprintf("localhost:%d", portCounterGRPC))),
		funding: assetholderrpcconnect.NewAssetHolderClient(hc, connectx.BaseURL(fmt.Sprintf("localhost:%d", portAssetGRPC))),
	}
}

// credit deposits amount of asset into the user's target wallet and returns
// the post-credit available balance. transferID makes the deposit idempotent;
// callers pass a fresh id per click so each click is a new deposit.
//
// Counter can briefly answer FailedPrecondition right after boot (the
// coordinator hasn't finished assigning the user's vshard to this node yet),
// and a just-launched holder can answer Unavailable; both are retried.
func (f *faucet) credit(ctx context.Context, target, user, transferID, asset, amount string) (string, error) {
	var client assetholderrpcconnect.AssetHolderClient
	peer := "faucet"
	switch target {
	case "spot", "":
		client = f.spot
	case "funding":
		client = f.funding
		peer = "external-deposit"
	default:
		return "", fmt.Errorf("unknown faucet target %q (want spot|funding)", target)
	}

	req := &assetholderrpc.TransferInRequest{
		UserId:     user,
		TransferId: transferID,
		Asset:      asset,
		Amount:     amount,
		PeerBiz:    peer,
		Memo:       "dev faucet",
	}

	var lastErr error
	for attempt := 0; attempt < 12; attempt++ {
		resp, err := client.TransferIn(ctx, connect.NewRequest(req))
		if err == nil {
			switch resp.Msg.Status {
			case assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED:
				return "", fmt.Errorf("rejected: %s", resp.Msg.RejectReason.String())
			default: // CONFIRMED or DUPLICATED
				return resp.Msg.AvailableAfter, nil
			}
		}
		switch connect.CodeOf(err) {
		case connect.CodeFailedPrecondition, connect.CodeUnavailable:
			lastErr = err
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 400 * time.Millisecond):
			}
			continue
		default:
			return "", err
		}
	}
	return "", fmt.Errorf("faucet credit not ready after retries: %w", lastErr)
}
