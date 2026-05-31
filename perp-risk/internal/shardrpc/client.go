package shardrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perprisk"
)

type Client struct {
	http *http.Client
}

func New(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{http: &http.Client{Timeout: timeout}}
}

// Candidates asks one perp-counter shard for price-scoped ADL candidates. The
// shard is still only reporting a view; the later ADLTask carries side,
// PosSeq, and PositionVersion so the execution side rejects stale reports.
func (c *Client) Candidates(ctx context.Context, endpoint string, req perprisk.CandidateRequest) ([]perprisk.ADLCandidate, error) {
	var resp perprisk.CandidateResponse
	if err := c.post(ctx, endpoint, perprisk.RiskCandidatesPath, req, &resp); err != nil {
		return nil, err
	}
	out := make([]perprisk.ADLCandidate, 0, len(resp.Candidates))
	for _, wire := range resp.Candidates {
		cand, err := perprisk.CandidateFromWire(wire)
		if err != nil {
			return nil, err
		}
		out = append(out, cand)
	}
	return out, nil
}

// ExecuteTask submits the version-stamped task to the shard endpoint that
// reported the candidate. A false response is not a transport failure; it means
// the shard's sequencer rejected the task, usually because one observed
// position stamp was stale.
func (c *Client) ExecuteTask(ctx context.Context, endpoint string, task perprisk.ADLTask) (perprisk.ADLTaskResult, error) {
	var resp perprisk.ADLTaskResponse
	if err := c.post(ctx, endpoint, perprisk.RiskTaskPath, perprisk.TaskToWire(task), &resp); err != nil {
		return perprisk.ADLTaskResult{}, err
	}
	factQty, err := dec.Parse(resp.FactQty)
	if err != nil {
		return perprisk.ADLTaskResult{}, err
	}
	realized, err := dec.Parse(resp.RealizedPnL)
	if err != nil {
		return perprisk.ADLTaskResult{}, err
	}
	return perprisk.ADLTaskResult{Applied: resp.Applied, FactQty: factQty, RealizedPnL: realized}, nil
}

func (c *Client) post(ctx context.Context, endpoint, path string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	url := strings.TrimRight(endpoint, "/") + path
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("content-type", "application/json")
	httpResp, err := c.http.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", url, httpResp.Status)
	}
	return json.NewDecoder(httpResp.Body).Decode(resp)
}
