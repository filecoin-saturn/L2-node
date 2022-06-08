package carstore

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/dagstore/mount"
	cid "github.com/ipfs/go-cid"
)

var (
	defaultURL = "https://ipfs.io/api/v0/dag/export"
)

type GatewayAPI interface {
	Fetch(ctx context.Context, rootCID cid.Cid) (mount.Reader, error)
}

var _ GatewayAPI = (*gatewayAPI)(nil)

type gatewayAPI struct {
	baseURL string
}

func NewGatewayAPI(baseURL string) *gatewayAPI {
	return &gatewayAPI{
		baseURL: baseURL,
	}
}

func (g *gatewayAPI) Fetch(ctx context.Context, rootCID cid.Cid) (mount.Reader, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", g.baseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	q := req.URL.Query()
	q.Add("arg", rootCID.String())
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute http request")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http req failed: code: %d, status: '%s'", resp.StatusCode, resp.Status)
	}

	return &GatewayReader{
		ReadCloser: resp.Body,
	}, nil
}

var _ mount.Reader = (*GatewayReader)(nil)

type GatewayReader struct {
	io.ReadCloser
	io.ReaderAt
	io.Seeker
}
