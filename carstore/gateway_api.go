package carstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/saturn-l2/station"

	"github.com/filecoin-project/dagstore/mount"
	cid "github.com/ipfs/go-cid"
)

var (
	defaultURL = "https://ipfs.io/api/v0/dag/export"
	// ErrDownloadTooLarge means that the file being downloaded from the IPFS Gateway is larger than the
	// maximum size allowed.
	ErrDownloadTooLarge = errors.New("download is too large")
)

type GatewayAPI interface {
	Fetch(ctx context.Context, rootCID cid.Cid) (mount.Reader, error)
}

var _ GatewayAPI = (*gatewayAPI)(nil)

type gatewayAPI struct {
	client                *http.Client
	baseURL               string
	sApi                  station.StationAPI
	maxDownloadPerRequest uint64
}

func NewGatewayAPI(baseURL string, sApi station.StationAPI, maxDownloadPerRequest uint64) *gatewayAPI {
	client := &http.Client{
		Timeout: defaultDownloadTimeout,
	}
	return &gatewayAPI{
		maxDownloadPerRequest: maxDownloadPerRequest,
		client:                client,
		baseURL:               baseURL,
		sApi:                  sApi,
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

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute http request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http req failed: code: %d, status: '%s'", resp.StatusCode, resp.Status)
	}

	return &GatewayReader{
		ctx:                   ctx,
		ReadCloser:            resp.Body,
		sapi:                  g.sApi,
		maxDownloadPerRequest: g.maxDownloadPerRequest,
	}, nil
}

var _ mount.Reader = (*GatewayReader)(nil)

type GatewayReader struct {
	ctx context.Context

	io.ReadCloser
	io.ReaderAt
	io.Seeker
	n uint64

	sapi station.StationAPI

	maxDownloadPerRequest uint64
}

func (gw *GatewayReader) Read(p []byte) (int, error) {
	n, err := gw.ReadCloser.Read(p)
	gw.n += uint64(n)
	if gw.n >= gw.maxDownloadPerRequest {
		return n, ErrDownloadTooLarge
	}
	return n, err
}

func (gw *GatewayReader) Close() error {
	var err error
	err = gw.sapi.RecordDataDownloaded(gw.ctx, gw.n)
	if err != nil {
		log.Errorw("failed to record download stats", "err", err)
	}
	return err
}
