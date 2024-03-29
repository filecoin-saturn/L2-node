package carstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/filecoin-project/saturn-l2/testutils"

	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

var (
	defaultURL = "https://ipfs.io/api/v0/dag/export" // nolint
	root       = "QmfMYyn8LUWEfRXfijKFjBAshSsPVRUgwLZzsD7kcTtX1A"
)

func TestGatewayAPI(t *testing.T) {
	ctx := context.Background()

	bz := []byte("hello")
	svc := testutils.GetTestServer(t, root, bz)
	defer svc.Close()

	gw := NewGatewayAPI(svc.URL, nil, 10000)

	c, err := cid.Decode(root)
	require.NoError(t, err)

	rd, err := gw.Fetch(ctx, c)
	require.NoError(t, err)
	require.NotEmpty(t, rd)

	out, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.EqualValues(t, bz, out)
}

func TestGatewayAPIFailure(t *testing.T) {
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer svc.Close()

	ctx := context.Background()
	gw := NewGatewayAPI(svc.URL, nil, 10000)

	c, err := cid.Decode(root)
	require.NoError(t, err)

	rd, err := gw.Fetch(ctx, c)
	require.Error(t, err)
	require.Empty(t, rd)
}

func TestIPFSGateway(t *testing.T) {
	t.Skip("e2e test with IPFS Gateway")
	ctx := context.Background()
	gw := &gatewayAPI{
		baseURL: defaultURL,
	}

	c, err := cid.Decode(root)
	require.NoError(t, err)

	rd, err := gw.Fetch(ctx, c)
	require.NoError(t, err)
	require.NotEmpty(t, rd)

	bz, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.NotEmpty(t, bz)
}

func TestDownloadFailsIfTooLarge(t *testing.T) {
	ctx := context.Background()

	bz := []byte("hello")
	svc := testutils.GetTestServer(t, root, bz)
	defer svc.Close()

	gw := NewGatewayAPI(svc.URL, nil, 1)

	c, err := cid.Decode(root)
	require.NoError(t, err)

	rd, err := gw.Fetch(ctx, c)
	require.NoError(t, err)
	require.NotEmpty(t, rd)

	_, err = io.ReadAll(rd)
	require.True(t, errors.Is(err, ErrDownloadTooLarge))
}
