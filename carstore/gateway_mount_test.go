package carstore

import (
	"context"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/filecoin-project/saturn-l2/station"

	"github.com/filecoin-project/saturn-l2/testutils"

	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestGatewayMount(t *testing.T) {
	bz := []byte("hello")

	svc := testutils.GetTestServer(t, root, bz)
	defer svc.Close()

	gwAPI := NewGatewayAPI(svc.URL, &mockStationAPI{}, 10000)
	ctx := context.Background()
	c, err := cid.Decode(root)
	require.NoError(t, err)
	gm := &GatewayMount{
		RootCID: c,
		API:     gwAPI,
	}

	u := gm.Serialize()
	require.NotEmpty(t, u)

	gm2 := &GatewayMount{
		API: gwAPI,
	}
	require.NoError(t, gm2.Deserialize(u))

	rd, err := gm2.Fetch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, rd)
	out, err := ioutil.ReadAll(rd)
	require.NoError(t, err)
	require.NotEmpty(t, out)
	require.EqualValues(t, out, bz)
}

type mockStationAPI struct {
	station.StationAPI

	mu         sync.Mutex
	downloaded uint64
}

func (m *mockStationAPI) RecordDataDownloaded(_ context.Context, bytesDownloaded uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.downloaded += bytesDownloaded
	return nil
}

func (m *mockStationAPI) nDownloaded() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.downloaded
}
