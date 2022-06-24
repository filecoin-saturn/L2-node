package libp2pcarserver

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car"

	"github.com/filecoin-project/saturn-l2/store"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"

	"github.com/ipld/go-car/v2/blockstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"

	"github.com/libp2p/go-libp2p-core/peer"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestSimpleTransfer(t *testing.T) {
	ctx := context.Background()
	mn := mocknet.New()

	p1, err := mn.GenPeer()
	require.NoError(t, err)
	p2, err := mn.GenPeer()
	require.NoError(t, err)
	cs, err := store.NewCARStore(t.TempDir())
	require.NoError(t, err)
	l := New(p2, cs)
	l.Start()

	require.NoError(t, mn.LinkAll())

	p1.Peerstore().AddAddrs(p2.ID(), p2.Addrs(), 1*time.Hour)
	require.NoError(t, p1.Connect(ctx, peer.AddrInfo{ID: p2.ID()}))

	s, err := p1.NewStream(ctx, p2.ID(), CARTransferProtocol)
	require.NoError(t, err)

	from, err := blockstore.OpenReadOnly("../testdata/files/sample-v1.car")
	require.NoError(t, err)
	rts, err := from.Roots()
	require.NoError(t, err)
	require.NoError(t, from.Close())
	// add the car file to the car store
	require.NoError(t, cs.Create(rts[0], rts[0], func(bs bstore.Blockstore) error {
		f, fErr := os.Open("../testdata/files/sample-v1.car")
		if fErr != nil {
			return err
		}
		if _, lErr := car.LoadCar(ctx, bs, f); lErr != nil {
			return lErr
		}
		return f.Close()
	}))

	rtbz := rts[0].Bytes()

	bf := bytes.Buffer{}
	require.NoError(t, dagcbor.Encode(selectorparse.CommonSelector_ExploreAllRecursively, &bf))

	req := CARTransferRequest{
		Root:     base64.StdEncoding.EncodeToString(rtbz),
		Selector: base64.StdEncoding.EncodeToString(bf.Bytes()),
	}
	reqBz, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = s.Write(reqBz)
	require.NoError(t, err)
	require.NoError(t, s.CloseWrite())

	resp, err := io.ReadAll(s)
	require.NoError(t, err)
	require.NotEmpty(t, resp)

	// ensure contents match
	fbz, err := os.ReadFile("../testdata/files/sample-v1.car")
	require.NoError(t, err)
	require.EqualValues(t, fbz, resp)
}
