package carserver

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-project/saturn-l2/logs"

	cid "github.com/ipfs/go-cid"

	"github.com/libp2p/go-libp2p-core/host"

	"github.com/filecoin-project/saturn-l2/testutils"

	p2phttp "github.com/libp2p/go-libp2p-http"

	"github.com/google/uuid"

	"github.com/filecoin-project/saturn-l2/carstore"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"

	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"

	"github.com/libp2p/go-libp2p-core/peer"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"github.com/stretchr/testify/require"
)

func TestSimpleTransfer(t *testing.T) {
	path := "../testdata/files/sample-v1.car"
	temp := t.TempDir()
	ctx := context.Background()

	// create the getway api with a test http server
	root, contents, svc := testutils.GetTestServerFor(t, path)
	defer svc.Close()
	gwAPI := carstore.NewGatewayAPI(svc.URL)
	lg := logs.NewSaturnLogger()
	cfg := carstore.Config{}
	cs, err := carstore.New(temp, gwAPI, cfg, lg)
	require.NoError(t, err)
	require.NoError(t, cs.Start(ctx))

	// create a mock libp2p network, two peers and a connection between them
	p1, p2 := buildPeers(t, ctx)

	// create and start the car server
	carserver := New(p2, cs, lg)
	require.NoError(t, carserver.Start(ctx))

	// send the request
	client := libp2pHTTPClient(p1)
	reqBz := mkRequest(t, root, 0)
	u := fmt.Sprintf("libp2p://%s", p2.ID())
	resp := sendHttpReq(t, client, u, reqBz)

	require.EqualValues(t, http.StatusNotFound, resp.StatusCode)

	// second fetch should not work
	resp = sendHttpReq(t, client, u, reqBz)
	require.EqualValues(t, http.StatusNotFound, resp.StatusCode)

	// wait till L2 has cached the data
	require.Eventually(t, func() bool {
		has, err := cs.IsIndexed(ctx, root)
		return has && err == nil
	}, 1*time.Second, 100*time.Millisecond)

	// third fetch should work
	resp = sendHttpReq(t, client, u, reqBz)
	require.EqualValues(t, http.StatusOK, resp.StatusCode)

	bz := readHTTPResponse(t, resp)
	// ensure contents match
	require.EqualValues(t, contents, bz)

	// send request with the skip param
	reqBz = mkRequest(t, root, 101)
	resp = sendHttpReq(t, client, u, reqBz)
	require.EqualValues(t, http.StatusOK, resp.StatusCode)

	bz = readHTTPResponse(t, resp)
	require.EqualValues(t, contents[101:], bz)
}

// TODO -> Test Parallel Transfers

func readHTTPResponse(t *testing.T, resp *http.Response) []byte {
	bz, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, resp)
	require.NoError(t, resp.Body.Close())
	return bz
}

func mkRequest(t *testing.T, root cid.Cid, offset uint64) []byte {
	bf := bytes.Buffer{}
	require.NoError(t, dagcbor.Encode(selectorparse.CommonSelector_ExploreAllRecursively, &bf))
	req := CARTransferRequest{
		Root:       base64.StdEncoding.EncodeToString(root.Bytes()),
		Selector:   base64.StdEncoding.EncodeToString(bf.Bytes()),
		ReqId:      uuid.New().String(),
		SkipOffset: offset,
	}
	reqBz, err := json.Marshal(req)
	require.NoError(t, err)
	return reqBz
}

func buildPeers(t *testing.T, ctx context.Context) (client host.Host, server host.Host) {
	mn := mocknet.New()
	p1, err := mn.GenPeer()
	require.NoError(t, err)
	p2, err := mn.GenPeer()
	require.NoError(t, err)
	require.NoError(t, mn.LinkAll())
	p1.Peerstore().AddAddrs(p2.ID(), p2.Addrs(), 1*time.Hour)
	require.NoError(t, p1.Connect(ctx, peer.AddrInfo{ID: p2.ID()}))

	return p1, p2
}

func libp2pHTTPClient(host host.Host) *http.Client {
	tr := &http.Transport{}
	p2ptr := p2phttp.NewTransport(host, p2phttp.ProtocolOption(CARTransferProtocol))
	tr.RegisterProtocol("libp2p", p2ptr)
	return &http.Client{Transport: tr}
}

func sendHttpReq(t *testing.T, client *http.Client, url string, body []byte) *http.Response {
	hreq, err := http.NewRequest("GET", url, bytes.NewReader(body))
	require.NoError(t, err)
	resp, err := client.Do(hreq)
	require.NoError(t, err)
	require.NotEmpty(t, resp)
	return resp
}
