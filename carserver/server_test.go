package carserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/filecoin-project/saturn-l2/types"

	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/saturn-l2/station"

	datastore "github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"

	"github.com/filecoin-project/saturn-l2/logs"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"

	cid "github.com/ipfs/go-cid"

	"github.com/filecoin-project/saturn-l2/testutils"

	"github.com/google/uuid"

	"github.com/filecoin-project/saturn-l2/carstore"

	"github.com/stretchr/testify/require"
)

func TestSimpleTransfer(t *testing.T) {
	ctx := context.Background()
	csh := buildHarness(t, ctx)
	defer csh.Stop(t)

	csh.assertStationStats(t, ctx, 0, 0, 0, 0, 0, 0, 0)

	url := csh.carserver.URL
	root := csh.root1
	contents := csh.bz1

	// send the request
	reqBz := mkRequestWithoutSelector(t, root, 0)
	resp := sendHttpReq(t, url, reqBz)

	require.EqualValues(t, http.StatusNotFound, resp.StatusCode)

	// second fetch should not work
	resp = sendHttpReq(t, url, reqBz)
	require.EqualValues(t, http.StatusNotFound, resp.StatusCode)

	// wait till L2 has cached the data
	require.Eventually(t, func() bool {
		has, err := csh.store.IsIndexed(ctx, root)
		return has && err == nil
	}, 1*time.Second, 100*time.Millisecond)

	// third fetch should work
	resp = sendHttpReq(t, url, reqBz)
	require.EqualValues(t, http.StatusOK, resp.StatusCode)

	bz := readHTTPResponse(t, resp)
	// ensure contents match
	require.EqualValues(t, contents, bz)

	csh.assertStationStats(t, ctx, len(contents), len(contents), 3, 0, len(contents), 2, 1)

	// send request with the skip param
	reqBz = mkRequestWithoutSelector(t, root, 101)
	resp = sendHttpReq(t, url, reqBz)
	require.EqualValues(t, http.StatusOK, resp.StatusCode)

	bz = readHTTPResponse(t, resp)
	require.EqualValues(t, contents[101:], bz)

	csh.assertStationStats(t, ctx, len(contents)+len(contents)-101, len(contents), 4, 0, len(contents), 2, 2)
}

func TestParallelTransfers(t *testing.T) {
	ctx := context.Background()
	csh := buildHarness(t, ctx)
	defer csh.Stop(t)

	csh.assertStationStats(t, ctx, 0, 0, 0, 0, 0, 0, 0)

	url := csh.carserver.URL
	root1 := csh.root1
	root2 := csh.root2
	contents1 := csh.bz1
	contents2 := csh.bz2

	count := 0

	// send the requests so both get cached
	require.Eventually(t, func() bool {
		count++
		reqBz := mkRequestWithoutSelector(t, root1, 0)
		resp := sendHttpReq(t, url, reqBz)
		if resp.StatusCode == http.StatusOK {
			bz := readHTTPResponse(t, resp)
			return bytes.Equal(contents1, bz)
		}
		return false
	}, 5*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		count++
		reqBz := mkRequestWithoutSelector(t, root2, 0)
		resp := sendHttpReq(t, url, reqBz)
		if resp.StatusCode == http.StatusOK {
			bz := readHTTPResponse(t, resp)
			return bytes.Equal(contents2, bz)
		}

		return false
	}, 5*time.Second, 100*time.Millisecond)

	l := len(contents1) + len(contents2)
	csh.assertStationStats(t, ctx, l, l, count, 0, l, count-2, 2)

	var errg errgroup.Group

	// fetch 10 in parallel
	for i := 0; i < 10; i++ {
		i := i
		errg.Go(func() error {
			var root cid.Cid

			if i%2 == 0 {
				root = root2
			} else {
				root = root1
			}

			reqBz := mkRequestWithoutSelector(t, root, 0)
			resp := sendHttpReq(t, url, reqBz)
			if resp.StatusCode != http.StatusOK {
				return errors.New("failed")
			}
			return nil
		})

	}
	require.NoError(t, errg.Wait())

	time.Sleep(1 * time.Second)

	csh.assertStationStats(t, ctx, 6*l, l, count+10, 0, l, count-2, 12)
}

type carServerHarness struct {
	store     *carstore.CarStore
	gwapi     *httptest.Server
	carserver *httptest.Server
	sapi      station.StationAPI
	root1     cid.Cid
	bz1       []byte
	root2     cid.Cid
	bz2       []byte
}

func (csh *carServerHarness) assertStationStats(t *testing.T, ctx context.Context, upload, download, reqs, errors, storage int, nNotFound int,
	nSuccess int) {
	as, err := csh.sapi.AllStats(ctx)
	require.NoError(t, err)
	require.EqualValues(t, upload, as.TotalBytesUploaded)
	require.EqualValues(t, reqs, as.NContentRequests)
	require.EqualValues(t, errors, as.NContentReqErrors)
	require.EqualValues(t, download, as.TotalBytesDownloaded)
	require.EqualValues(t, storage, as.StorageStats.BytesCurrentlyStored)
	require.EqualValues(t, nNotFound, as.NContentNotFoundReqs)
	require.EqualValues(t, nSuccess, as.NSuccessfulRetrievals)
}

func (csh *carServerHarness) Stop(t *testing.T) {
	require.NoError(t, csh.store.Stop())
	csh.gwapi.Close()
	csh.carserver.Close()
}

func buildHarness(t *testing.T, ctx context.Context) *carServerHarness {
	carFile1 := "../testdata/files/sample-v1.car"
	rootcid1, bz1 := testutils.ParseCar(t, ctx, carFile1)
	carFile2 := "../testdata/files/sample-rw-bs-v2.car"
	rootcid2, bz2 := testutils.ParseCar(t, ctx, carFile2)
	out := make(map[string][]byte)
	out[rootcid1.String()] = bz1
	out[rootcid2.String()] = bz2

	temp := t.TempDir()

	mds := dss.MutexWrap(datastore.NewMapDatastore())
	sapi := NewStationAPIImpl(mds, nil)

	// create the getway api with a test http server
	svc := testutils.GetTestServerForRoots(t, out)
	gwAPI := carstore.NewGatewayAPI(svc.URL, sapi)
	lg := logs.NewSaturnLogger()
	cfg := carstore.Config{MaxCARFilesDiskSpace: 100000000}
	cs, err := carstore.New(temp, gwAPI, cfg, lg)
	require.NoError(t, err)
	sapi.SetStorageStatsFetcher(cs)
	require.NoError(t, cs.Start(ctx))

	// create and start the car server
	carserver := New(cs, lg, sapi)
	csvc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bz, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var cr types.CARTransferRequest
		if err := json.Unmarshal(bz, &cr); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		dr, err := cr.ToDAGRequest()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = carserver.ServeCARFile(ctx, dr, w)
		if errors.Is(err, carstore.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}))

	return &carServerHarness{
		store:     cs,
		gwapi:     svc,
		carserver: csvc,
		sapi:      sapi,
		root1:     rootcid1,
		root2:     rootcid2,
		bz1:       bz1,
		bz2:       bz2,
	}
}

func readHTTPResponse(t *testing.T, resp *http.Response) []byte {
	bz, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, resp)
	require.NoError(t, resp.Body.Close())
	return bz
}

func mkRequestWithoutSelector(t *testing.T, root cid.Cid, offset uint64) []byte {
	req := types.CARTransferRequest{
		Root:       root.String(),
		RequestId:  uuid.New().String(),
		SkipOffset: offset,
	}
	reqBz, err := json.Marshal(req)
	require.NoError(t, err)
	return reqBz
}

func sendHttpReq(t *testing.T, url string, body []byte) *http.Response {
	hreq, err := http.NewRequest("GET", url, bytes.NewReader(body))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(hreq)
	require.NoError(t, err)
	require.NotEmpty(t, resp)
	return resp
}
