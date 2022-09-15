package carstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/multicodec"

	"github.com/filecoin-project/saturn-l2/station"

	cid "github.com/ipfs/go-cid"

	"github.com/filecoin-project/dagstore"

	"github.com/filecoin-project/saturn-l2/logs"
	"github.com/google/uuid"

	"github.com/filecoin-project/saturn-l2/testutils"
	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/stretchr/testify/require"
)

var defaultMaxSize = int64(200 * 1073741824) // 200 Gib

func init() {
	multicodec.RegisterEncoder(0x71, dagcbor.Encode)
	multicodec.RegisterDecoder(0x71, dagcbor.Decode)
}

func TestPersistentCache(t *testing.T) {
	ctx := context.Background()

	carv1File := "../testdata/files/sample-v1.car"
	rootcid, bz := testutils.ParseCar(t, ctx, carv1File)
	svc := testutils.GetTestServer(t, rootcid.String(), bz)
	defer svc.Close()
	csh := newCarStoreHarness(t, svc.URL, Config{MaxCARFilesDiskSpace: defaultMaxSize})
	reqID := uuid.New()

	csh.assertStorageStats(t, station.StorageStats{BytesCurrentlyStored: 0})
	// first hit -> not found
	csh.fetchAndAssertNotFound(reqID, rootcid)

	// second hit -> not found
	csh.fetchAndAssertNotFound(reqID, rootcid)

	// wait till l2 has fetched and cached it
	csh.assertAvailable(t, ctx, rootcid)

	// third hit -> found
	csh.fetchAndAssertFound(ctx, reqID, rootcid)
	require.EqualValues(t, len(bz), csh.ms.nDownloaded())

	// fourth hit -> found
	csh.fetchAndAssertFound(ctx, reqID, rootcid)
	require.EqualValues(t, len(bz), csh.ms.nDownloaded())

	// wait for shard to become reclaimable again
	require.Eventually(t, func() bool {
		si, err := csh.cs.dagst.GetShardInfo(keyFromCIDMultihash(rootcid))
		return err == nil && si.ShardState == dagstore.ShardStateAvailable
	}, 50*time.Second, 200*time.Millisecond)

	csh.assertStorageStats(t, station.StorageStats{BytesCurrentlyStored: uint64(len(bz))})

	// run dagstore GC -> CAR file is removed
	res, err := csh.cs.dagst.GC(ctx)
	require.NoError(t, err)
	require.Len(t, res.Shards, 1)
	csh.assertStorageStats(t, station.StorageStats{BytesCurrentlyStored: 0})

	// fetch car -> fails as we do not have it but will trigger a fetch again
	csh.fetchAndAssertNotFound(reqID, rootcid)

	// fetch car -> works now as car file was downloaded in the previous fetch
	require.Eventually(t, func() bool {
		err = csh.cs.FetchAndWriteCAR(reqID, rootcid, func(_ bstore.Blockstore) error {
			return nil
		})
		return err == nil
	}, 50*time.Second, 200*time.Millisecond)

	require.NoError(t, csh.cs.Stop())
	require.EqualValues(t, 2*len(bz), csh.ms.nDownloaded())

	csh.assertStorageStats(t, station.StorageStats{BytesCurrentlyStored: uint64(len(bz))})
}

/*
func TestPersistentCacheConcurrent(t *testing.T) {
	ctx := context.Background()
	carv1File := "../testdata/files/sample-v1.car"
	rootcid, bz := testutils.ParseCar(t, ctx, carv1File)
	svc := testutils.GetTestServer(t, rootcid.String(), bz)
	defer svc.Close()
	csh := newCarStoreHarness(t, svc.URL, Config{MaxCARFilesDiskSpace: defaultMaxSize})

	// send 100 concurrent requests
	csh.fetchNAsyNC(rootcid, 100)

	csh.assertAvailable(t, ctx, rootcid)

	// fetch shard 100 times -> should work
	var errg errgroup.Group
	for i := 0; i < 100; i++ {
		errg.Go(func() error {
			return csh.cs.FetchAndWriteCAR(uuid.New(), rootcid, func(_ bstore.Blockstore) error {
				return nil
			})
		})

	}
	require.NoError(t, errg.Wait())
	require.EqualValues(t, len(bz), csh.ms.nDownloaded())

	csh.assertStorageStats(t, station.StorageStats{BytesCurrentlyStored: uint64(len(bz))})
}

func TestPersistentCacheMultipleParallelRequests(t *testing.T) {
	ctx := context.Background()
	carFile1 := "../testdata/files/sample-v1.car"
	rootcid1, bz1 := testutils.ParseCar(t, ctx, carFile1)

	carFile2 := "../testdata/files/sample-rw-bs-v2.car"
	rootcid2, bz2 := testutils.ParseCar(t, ctx, carFile2)

	out := make(map[string][]byte)
	out[rootcid1.String()] = bz1
	out[rootcid2.String()] = bz2

	svc := testutils.GetTestServerForRoots(t, out)
	defer svc.Close()

	csh := newCarStoreHarness(t, svc.URL, Config{MaxCARFilesDiskSpace: defaultMaxSize})
	// send 100 concurrent requests
	csh.fetchNAsyNC(rootcid1, 100)
	// send 100 concurrent requests
	csh.fetchNAsyNC(rootcid2, 100)

	csh.assertAvailable(t, ctx, rootcid1)
	csh.assertAvailable(t, ctx, rootcid2)

	roots := []cid.Cid{rootcid1, rootcid2}

	// fetch shard 100 times -> should work
	var errg errgroup.Group
	for i := 0; i < 100; i++ {
		i := i
		errg.Go(func() error {
			return csh.cs.FetchAndWriteCAR(uuid.New(), roots[i%2], func(bs bstore.Blockstore) error {
				blk, err := bs.Get(ctx, roots[i%2])
				if err != nil {
					return err
				}
				if blk == nil {
					return errors.New("not found")
				}

				return nil
			})
		})

	}
	require.NoError(t, errg.Wait())

	csh.assertStorageStats(t, station.StorageStats{BytesCurrentlyStored: uint64(len(bz1) + len(bz2))})
	require.EqualValues(t, len(bz1)+len(bz2), csh.ms.nDownloaded())
}

func TestMountFetchErrorConcurrent(t *testing.T) {
	ctx := context.Background()
	carv1File := "../testdata/files/sample-v1.car"
	rootcid, _ := testutils.ParseCar(t, ctx, carv1File)
	svc := testutils.GetTestErrorServer(t)
	defer svc.Close()
	csh := newCarStoreHarness(t, svc.URL, Config{MaxCARFilesDiskSpace: defaultMaxSize})

	// send 100 concurrent requests
	csh.fetchNAsyNC(rootcid, 100)

	// fetch 100 times -> all fail and no panic
	errCh := make(chan error, 100)

	for i := 0; i < 100; i++ {
		go func() {
			errCh <- csh.cs.FetchAndWriteCAR(uuid.New(), rootcid, func(_ bstore.Blockstore) error {
				return nil
			})
		}()
	}

	for i := 0; i < 100; i++ {
		err := <-errCh
		require.EqualError(t, err, ErrNotFound.Error())
	}

	require.EqualValues(t, 0, csh.ms.nDownloaded())
	csh.assertStorageStats(t, station.StorageStats{BytesCurrentlyStored: 0})
}

func TestDownloadTimeout(t *testing.T) {
	ctx := context.Background()
	carv1File := "../testdata/files/sample-v1.car"

	rootcid, _ := testutils.ParseCar(t, ctx, carv1File)

	svc := testutils.GetTestHangingServer(t)
	csh := newCarStoreHarness(t, svc.URL, Config{MaxCARFilesDiskSpace: defaultMaxSize, DownloadTimeout: 1 * time.Millisecond})

	reqID := uuid.New()
	// first try -> not found
	csh.fetchAndAssertNotFound(reqID, rootcid)

	// second try -> not found
	csh.fetchAndAssertNotFound(reqID, rootcid)

	time.Sleep(3 * time.Second)

	// still errors out
	csh.fetchAndAssertNotFound(reqID, rootcid)
	require.EqualValues(t, 0, csh.ms.nDownloaded())
	csh.assertStorageStats(t, station.StorageStats{BytesCurrentlyStored: 0})
}*/

func (csh *carstoreHarness) assertAvailable(t *testing.T, ctx context.Context, c cid.Cid) {
	require.Eventually(t, func() bool {
		ks, err := csh.cs.dagst.ShardsContainingMultihash(ctx, c.Hash())
		return err == nil && len(ks) == 1
	}, 50*time.Second, 200*time.Millisecond)
}

func (csh *carstoreHarness) fetchNAsyNC(rootCid cid.Cid, n int) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			csh.cs.FetchAndWriteCAR(uuid.New(), rootCid, func(_ bstore.Blockstore) error { // nolint: errcheck
				return nil
			})
		}()
	}
	wg.Wait()
}

type carstoreHarness struct {
	t  *testing.T
	cs *CarStore
	ms *mockStationAPI
}

func newCarStoreHarness(t *testing.T, apiurl string, cfg Config) *carstoreHarness {
	sapi := &mockStationAPI{}
	lg := logs.NewSaturnLogger()

	ctx := context.Background()
	temp := t.TempDir()

	cs, err := New(temp, NewGatewayAPI(apiurl, sapi, 100000000), cfg, lg)
	require.NoError(t, err)
	require.NoError(t, cs.Start(ctx))

	return &carstoreHarness{
		cs: cs,
		t:  t,
		ms: sapi,
	}
}

func (csh *carstoreHarness) fetchAndAssertNotFound(reqID uuid.UUID, rootCid cid.Cid) {
	err := csh.cs.FetchAndWriteCAR(reqID, rootCid, func(_ bstore.Blockstore) error {
		return nil
	})
	require.EqualValues(csh.t, err, ErrNotFound)
}

func (csh *carstoreHarness) fetchAndAssertFound(ctx context.Context, reqID uuid.UUID, rootCid cid.Cid) {
	err := csh.cs.FetchAndWriteCAR(reqID, rootCid, func(bs bstore.Blockstore) error {
		blk, err := bs.Get(ctx, rootCid)
		if err != nil {
			return err
		}
		if blk == nil {
			return errors.New("empty root")
		}
		return nil
	})
	require.NoError(csh.t, err)
}

func (csh *carstoreHarness) assertStorageStats(t *testing.T, ess station.StorageStats) {
	ss, err := csh.cs.Stat()
	require.NoError(t, err)
	require.Equal(t, ess, ss)
}
