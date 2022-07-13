package carstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"

	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/dagstore"

	"github.com/filecoin-project/saturn-l2/logs"
	"github.com/google/uuid"

	"github.com/filecoin-project/saturn-l2/testutils"
	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/stretchr/testify/require"
)

func TestPersistentCache(t *testing.T) {
	ctx := context.Background()

	carv1File := "../testdata/files/sample-v1.car"
	rootcid, bz := testutils.ParseCar(t, carv1File)
	svc := testutils.GetTestServer(t, rootcid.String(), bz)
	defer svc.Close()
	csh := newCarStoreHarness(t, svc.URL)
	reqID := uuid.New()

	// first hit -> not found
	csh.fetchAndAssertNotFound(reqID, rootcid)

	// second hit -> not found
	csh.fetchAndAssertNotFound(reqID, rootcid)

	require.Eventually(t, func() bool {
		ks, err := csh.cs.dagst.ShardsContainingMultihash(ctx, rootcid.Hash())
		return err == nil && len(ks) == 1
	}, 50*time.Second, 200*time.Millisecond)

	// third hit -> found
	csh.fetchAndAssertFound(ctx, reqID, rootcid)

	// fourth hit -> found
	csh.fetchAndAssertFound(ctx, reqID, rootcid)

	// wait for shard to become reclaimable again
	require.Eventually(t, func() bool {
		si, err := csh.cs.dagst.GetShardInfo(keyFromCIDMultihash(rootcid))
		return err == nil && si.ShardState == dagstore.ShardStateAvailable
	}, 50*time.Second, 200*time.Millisecond)

	// run dagstore GC -> CAR file is removed
	res, err := csh.cs.dagst.GC(ctx)
	require.NoError(t, err)
	require.Len(t, res.Shards, 1)

	// fetch car -> fails as we do not have it but will trigger a fetch again
	csh.fetchAndAssertNotFound(reqID, rootcid)

	// fetch car -> works now as car file was downloaded in the previous fetch
	require.Eventually(t, func() bool {
		err = csh.cs.FetchAndWriteCAR(reqID, rootcid, func(_ bstore.Blockstore) error {
			return nil
		})
		return err == nil
	}, 50*time.Second, 200*time.Millisecond)

	require.NoError(t, csh.cs.Close())
}

func TestPersistentCacheConcurrent(t *testing.T) {
	ctx := context.Background()
	carv1File := "../testdata/files/sample-v1.car"
	rootcid, bz := testutils.ParseCar(t, carv1File)
	svc := testutils.GetTestServer(t, rootcid.String(), bz)
	defer svc.Close()
	csh := newCarStoreHarness(t, svc.URL)

	// send 100 concurrent requests
	csh.fetchNAsyNC(rootcid, 100)

	require.Eventually(t, func() bool {
		ks, err := csh.cs.dagst.ShardsContainingMultihash(ctx, rootcid.Hash())
		return err == nil && len(ks) == 1
	}, 50*time.Second, 200*time.Millisecond)

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
}

func TestMountFetchErrorConcurrent(t *testing.T) {
	carv1File := "../testdata/files/sample-v1.car"
	rootcid, _ := testutils.ParseCar(t, carv1File)
	svc := testutils.GetTestErrorServer(t)
	defer svc.Close()
	csh := newCarStoreHarness(t, svc.URL)

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
}

func TestDownloadTimeout(t *testing.T) {
	carv1File := "../testdata/files/sample-v1.car"
	rootcid, _ := testutils.ParseCar(t, carv1File)
	x := defaultDownloadTimeout
	defer func() {
		defaultDownloadTimeout = x
	}()
	defaultDownloadTimeout = 100 * time.Millisecond
	svc := testutils.GetTestHangingServer(t)
	csh := newCarStoreHarness(t, svc.URL)

	reqID := uuid.New()
	// first try -> not found
	csh.fetchAndAssertNotFound(reqID, rootcid)

	// second try -> not found
	csh.fetchAndAssertNotFound(reqID, rootcid)
	time.Sleep(1 * time.Second)

	// still errors out
	csh.fetchAndAssertNotFound(reqID, rootcid)
}

func (csh *carstoreHarness) fetchNAsyNC(rootCid cid.Cid, n int) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			csh.cs.FetchAndWriteCAR(uuid.New(), rootCid, func(_ bstore.Blockstore) error {
				return nil
			})
		}()
	}
	wg.Wait()
}

func transientDirSize(dir string) (int64, error) {
	var size int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

type carstoreHarness struct {
	t  *testing.T
	cs *CarStore
}

func newCarStoreHarness(t *testing.T, apiurl string) *carstoreHarness {
	lg := logs.NewSaturnLogger()
	cfg := Config{}

	ctx := context.Background()
	temp := t.TempDir()

	cs, err := New(temp, &gatewayAPI{
		baseURL: apiurl,
	}, cfg, lg)
	require.NoError(t, err)
	require.NoError(t, cs.Start(ctx))

	return &carstoreHarness{
		cs: cs,
		t:  t,
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
