package carstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/dagstore"

	"github.com/filecoin-project/saturn-l2/logs"
	"github.com/google/uuid"

	"github.com/filecoin-project/saturn-l2/testutils"
	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/stretchr/testify/require"
)

func TestPersistentCache(t *testing.T) {
	lg := logs.NewSaturnLogger()
	cfg := Config{}

	ctx := context.Background()
	temp := t.TempDir()

	carv1File := "../testdata/files/sample-v1.car"
	rootcid, bz := testutils.ParseCar(t, carv1File)

	svc := testutils.GetTestServer(t, rootcid.String(), bz)
	defer svc.Close()
	cs, err := New(temp, &gatewayAPI{
		baseURL: svc.URL,
	}, cfg, lg)
	require.NoError(t, err)
	require.NoError(t, cs.Start(ctx))

	reqID := uuid.New()

	// first hit -> not found
	err = cs.FetchAndWriteCAR(reqID, rootcid, func(_ bstore.Blockstore) error {
		return nil
	})
	require.EqualValues(t, err, ErrNotFound)

	// second hit -> not found
	err = cs.FetchAndWriteCAR(reqID, rootcid, func(_ bstore.Blockstore) error {
		return nil
	})
	require.EqualValues(t, err, ErrNotFound)

	require.Eventually(t, func() bool {
		ks, err := cs.dagst.ShardsContainingMultihash(ctx, rootcid.Hash())
		return err == nil && len(ks) == 1
	}, 50*time.Second, 200*time.Millisecond)

	// third hit -> found
	err = cs.FetchAndWriteCAR(reqID, rootcid, func(bs bstore.Blockstore) error {
		blk, err := bs.Get(ctx, rootcid)
		if err != nil {
			return err
		}
		if blk == nil {
			return errors.New("empty root")
		}
		return nil
	})
	require.NoError(t, err)

	// fourth hit -> found
	err = cs.FetchAndWriteCAR(reqID, rootcid, func(bs bstore.Blockstore) error {
		blk, err := bs.Get(ctx, rootcid)
		if err != nil {
			return err
		}
		if blk == nil {
			return errors.New("empty root")
		}
		return nil
	})
	require.NoError(t, err)

	// wait for shard to become reclaimable again
	require.Eventually(t, func() bool {
		si, err := cs.dagst.GetShardInfo(keyFromCIDMultihash(rootcid))
		return err == nil && si.ShardState == dagstore.ShardStateAvailable
	}, 50*time.Second, 200*time.Millisecond)

	// run dagstore GC -> CAR file is removed
	res, err := cs.dagst.GC(ctx)
	require.NoError(t, err)
	require.Len(t, res.Shards, 1)

	// fetch car -> fails as we do not have it but will trigger a fetch again
	err = cs.FetchAndWriteCAR(reqID, rootcid, func(_ bstore.Blockstore) error {
		return nil
	})
	require.EqualValues(t, err, ErrNotFound)

	// fetch car -> works now as car file was downloaded in the previous fetch
	require.Eventually(t, func() bool {
		err = cs.FetchAndWriteCAR(reqID, rootcid, func(_ bstore.Blockstore) error {
			return nil
		})
		return err == nil
	}, 50*time.Second, 200*time.Millisecond)

	require.NoError(t, cs.Close())
}

func TestPersistentCacheConcurrent(t *testing.T) {
	lg := logs.NewSaturnLogger()
	cfg := Config{}

	ctx := context.Background()
	temp := t.TempDir()

	carv1File := "../testdata/files/sample-v1.car"
	rootcid, bz := testutils.ParseCar(t, carv1File)

	svc := testutils.GetTestServer(t, rootcid.String(), bz)
	defer svc.Close()
	cs, err := New(temp, &gatewayAPI{
		baseURL: svc.URL,
	}, cfg, lg)
	require.NoError(t, err)
	require.NoError(t, cs.Start(ctx))

	// send 100 concurrent requests
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cs.FetchAndWriteCAR(uuid.New(), rootcid, func(_ bstore.Blockstore) error {
				return nil
			})
		}()
	}
	wg.Wait()

	require.Eventually(t, func() bool {
		ks, err := cs.dagst.ShardsContainingMultihash(ctx, rootcid.Hash())
		return err == nil && len(ks) == 1
	}, 50*time.Second, 200*time.Millisecond)

	// fetch shard 100 times -> should work
	var errg errgroup.Group
	for i := 0; i < 100; i++ {
		errg.Go(func() error {

			return cs.FetchAndWriteCAR(uuid.New(), rootcid, func(_ bstore.Blockstore) error {
				return nil
			})
		})

	}
	require.NoError(t, errg.Wait())

	// dagstore should only have one copy
	sz, err := transientDirSize(filepath.Join(temp, "transients"))
	require.NoError(t, err)
	require.EqualValues(t, len(bz), sz)
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
