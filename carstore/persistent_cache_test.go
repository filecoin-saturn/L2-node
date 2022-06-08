package carstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/filecoin-project/saturn-l2/testutils"
	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/stretchr/testify/require"
)

func TestPersistentCache(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()

	carv1File := "../testdata/files/sample-v1.car"
	rootcid, bz := testutils.ParseCar(t, carv1File)

	svc := testutils.GetTestServer(t, rootcid.String(), bz)
	defer svc.Close()
	cs, err := New(temp, &gatewayAPI{
		baseURL: svc.URL,
	})
	require.NoError(t, err)
	require.NoError(t, cs.Start(ctx))

	// first hit -> not found
	err = cs.FetchAndWriteCAR(rootcid, func(_ bstore.Blockstore) error {
		return nil
	})
	require.Error(t, err)
	require.EqualValues(t, err, ErrNotFound)

	// second hit -> not found
	err = cs.FetchAndWriteCAR(rootcid, func(_ bstore.Blockstore) error {
		return nil
	})
	require.Error(t, err)
	require.EqualValues(t, err, ErrNotFound)

	require.Eventually(t, func() bool {
		ks, err := cs.dagst.ShardsContainingMultihash(ctx, rootcid.Hash())
		return err == nil && len(ks) == 1
	}, 50*time.Second, 200*time.Millisecond)

	// third hit -> found
	err = cs.FetchAndWriteCAR(rootcid, func(bs bstore.Blockstore) error {
		return nil
	})
	require.NoError(t, err)

	// fourth hit -> found
	err = cs.FetchAndWriteCAR(rootcid, func(bs bstore.Blockstore) error {
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

	require.NoError(t, cs.Close())
}
