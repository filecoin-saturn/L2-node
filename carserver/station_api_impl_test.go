package carserver

import (
	"context"
	"testing"

	"github.com/filecoin-project/saturn-l2/station"
	datastore "github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func TestStationAPIImpl(t *testing.T) {
	ctx := context.Background()
	ds := dss.MutexWrap(datastore.NewMapDatastore())
	sapi := NewStationAPIImpl(ds, &mockStorageStatsFetcher{
		out: 790,
	})

	as, err := sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			Bytes: 790,
		}}, as)

	require.NoError(t, sapi.RecordDataDownloaded(ctx, 100))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			Bytes: 790,
		},
		ReqStats: station.ReqStats{
			Download: 100,
		}}, as)

	require.NoError(t, sapi.RecordDataDownloaded(ctx, 200))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			Bytes: 790,
		},
		ReqStats: station.ReqStats{
			Download: 300,
		}}, as)

	require.NoError(t, sapi.RecordRetrievalServed(ctx, 100, 0))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			Bytes: 790,
		},
		ReqStats: station.ReqStats{
			Upload:          100,
			ContentRequests: 1,
			Download:        300,
		}}, as)

	require.NoError(t, sapi.RecordRetrievalServed(ctx, 500, 2))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			Bytes: 790,
		},
		ReqStats: station.ReqStats{
			Upload:           600,
			ContentRequests:  2,
			ContentReqErrors: 2,
			Download:         300,
		}}, as)
}

type mockStorageStatsFetcher struct {
	out uint64
}

func (ms *mockStorageStatsFetcher) Stat() (station.StorageStats, error) {
	return station.StorageStats{
		Bytes: ms.out,
	}, nil
}
