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
			BytesCurrentlyStored: 790,
		}}, as)

	require.NoError(t, sapi.RecordDataDownloaded(ctx, 100))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			BytesCurrentlyStored: 790,
		},
		ReqStats: station.ReqStats{
			TotalBytesDownloaded: 100,
		}}, as)

	require.NoError(t, sapi.RecordDataDownloaded(ctx, 200))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			BytesCurrentlyStored: 790,
		},
		ReqStats: station.ReqStats{
			TotalBytesDownloaded: 300,
		}}, as)

	require.NoError(t, sapi.RecordRetrievalServed(ctx, 100, 0))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			BytesCurrentlyStored: 790,
		},
		ReqStats: station.ReqStats{
			TotalBytesUploaded:    100,
			NContentRequests:      1,
			TotalBytesDownloaded:  300,
			NSuccessfulRetrievals: 1,
		}}, as)

	require.NoError(t, sapi.RecordRetrievalServed(ctx, 500, 2))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			BytesCurrentlyStored: 790,
		},
		ReqStats: station.ReqStats{
			TotalBytesUploaded:    600,
			NContentRequests:      2,
			NContentReqErrors:     2,
			TotalBytesDownloaded:  300,
			NSuccessfulRetrievals: 1,
		}}, as)

	// restart API -> we should still get the same values
	sapi = NewStationAPIImpl(ds, &mockStorageStatsFetcher{
		out: 790,
	})
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			BytesCurrentlyStored: 790,
		},
		ReqStats: station.ReqStats{
			TotalBytesUploaded:    600,
			NContentRequests:      2,
			NContentReqErrors:     2,
			NSuccessfulRetrievals: 1,
			TotalBytesDownloaded:  300,
		}}, as)

	require.NoError(t, sapi.RecordRetrievalServed(ctx, 500, 0))
	as, err = sapi.AllStats(ctx)
	require.NoError(t, err)
	require.Equal(t, station.StationStats{RPInfo: station.RPInfo{Version: Version},
		StorageStats: station.StorageStats{
			BytesCurrentlyStored: 790,
		},
		ReqStats: station.ReqStats{
			TotalBytesUploaded:    1100,
			NContentRequests:      3,
			NContentReqErrors:     2,
			NSuccessfulRetrievals: 2,
			TotalBytesDownloaded:  300,
		}}, as)

}

type mockStorageStatsFetcher struct {
	out uint64
}

func (ms *mockStorageStatsFetcher) Stat() (station.StorageStats, error) {
	return station.StorageStats{
		BytesCurrentlyStored: ms.out,
	}, nil
}
