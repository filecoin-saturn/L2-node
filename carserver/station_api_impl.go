package carserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ipfs/go-datastore/namespace"

	datastore "github.com/ipfs/go-datastore"

	"github.com/filecoin-project/saturn-l2/station"
)

var Version = "v0.0.0"
var contentReqKey = datastore.NewKey("/content-req")
var storeNameSpace = "station"

type StationAPIImpl struct {
	ss station.StorageStatsFetcher

	mu sync.RWMutex
	ds datastore.Batching
}

func NewStationAPIImpl(ds datastore.Batching, ss station.StorageStatsFetcher) *StationAPIImpl {
	nds := namespace.Wrap(ds, datastore.NewKey(storeNameSpace))
	return &StationAPIImpl{
		ss: ss,
		ds: nds,
	}
}

func (s *StationAPIImpl) SetStorageStatsFetcher(ss station.StorageStatsFetcher) {
	s.ss = ss
}

func (s *StationAPIImpl) RecordRetrievalServed(ctx context.Context, bytesServed, nErrors, nNotFound, nSuccess uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.createOrUpdateReqStatsUnlocked(ctx, func(r *station.ReqStats) {
		r.TotalBytesUploaded = bytesServed
		r.NContentRequests = 1
		r.NContentNotFoundReqs = nNotFound
		r.NContentReqErrors = nErrors
		r.NSuccessfulRetrievals = nSuccess
	}, func(r *station.ReqStats) {
		r.TotalBytesUploaded += bytesServed
		r.NContentRequests += 1
		r.NContentNotFoundReqs += nNotFound
		r.NContentReqErrors += nErrors
		r.NSuccessfulRetrievals += nSuccess
	})
}

func (s *StationAPIImpl) RecordDataDownloaded(ctx context.Context, bytesDownloaded uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.createOrUpdateReqStatsUnlocked(ctx, func(r *station.ReqStats) {
		r.TotalBytesDownloaded = bytesDownloaded
	}, func(r *station.ReqStats) {
		r.TotalBytesDownloaded += bytesDownloaded
	})
}

func (s *StationAPIImpl) createOrUpdateReqStatsUnlocked(ctx context.Context, createFn func(s *station.ReqStats),
	updateFn func(s *station.ReqStats)) error {

	bz, err := s.ds.Get(ctx, contentReqKey)
	if err != nil && err != datastore.ErrNotFound {
		return fmt.Errorf("failed to get retrieval stats from datastore: %w", err)
	}
	if err == datastore.ErrNotFound {
		stats := station.ReqStats{}
		createFn(&stats)
		bz, err := json.Marshal(stats)
		if err != nil {
			return fmt.Errorf("failed to marshal retrieval stats to json: %w", err)
		}

		if err := s.ds.Put(ctx, contentReqKey, bz); err != nil {
			return fmt.Errorf("failed to put to datastore: %w", err)
		}
		if err := s.ds.Sync(ctx, contentReqKey); err != nil {
			return fmt.Errorf("failed to sync datsstore key: %w", err)
		}
		return nil
	}
	var stats station.ReqStats
	if err := json.Unmarshal(bz, &stats); err != nil {
		return fmt.Errorf("failed to unmarshal existing retrieval stats: %w", err)
	}

	updateFn(&stats)

	bz, err = json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("failed to marshal retrieval stats to json: %w", err)
	}
	if err := s.ds.Put(ctx, contentReqKey, bz); err != nil {
		return fmt.Errorf("failed to put datastore key: %w", err)
	}
	if err := s.ds.Sync(ctx, contentReqKey); err != nil {
		return fmt.Errorf("failed to sync datsstore key: %w", err)
	}
	return nil
}

func (s *StationAPIImpl) AllStats(ctx context.Context) (station.StationStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// storage stats
	storage, err := s.ss.Stat()
	if err != nil {
		return station.StationStats{}, fmt.Errorf("failed to fetch storage stats: %w", err)
	}

	// info
	info := station.RPInfo{
		Version: Version,
	}

	// fetch retrieval stats
	bz, err := s.ds.Get(ctx, contentReqKey)
	if err != nil && err != datastore.ErrNotFound {
		return station.StationStats{}, fmt.Errorf("failed to fetch retrieval stats: %w", err)
	}
	if err == datastore.ErrNotFound {
		return station.StationStats{
			RPInfo:       info,
			StorageStats: storage,
		}, nil
	}

	var rs station.ReqStats
	if err := json.Unmarshal(bz, &rs); err != nil {
		return station.StationStats{}, fmt.Errorf("failed to unmarshal retrieval stats from json: %w", err)
	}

	return station.StationStats{
		RPInfo:       info,
		StorageStats: storage,
		ReqStats:     rs,
	}, nil
}

var _ station.StationAPI = &StationAPIImpl{}
