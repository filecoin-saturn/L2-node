package station

import (
	"context"
)

type StationAPI interface {
	RecordRetrievalServed(ctx context.Context, bytesServed, nErrors uint64) error
	AllStats(ctx context.Context) (StationStats, error)
	RecordDataDownloaded(ctx context.Context, bytesDownloaded uint64) error
}

type StorageStatsFetcher interface {
	Stat() (StorageStats, error)
}

type StationStats struct {
	RPInfo
	StorageStats
	ReqStats
}

type RPInfo struct {
	Version string
}

type StorageStats struct {
	Bytes uint64
}

type ReqStats struct {
	Upload           uint64
	Download         uint64
	ContentRequests  uint64
	ContentReqErrors uint64
}
