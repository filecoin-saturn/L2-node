package station

import (
	"context"
)

type StationAPI interface {
	RecordRetrievalServed(ctx context.Context, bytesServed, nErrors, nNotFound, nSuccess uint64) error
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
	BytesCurrentlyStored uint64
}

type ReqStats struct {
	TotalBytesUploaded    uint64
	TotalBytesDownloaded  uint64
	NContentRequests      uint64
	NContentNotFoundReqs  uint64
	NSuccessfulRetrievals uint64

	NContentReqErrors uint64
}
