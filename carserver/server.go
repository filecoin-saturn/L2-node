package carserver

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/filecoin-project/saturn-l2/types"

	"github.com/filecoin-project/saturn-l2/station"

	"github.com/filecoin-project/saturn-l2/logs"

	"github.com/filecoin-project/saturn-l2/carstore"

	bstore "github.com/ipfs/go-ipfs-blockstore"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"

	car "github.com/ipld/go-car/v2"
)

// CarServer serves CAR files for a given root and selector.
type CarServer struct {
	cs     *carstore.CarStore
	logger *logs.SaturnLogger
	spai   station.StationAPI
}

func New(cs *carstore.CarStore, logger *logs.SaturnLogger, sapi station.StationAPI) *CarServer {
	return &CarServer{
		cs:     cs,
		logger: logger,
		spai:   sapi,
	}
}

func (l *CarServer) ServeCARFile(ctx context.Context, dr *types.DagTraversalRequest, w io.Writer) error {
	sw := &statWriter{w: w}

	if err := l.cs.FetchAndWriteCAR(dr.RequestId, dr.Root, func(ro bstore.Blockstore) error {
		ls := cidlink.DefaultLinkSystem()
		bsa := bsadapter.Adapter{Wrapped: ro}
		ls.SetReadStorage(&bsa)

		_, err := car.TraverseV1(ctx, &ls, dr.Root, dr.Selector, sw, car.WithSkipOffset(dr.SkipOffset))
		if err != nil {
			if err := l.spai.RecordRetrievalServed(ctx, sw.n, 1); err != nil {
				l.logger.LogError(dr.RequestId, "failed to record retrieval failure", err)
			}

			l.logger.LogError(dr.RequestId, "car traversal failed", err)
			return fmt.Errorf("car traversal failed: %w", err)
		}

		if err := l.spai.RecordRetrievalServed(ctx, sw.n, 0); err != nil {
			l.logger.LogError(dr.RequestId, "failed to record successful retrieval", err)
		}
		return nil
	}); err != nil {
		if err := l.spai.RecordRetrievalServed(ctx, sw.n, 1); err != nil {
			l.logger.LogError(dr.RequestId, "failed to record retrieval failure", err)
		}

		if errors.Is(err, carstore.ErrNotFound) {
			l.logger.Infow(dr.RequestId, "not serving CAR as CAR not found", "err", err)
		} else {
			l.logger.LogError(dr.RequestId, "failed to traverse and serve car", err)
		}

		return err
	}

	l.logger.Infow(dr.RequestId, "car transfer successful")
	return nil
}

type statWriter struct {
	w io.Writer
	n uint64
}

func (sw *statWriter) Write(p []byte) (n int, err error) {
	n, err = sw.w.Write(p)
	sw.n += uint64(n)
	return
}
