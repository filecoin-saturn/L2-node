package carserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/saturn-l2/types"

	"github.com/filecoin-project/saturn-l2/station"

	"github.com/filecoin-project/saturn-l2/logs"

	"github.com/filecoin-project/saturn-l2/carstore"

	"github.com/pkg/errors"

	bstore "github.com/ipfs/go-ipfs-blockstore"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"

	car "github.com/ipld/go-car/v2"
)

var (
	maxRequestSize = int64(1048576) // 1 MiB - max size of the CAR transfer request
)

// HTTPCARServer serves CAR files for a given root and selector over http.
type HTTPCARServer struct {
	cs     *carstore.CarStore
	logger *logs.SaturnLogger
	spai   station.StationAPI
}

func New(cs *carstore.CarStore, logger *logs.SaturnLogger, sapi station.StationAPI) *HTTPCARServer {
	return &HTTPCARServer{
		cs:     cs,
		logger: logger,
		spai:   sapi,
	}
}

func (l *HTTPCARServer) ServeCARFile(w http.ResponseWriter, r *http.Request) {
	// read the json car transfer request
	var req types.CARTransferRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("failed to parse request: %s", err), http.StatusBadRequest)
		return
	}
	dr, err := req.ToDAGRequest()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse request: %s", err), http.StatusBadRequest)
		return
	}

	// we have parsed the request successfully -> start logging and serving it
	l.logger.Infow(dr.ReqId, "got car transfer request")

	sw := &statWriter{w: w}

	if err := l.cs.FetchAndWriteCAR(dr.ReqId, dr.Root, func(ro bstore.Blockstore) error {
		ls := cidlink.DefaultLinkSystem()
		bsa := bsadapter.Adapter{Wrapped: ro}
		ls.SetReadStorage(&bsa)

		_, err = car.TraverseV1(r.Context(), &ls, dr.Root, dr.Selector, sw, car.WithSkipOffset(dr.Skip))
		if err != nil {
			if err := l.spai.RecordRetrievalServed(r.Context(), sw.n, 1); err != nil {
				l.logger.LogError(dr.ReqId, "failed to record retrieval failure", err)
			}

			l.logger.LogError(dr.ReqId, "car transfer failed", err)
			return fmt.Errorf("car traversal failed: %w", err)
		}

		if err := l.spai.RecordRetrievalServed(r.Context(), sw.n, 0); err != nil {
			l.logger.LogError(dr.ReqId, "failed to record successful retrieval", err)
		}
		return nil
	}); err != nil {
		if err := l.spai.RecordRetrievalServed(r.Context(), sw.n, 1); err != nil {
			l.logger.LogError(dr.ReqId, "failed to record retrieval failure", err)
		}
		l.logger.LogError(dr.ReqId, "failed to server car", err)

		if errors.Is(err, carstore.ErrNotFound) {
			http.Error(w, "car not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	l.logger.Infow(dr.ReqId, "car transfer successful")
	// TODO: Talk to Log injestor here
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
