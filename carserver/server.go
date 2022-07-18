package carserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/filecoin-project/saturn-l2/station"

	"github.com/filecoin-project/saturn-l2/logs"

	"github.com/filecoin-project/saturn-l2/carstore"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"

	gostream "github.com/libp2p/go-libp2p-gostream"

	logging "github.com/ipfs/go-log/v2"

	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/libp2p/go-libp2p-core/host"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"

	car "github.com/ipld/go-car/v2"
)

// CARTransferProtocol is the protocol on which the CAR file will be streamed
const CARTransferProtocol = "/saturn/l2/car/1.0"

var log = logging.Logger("libp2p-http-server")

var (
	maxRequestSize = int64(1048576) // 1 MiB - max size of the CAR transfer request
)

// Libp2pHttpCARServer serves CAR files for a given root and selector over the libp2p-http CARTransferProtocol.
type Libp2pHttpCARServer struct {
	ctx    context.Context
	cancel context.CancelFunc

	h           host.Host
	server      *http.Server
	netListener net.Listener

	cs     *carstore.CarStore
	logger *logs.SaturnLogger

	spai station.StationAPI
}

func New(h host.Host, cs *carstore.CarStore, logger *logs.SaturnLogger, sapi station.StationAPI) *Libp2pHttpCARServer {
	return &Libp2pHttpCARServer{
		h:      h,
		cs:     cs,
		logger: logger,
		spai:   sapi,
	}
}

func (l *Libp2pHttpCARServer) Start(ctx context.Context) error {
	l.ctx, l.cancel = context.WithCancel(ctx)

	// Listen on HTTP over libp2p
	listener, err := gostream.Listen(l.h, CARTransferProtocol)
	if err != nil {
		return fmt.Errorf("starting gostream listener: %w", err)
	}

	l.netListener = listener
	handler := http.NewServeMux()
	handler.HandleFunc("/", l.serveCARFile)
	l.server = &http.Server{
		Handler: handler,
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return l.ctx
		},
	}
	go l.server.Serve(listener) //nolint:errcheck

	return nil
}

func (l *Libp2pHttpCARServer) Stop() error {
	l.cancel()

	lerr := l.netListener.Close()
	serr := l.server.Close()

	if lerr != nil {
		return lerr
	}
	if serr != nil {
		return serr
	}
	return nil
}

func (l *Libp2pHttpCARServer) serveCARFile(w http.ResponseWriter, r *http.Request) {
	// decode the remote peer ID and protect the libp2p connection for the lifetime of the transfer
	pid, err := peer.Decode(r.RemoteAddr)
	if err != nil {
		log.Infow("car transfer request failed: parsing remote address as peer ID",
			"remote-addr", r.RemoteAddr, "err", err)
		http.Error(w, "Failed to parse remote address '"+r.RemoteAddr+"' as peer ID", http.StatusBadRequest)
		return
	}
	tag := uuid.New().String()
	l.h.ConnManager().Protect(pid, tag)
	defer l.h.ConnManager().Unprotect(pid, tag)

	// read the json car transfer request
	var req CARTransferRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("failed to parse request: %s", err), http.StatusBadRequest)
		return
	}
	dr, err := carRequestToDAGRequest(&req)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse request: %s", err), http.StatusBadRequest)
		return
	}

	// we have parsed the request successfully -> start logging and serving it
	l.logger.Infow(dr.reqId, "got car transfer request")

	sw := &statWriter{w: w}

	if err := l.cs.FetchAndWriteCAR(dr.reqId, dr.root, func(ro bstore.Blockstore) error {
		ls := cidlink.DefaultLinkSystem()
		bsa := bsadapter.Adapter{Wrapped: ro}
		ls.SetReadStorage(&bsa)

		_, err = car.TraverseV1(l.ctx, &ls, dr.root, dr.selector, sw, car.WithSkipOffset(dr.skip))
		if err != nil {
			if err := l.spai.RecordRetrievalServed(l.ctx, sw.n, 1); err != nil {
				l.logger.LogError(dr.reqId, "failed to record retrieval failure", err)
			}

			l.logger.LogError(dr.reqId, "car transfer failed", err)
			return fmt.Errorf("car traversal failed: %w", err)
		}
		if err := l.spai.RecordRetrievalServed(l.ctx, sw.n, 0); err != nil {
			l.logger.LogError(dr.reqId, "failed to record successful retrieval", err)
		}
		return nil
	}); err != nil {
		if errors.Is(err, carstore.ErrNotFound) {
			l.logger.Debugw(dr.reqId, "car not found")
			w.WriteHeader(http.StatusNotFound)
		} else {
			l.logger.LogError(dr.reqId, "car transfer failed", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	l.logger.Infow(dr.reqId, "car transfer successful")
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
