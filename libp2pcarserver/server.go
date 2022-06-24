package libp2pcarserver

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"time"

	logging "github.com/ipfs/go-log/v2"

	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/filecoin-project/saturn-l2/store"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/libp2p/go-libp2p-core/host"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"

	"github.com/ipld/go-car/v2"
)

const CARTransferProtocol = "/saturn/l2/car/1.0" // car transfer protocol
var log = logging.Logger("car-transfer")

var (
	maxRequestSize = 1048576 // 1 MiB - max size of the CAR transfer request
	readDeadline   = 10 * time.Second
	writeDeadline  = 30 * time.Minute
)

// Libp2pCARServer serves CAR files for a given root and selector over the libp2p CARTransferProtocol.
type Libp2pCARServer struct {
	ctx    context.Context
	cancel context.CancelFunc
	h      host.Host

	cs *store.CARStore
}

func New(h host.Host, cs *store.CARStore) *Libp2pCARServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Libp2pCARServer{
		ctx:    ctx,
		cancel: cancel,
		h:      h,
		cs:     cs,
	}
}

func (l *Libp2pCARServer) Start() {
	l.h.SetStreamHandler(protocol.ID(CARTransferProtocol), l.serveCARFile)
}

func (l *Libp2pCARServer) Stop() {
	l.cancel()
	l.h.RemoveStreamHandler(protocol.ID(CARTransferProtocol))
}

func (l *Libp2pCARServer) serveCARFile(s network.Stream) {
	defer s.Close()

	// Set a deadline on reading from the stream so it does NOT hang
	_ = s.SetReadDeadline(time.Now().Add(readDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	reqBz, reqErr := io.ReadAll(io.LimitReader(s, network.MessageSizeMax))
	if reqErr != nil {
		return
	}

	// read the json car transfer request
	var req CARTransferRequest
	if err := json.Unmarshal(reqBz, &req); err != nil {
		return
	}
	dr, drErr := carRequestToDAGRequest(&req)
	if drErr != nil {
		return
	}
	log.Debugw("car transfer request", "base64-root", req.Root, "base64-selector", req.Selector)

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(writeDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	if err := l.cs.WithStore(dr.root, func(ro bstore.Blockstore) error {
		ls := cidlink.DefaultLinkSystem()
		bsa := bsadapter.Adapter{Wrapped: ro}
		ls.SetReadStorage(&bsa)

		bf := bufio.NewWriter(s)
		defer bf.Flush()

		if _, err := car.TraverseV1(l.ctx, &ls, dr.root, dr.selector, bf); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Errorw("car transfer failed", "base64-root", req.Root, "base64-selector", req.Selector, "err", err)
	}
	log.Debugw("car transfer successful", "base64-root", req.Root, "base64-selector", req.Selector)

	// TODO record sent bytes
}
