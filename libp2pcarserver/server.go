package libp2pcarserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/libp2p/go-libp2p-core/host"

	"github.com/ipld/go-car/v2/blockstore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"

	"github.com/ipld/go-car/v2"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
)

const CarTransferProtocol = "/saturn/l2/car/1.0" // car transfer protocol

var (
	maxRequestSize = 1048576 // 1 MiB - max size of the CAR transfer request
	readDeadline   = 10 * time.Second
	writeDeadline  = 30 * time.Minute
)

// CarTransferRequest is the request sent by the client to transfer a CAR file
// for the given root and selector
type CarTransferRequest struct {
	Root     string // base64 encoded byte array
	Selector string // base 64 encoded byte array
}

type dagTraversalRequest struct {
	root     cid.Cid
	selector ipld.Node
}

// Libp2pCARServer serves CAR files for a given root and selector over the libp2p CarTransferProtocol.
type Libp2pCARServer struct {
	ctx    context.Context
	cancel context.CancelFunc
	h      host.Host
}

func New(h host.Host) *Libp2pCARServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Libp2pCARServer{
		ctx:    ctx,
		cancel: cancel,
		h:      h,
	}
}

func (l *Libp2pCARServer) Start() {
	l.h.SetStreamHandler(protocol.ID(CarTransferProtocol), l.Serve)
}

func (l *Libp2pCARServer) Stop() {
	l.cancel()
	l.h.RemoveStreamHandler(protocol.ID(CarTransferProtocol))
}

func (l *Libp2pCARServer) Serve(s network.Stream) {
	defer s.Close()

	// Set a deadline on reading from the stream so it does NOT hang
	_ = s.SetReadDeadline(time.Now().Add(readDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	reqBz, err := ioutil.ReadAll(io.LimitReader(s, network.MessageSizeMax))
	if err != nil {
		return
	}
	// read the json car transfer request
	var req CarTransferRequest
	if err := json.Unmarshal(reqBz, &req); err != nil {
		return
	}
	dr, err := carRequestToDAGRequest(&req)
	if err != nil {
		return
	}

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(writeDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// TODO Map root to CAR files
	from, err := blockstore.OpenReadOnly("../testdata/files/sample-v1.car")
	if err != nil {
		return
	}
	ls := cidlink.DefaultLinkSystem()
	bsa := bsadapter.Adapter{Wrapped: from}
	ls.SetReadStorage(&bsa)

	bf := bufio.NewWriter(s)
	defer bf.Flush()

	_, err = car.TraverseV1(l.ctx, &ls, dr.root, dr.selector, bf)
	if err != nil {
		return
	}
	// TODO record sent bytes
}

func carRequestToDAGRequest(req *CarTransferRequest) (*dagTraversalRequest, error) {
	rootbz, err := base64.StdEncoding.DecodeString(req.Root)
	if err != nil {
		return nil, fmt.Errorf("failed to decode root: %s", err)
	}
	rootcid, err := cid.Cast(rootbz)
	if err != nil {
		return nil, fmt.Errorf("failed to cast root to cid: %s", err)
	}

	selbz, err := base64.StdEncoding.DecodeString(req.Selector)
	if err != nil {
		return nil, fmt.Errorf("failed to decode selector: %s", err)
	}
	sel, err := decodeSelector(selbz)
	if err != nil {
		return nil, fmt.Errorf("failed to decode selector to ipld node: %s", err)
	}

	return &dagTraversalRequest{
		root:     rootcid,
		selector: sel,
	}, nil
}

func decodeSelector(sel []byte) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(sel)); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}
