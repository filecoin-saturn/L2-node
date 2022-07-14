package carserver

import (
	"bytes"
	"encoding/base64"
	"fmt"

	"github.com/google/uuid"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

// CARTransferRequest is the request sent by the client to transfer a CAR file
// for the given root and selector.
type CARTransferRequest struct {
	ReqId      string
	Root       string // base64 encoded byte array
	Selector   string // base 64 encoded byte array
	SkipOffset uint64
}

type dagTraversalRequest struct {
	reqId    uuid.UUID
	root     cid.Cid
	selector ipld.Node
	skip     uint64
}

func carRequestToDAGRequest(req *CARTransferRequest) (*dagTraversalRequest, error) {
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

	reqId, err := uuid.Parse(req.ReqId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse uuid: %w", err)
	}

	return &dagTraversalRequest{
		reqId:    reqId,
		root:     rootcid,
		selector: sel,
		skip:     req.SkipOffset,
	}, nil
}

func decodeSelector(sel []byte) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(sel)); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}
