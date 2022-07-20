package types

import (
	"bytes"
	"encoding/base64"
	"fmt"

	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"

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

func (c *CARTransferRequest) ToDAGRequest() (*DagTraversalRequest, error) {
	rootbz, err := base64.StdEncoding.DecodeString(c.Root)
	if err != nil {
		return nil, fmt.Errorf("failed to decode root: %s", err)
	}
	rootcid, err := cid.Cast(rootbz)
	if err != nil {
		return nil, fmt.Errorf("failed to cast root to cid: %s", err)
	}

	var sel ipld.Node
	if c.Selector != "" {
		selbz, err := base64.StdEncoding.DecodeString(c.Selector)
		if err != nil {
			return nil, fmt.Errorf("failed to decode selector: %s", err)
		}
		sel, err = decodeSelector(selbz)
		if err != nil {
			return nil, fmt.Errorf("failed to decode selector to ipld node: %s", err)
		}
	} else {
		// use the default "select all" selector.
		sel = selectorparse.CommonSelector_ExploreAllRecursively
	}

	reqId, err := uuid.Parse(c.ReqId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse uuid: %w", err)
	}

	return &DagTraversalRequest{
		ReqId:    reqId,
		Root:     rootcid,
		Selector: sel,
		Skip:     c.SkipOffset,
	}, nil
}

type DagTraversalRequest struct {
	ReqId    uuid.UUID
	Root     cid.Cid
	Selector ipld.Node
	Skip     uint64
}

func decodeSelector(sel []byte) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(sel)); err != nil {
		return nil, err
	}
	return nb.Build(), nil
}
