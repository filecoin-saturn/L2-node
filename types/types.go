package types

import (
	"fmt"

	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"

	"github.com/google/uuid"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
)

// CARTransferRequest is the request sent by the client to transfer a CAR file
// for the given root and selector.
type CARTransferRequest struct {
	RequestId  string
	Root       string
	SkipOffset uint64
}

func (c *CARTransferRequest) ToDAGRequest() (*DagTraversalRequest, error) {
	rootCid, err := cid.Decode(c.Root)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cid: %w", err)
	}

	// use the default "select all" selector for now.
	sel := selectorparse.CommonSelector_ExploreAllRecursively

	reqId, err := uuid.Parse(c.RequestId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse uuid: %w", err)
	}

	return &DagTraversalRequest{
		RequestId: reqId,
		Root:      rootCid,
		Selector:  sel,
	}, nil
}

type DagTraversalRequest struct {
	RequestId uuid.UUID
	Root      cid.Cid
	Selector  ipld.Node
}
