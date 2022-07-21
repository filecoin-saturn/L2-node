package carstore

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
)

// provides a writeable blockstore wrapper over the dagstore blockstore
type blockstore struct {
	dagstore.ReadBlockstore
}

func (b *blockstore) DeleteBlock(context.Context, cid.Cid) error {
	return fmt.Errorf("DeleteBlock called but not implemented")
}
func (b *blockstore) Put(context.Context, blocks.Block) error {
	return fmt.Errorf("Put called but not implemented")
}
func (b *blockstore) PutMany(context.Context, []blocks.Block) error {
	return fmt.Errorf("PutMany called but not implemented")
}

var _ bstore.Blockstore = (*blockstore)(nil)
