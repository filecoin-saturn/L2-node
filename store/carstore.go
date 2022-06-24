package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2/blockstore"
)

type CarRef = cid.Cid

type CARStore struct {
	writing map[CarRef]*blockstore.ReadWrite
	open    map[CarRef]*blockstore.ReadOnly

	rootdir string
	lk      sync.Mutex
}

// Create a new CAR store roooted at the given directory.
func NewCARStore(rootdir string) (*CARStore, error) {
	if err := os.MkdirAll(filepath.Join(rootdir, "temp"), 0755); err != nil {
		return nil, err
	}

	return &CARStore{
		rootdir: rootdir,

		writing: make(map[CarRef]*blockstore.ReadWrite),
		open:    make(map[CarRef]*blockstore.ReadOnly),
	}, nil
}

func (c *CARStore) Create(id CarRef, payloadCid cid.Cid, writer func(bstore.Blockstore) error) error {
	c.lk.Lock()
	defer c.lk.Unlock()

	// check if fully-written is already open
	_, ok := c.open[id]
	if ok {
		return nil
	}

	// check if another write to the same CarRef is ongoing
	_, ok = c.writing[id]
	if ok {
		return nil
	}

	// check if fully written already exists
	if _, err := os.Stat(c.pathFor(false, id)); !os.IsNotExist(err) {
		return err
	}

	if _, err := os.Stat(c.pathFor(true, id)); err == nil {
		// if old temp store exists, but is not in c.writing it's possibly left from a previous run; clean up

		if err := os.Remove(c.pathFor(true, id)); err != nil {
			return fmt.Errorf("cleaning up old temp store: %w", err)
		}
	}

	path := c.pathFor(true, id)
	bs, bsErr := blockstore.OpenReadWrite(path, []cid.Cid{payloadCid}, blockstore.UseWholeCIDs(true))
	if bsErr != nil {
		return fmt.Errorf("opening new rw store: %w", bsErr)
	}

	c.lk.Unlock()
	bsErr = writer(bs)
	ferr := bs.Finalize()
	c.lk.Lock()

	if ferr != nil {
		return fmt.Errorf("finalize store: %w", ferr)
	}

	if bsErr != nil {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("cleaning up old temp store: %w", err)
		}
		return fmt.Errorf("calling car writer: %w", bsErr)
	}

	delete(c.writing, id)

	if err := os.Rename(path, c.pathFor(false, id)); err != nil {
		return fmt.Errorf("move temp to complete store: %w", err)
	}

	return nil
}

func (c *CARStore) Drop(id CarRef) error {
	return os.RemoveAll(c.PathFor(id))
}

func (c *CARStore) pathFor(temp bool, id CarRef) string {
	if temp {
		return filepath.Join(c.rootdir, "temp", fmt.Sprintf("temp%s.car", id))
	}

	return filepath.Join(c.rootdir, fmt.Sprintf("%s.car", id))
}

func (c *CARStore) PathFor(id CarRef) string {
	return c.pathFor(false, id)
}

func (c *CARStore) WithStore(id CarRef, writer func(bstore.Blockstore) error) error {
	c.lk.Lock()

	// check if fully-written is already open
	_, ok := c.open[id]
	if !ok {
		ro, err := blockstore.OpenReadOnly(c.pathFor(false, id))
		if err != nil {
			c.lk.Unlock()
			return fmt.Errorf("open read only store: %w", err)
		}

		c.open[id] = ro
	}

	ro := c.open[id]
	c.lk.Unlock()

	return writer(ro)
}
