package carstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"

	"github.com/filecoin-project/dagstore/helpers"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"

	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/filecoin-project/dagstore/index"
	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"

	logging "github.com/ipfs/go-log/v2"

	"github.com/whyrusleeping/timecache"

	"github.com/filecoin-project/dagstore"
	cid "github.com/ipfs/go-cid"
)

var log = logging.Logger("l2-cache")

var (
	maxConcurrentIndex        = 5
	maxConcurrentReadyFetches = 5
	secondHitDuration         = 24 * time.Hour
	maxRecoverAttempts        = uint64(1)
)

var (
	gwScheme = "gateway"

	ErrNotFound = errors.New("not found")
)

type PersistentCache struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu sync.Mutex
	tc *timecache.TimeCache

	dagst     *dagstore.DAGStore
	failureCh chan dagstore.ShardResult
	traceCh   chan dagstore.Trace

	gwAPI GatewayAPI
}

func New(rootDir string, gwAPI GatewayAPI) (*PersistentCache, error) {
	// construct the DAG Store.
	registry := mount.NewRegistry()
	if err := registry.Register(gwScheme, mountTemplate(gwAPI)); err != nil {
		return nil, fmt.Errorf("failed to create registry: %w", err)
	}

	var (
		transientsDir = filepath.Join(rootDir, "transients")
		datastoreDir  = filepath.Join(rootDir, "datastore")
		indexDir      = filepath.Join(rootDir, "index")
	)

	dstore, err := newDatastore(datastoreDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create datastore in %s: %w", datastoreDir, err)
	}
	irepo, err := index.NewFSRepo(indexDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise dagstore index repo: %w", err)
	}
	topIndex := index.NewInverted(dstore)

	// The dagstore will write Shard failures to the `failureCh` here.
	failureCh := make(chan dagstore.ShardResult, 1)
	// The dagstore will write Trace events to the `traceCh` here.
	traceCh := make(chan dagstore.Trace, 32)

	dcfg := dagstore.Config{
		TransientsDir:             transientsDir,
		IndexRepo:                 irepo,
		Datastore:                 dstore,
		MountRegistry:             registry,
		FailureCh:                 failureCh,
		TraceCh:                   traceCh,
		TopLevelIndex:             topIndex,
		MaxConcurrentIndex:        maxConcurrentIndex,
		MaxConcurrentReadyFetches: maxConcurrentReadyFetches,
		RecoverOnStart:            dagstore.RecoverOnAcquire,
	}

	dagst, err := dagstore.NewDAGStore(dcfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create dagstore for cache: %w", err)
	}

	return &PersistentCache{
		tc:        timecache.NewTimeCache(secondHitDuration),
		dagst:     dagst,
		traceCh:   traceCh,
		failureCh: failureCh,
		gwAPI:     gwAPI,
	}, nil
}

func (l *PersistentCache) Start(ctx context.Context) error {
	l.ctx, l.cancel = context.WithCancel(ctx)

	// run a go-routine to read the trace for debugging.
	l.wg.Add(1)
	go dagstore.LogTraceLoop(l.ctx, l.traceCh, l.wg.Done)

	l.wg.Add(1)
	go dagstore.RecoverImmediately(l.ctx, l.dagst, l.failureCh, maxRecoverAttempts, l.wg.Done)

	return l.dagst.Start(ctx)
}

func (l *PersistentCache) Has(root cid.Cid) (bool, error) {
	ks, err := l.dagst.ShardsContainingMultihash(l.ctx, root.Hash())
	return len(ks) != 0, err
}

func (l *PersistentCache) Close() error {
	// Cancel the context
	l.cancel()

	// Close the DAG store
	if err := l.dagst.Close(); err != nil {
		return fmt.Errorf("failed to close dagstore: %w", err)
	}
	log.Info("dagstore closed")

	// Wait for the background go routine to exit
	log.Info("waiting for dagstore background  goroutines to exit")
	l.wg.Wait()

	log.Info("finished shutting down L2 cache")
	return nil
}

func (l *PersistentCache) FetchAndWriteCAR(root cid.Cid, writer func(bstore.Blockstore) error) error {
	mh := root.Hash()
	sks, err := l.dagst.ShardsContainingMultihash(l.ctx, mh)
	if err != nil && !errors.Is(err, ds.ErrNotFound) {
		return fmt.Errorf("failed to lookup dagstore containing the given multihash: %w", err)
	}

	if err == nil && len(sks) != 0 {
		// we have the requested CAR
		sa, err := helpers.AcquireShardSync(l.ctx, l.dagst, shard.KeyFromCIDMultihash(root))
		if err != nil {
			return fmt.Errorf("failed to get blockstore: %w", err)
		}
		defer sa.Close()

		bs, err := sa.Blockstore()
		if err != nil {
			return fmt.Errorf("failed to get blockstore: %w", err)
		}

		return writer(&blockstore{bs})
	}

	// we don't have the requested CAR -> apply "cache on second hit" rule
	l.mu.Lock()
	has := l.tc.Has(mh.String())
	l.mu.Unlock()
	if has {
		// second-hit -> fetch the content
		mnt := &GatewayMount{RootCID: root, API: l.gwAPI}
		if err := l.dagst.RegisterShard(l.ctx, shard.KeyFromCIDMultihash(root), mnt, nil, dagstore.RegisterOpts{}); err != nil {
			log.Errorw("failed to register shard", "root", root, "err", err)
		}
	} else {
		l.mu.Lock()
		l.tc.Add(mh.String())
		l.mu.Unlock()
	}

	return ErrNotFound
}

// newDatastore creates a datastore under the given base directory
// for dagstore metadata.
func newDatastore(dir string) (ds.Batching, error) {
	// Create the datastore directory if it doesn't exist yet.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create a new LevelDB datastore
	dstore, err := levelds.NewDatastore(dir, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open datastore: %w", err)
	}
	return dstore, nil
}

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
