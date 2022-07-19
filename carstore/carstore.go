package carstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/filecoin-project/saturn-l2/station"

	"github.com/google/uuid"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/saturn-l2/logs"

	"github.com/filecoin-project/dagstore/gc"

	"github.com/filecoin-project/dagstore/helpers"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"

	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/filecoin-project/dagstore/index"
	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/filecoin-project/dagstore"
	cid "github.com/ipfs/go-cid"

	cache "github.com/patrickmn/go-cache"
)

var (
	log                       = logging.Logger("car-store")
	maxConcurrentIndex        = 3
	maxConcurrentReadyFetches = 3
	secondMissDuration        = 24 * time.Hour
	maxRecoverAttempts        = uint64(1)
	defaultDownloadTimeout    = 20 * time.Minute
)

var (
	gwScheme = "gateway"
	// ErrNotFound indicates that we do not have the requested CAR file in the car store
	ErrNotFound = errors.New("CAR not found")
)

type Config struct {
	// Maximum size to allocate to the car files directory on disk.
	// defaults to 200 Gib
	MaxCARFilesDiskSpace int64
	DownloadTimeout      time.Duration
}

type CarStore struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dagst     *dagstore.DAGStore
	failureCh chan dagstore.ShardResult
	traceCh   chan dagstore.Trace
	gcCh      chan dagstore.AutomatedGCResult

	gwAPI  GatewayAPI
	logger *logs.SaturnLogger

	mu                 sync.Mutex
	cacheMissTimeCache *cache.Cache
	downloading        map[string]struct{}

	transientsDir   string
	downloadTimeout time.Duration
}

func New(rootDir string, gwAPI GatewayAPI, cfg Config, logger *logs.SaturnLogger) (*CarStore, error) {
	// construct the DAG Store.
	registry := mount.NewRegistry()
	if err := registry.Register(gwScheme, mountTemplate(gwAPI)); err != nil {
		return nil, fmt.Errorf("failed to create dagstore registry: %w", err)
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
	// dagstore will write the automated GC trace to this channel
	gcCh := make(chan dagstore.AutomatedGCResult, 1)

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
		AutomatedGCEnabled:        true,
		AutomatedGCConfig: &dagstore.AutomatedGCConfig{
			GarbeCollectionStrategy:   gc.NewLRUGarbageCollector(),
			MaxTransientDirSize:       cfg.MaxCARFilesDiskSpace,
			TransientsGCWatermarkHigh: 0.9,
			TransientsGCWatermarkLow:  0.7,
			AutomatedGCTraceCh:        gcCh,
		},
	}

	dagst, err := dagstore.NewDAGStore(dcfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create dagstore to use for the car-store: %w", err)
	}

	downloadTimeout := defaultDownloadTimeout
	if cfg.DownloadTimeout != time.Duration(0) {
		downloadTimeout = cfg.DownloadTimeout
	}

	return &CarStore{
		cacheMissTimeCache: cache.New(secondMissDuration, 5*time.Minute),
		downloading:        make(map[string]struct{}),
		dagst:              dagst,
		traceCh:            traceCh,
		failureCh:          failureCh,
		gwAPI:              gwAPI,
		gcCh:               gcCh,
		logger:             logger.Subsystem("car-store"),
		transientsDir:      transientsDir,
		downloadTimeout:    downloadTimeout,
	}, nil
}

func (cs *CarStore) Start(ctx context.Context) error {
	log.Info("starting car store")
	cs.ctx, cs.cancel = context.WithCancel(ctx)

	cs.wg.Add(1)
	go dagstore.RecoverImmediately(cs.ctx, cs.dagst, cs.failureCh, maxRecoverAttempts, cs.wg.Done)

	cs.wg.Add(1)
	go cs.gcTraceLoop()

	cs.wg.Add(1)
	go cs.traceLoop()

	err := cs.dagst.Start(ctx)
	if err == nil {
		log.Info("successfully started car store")
	}
	return err
}

func (cs *CarStore) IsIndexed(ctx context.Context, root cid.Cid) (bool, error) {
	sks, err := cs.dagst.ShardsContainingMultihash(ctx, root.Hash())
	return len(sks) != 0, err
}

func (cs *CarStore) traceLoop() {
	defer cs.wg.Done()

	for {
		select {
		// Log trace events from the DAG store
		case tr := <-cs.traceCh:
			log.Debugw("trace",
				"shard-key", tr.Key.String(),
				"op-type", tr.Op.String(),
				"after", tr.After.String())

		case <-cs.ctx.Done():
			return
		}
	}
}

func (cs *CarStore) gcTraceLoop() {
	defer cs.wg.Done()
	for {
		select {
		case res := <-cs.gcCh:
			log.Debugw("shard reclaimed by automated gc", "shard", res.ReclaimedShard,
				"disk-size-after-reclaim", res.TransientsDirSizeAfterReclaim, "disk-size-before-reclaim", res.TransientsDirSizeBeforeReclaim)
		case <-cs.ctx.Done():
			return
		}
	}
}

func (cs *CarStore) Close() error {
	log.Info("shutting down the carstore")
	// Cancel the context
	cs.cancel()

	// Close the DAG store
	if err := cs.dagst.Close(); err != nil {
		return fmt.Errorf("failed to close the dagstore: %w", err)
	}
	log.Info("dagstore closed")

	// Wait for the background go routine to exit
	log.Info("waiting for carstore background goroutines to exit")
	cs.wg.Wait()

	log.Info("successfully shut down the carstore")
	return nil
}

func (cs *CarStore) FetchAndWriteCAR(reqID uuid.UUID, root cid.Cid, writer func(bstore.Blockstore) error) error {
	cs.logger.Infow(reqID, "got CAR request for root", "root", root.String())
	mh := root.Hash()

	// which dagstore shards have the requested root cid ?
	sks, err := cs.dagst.ShardsContainingMultihash(cs.ctx, mh)
	if err != nil && !errors.Is(err, ds.ErrNotFound) {
		cs.logger.LogError(reqID, "failed to lookup dagstore for shards containing the given multihash", err)
		return fmt.Errorf("failed to lookup dagstore for the given multihash: %w", err)
	}

	if err == nil && len(sks) != 0 {
		var sa *dagstore.ShardAccessor

		// among all the shards that have the requested root, select the first shard that we already have the CAR for locally.
		// If we don't have the CAR locally for any of the requested shards, we will simply return NO here and
		// asynchronously download the CAR from the origin server using the "cache on second miss" rule.
		for _, sk := range sks {
			sa, err = helpers.AcquireShardSync(cs.ctx, cs.dagst, sk, dagstore.AcquireOpts{
				NoDownload: true,
			})
			if err == nil {
				break
			}
		}

		// if we weren't able to acquire the shard using an already existing CAR file -> execute the cache on second miss rule
		// and return not found here.
		if sa == nil {
			cs.logger.Infow(reqID, "failed to acquire shard with nodownload=true, will execute the cache miss code", "err", err)
			cs.executeCacheMiss(reqID, root)
			return ErrNotFound
		}
		defer sa.Close()
		cs.logger.Infow(reqID, "acquired shard with nodownload=true")

		bs, err := sa.Blockstore()
		if err != nil {
			cs.logger.LogError(reqID, "failed to get blockstore for acquired shard", err)
			return fmt.Errorf("failed to get blockstore for shard: %w", err)
		}
		cs.logger.Infow(reqID, "acquired blockstore for shard")

		return writer(&blockstore{bs})
	}

	// we don't have the requested CAR -> apply "cache on second miss" rule
	cs.executeCacheMiss(reqID, root)

	cs.logger.Infow(reqID, "returning not found for requested root")
	return ErrNotFound
}

func (cs *CarStore) executeCacheMiss(reqID uuid.UUID, root cid.Cid) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	mhkey := root.Hash().String()

	_, found := cs.cacheMissTimeCache.Get(mhkey)
	// add the key to our cache miss timecache no matter what
	// if the key already exists in the timecache -> this will simply give a bump to it's longevity in the time cache
	cs.cacheMissTimeCache.Add(mhkey, struct{}{}, cache.DefaultExpiration)
	// if this is the very first cache miss for this key, there's nothing to do here.
	if !found {
		cs.logger.Infow(reqID, "first cache miss for given root, not downloading it")
		return
	}
	cs.logger.Infow(reqID, "more than one cache miss for given root, downloading and caching it")

	// if we're in the process of downloading and caching the key -> there's nothing to do here.
	if _, ok := cs.downloading[mhkey]; ok {
		cs.logger.Infow(reqID, "download already in progress for given root, returning")
		return
	}

	// if we have seen a cache miss for this key before and we're not already downloading and caching it -> do it !
	mnt := &GatewayMount{RootCID: root, API: cs.gwAPI}
	cs.downloading[mhkey] = struct{}{}

	go func(mhkey string) {
		ctx, cancel := context.WithDeadline(cs.ctx, time.Now().Add(cs.downloadTimeout))
		defer cancel()
		sa, err := helpers.RegisterAndAcquireSync(ctx, cs.dagst, keyFromCIDMultihash(root), mnt, dagstore.RegisterOpts{}, dagstore.AcquireOpts{})
		if err == nil {
			cs.logger.Infow(reqID, "successfully downloaded and cached given root")
			sa.Close()
		} else {
			cs.logger.LogError(reqID, "failed to register/acquire shard", err)
		}

		cs.mu.Lock()
		delete(cs.downloading, mhkey)
		cs.mu.Unlock()
	}(mhkey)
}

func (cs *CarStore) Stat() (station.StorageStats, error) {
	var out station.StorageStats

	err := filepath.WalkDir(cs.transientsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		fi, err := d.Info()
		if err != nil {
			return err
		}
		out.Bytes += uint64(fi.Size())
		return nil
	})

	return out, err
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

func keyFromCIDMultihash(c cid.Cid) shard.Key {
	return shard.KeyFromString(c.Hash().String())
}
