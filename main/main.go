package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/saturn-l2/station"

	"github.com/filecoin-project/saturn-l2/carserver"
	"github.com/filecoin-project/saturn-l2/carstore"
	"github.com/filecoin-project/saturn-l2/logs"
	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"

	address "github.com/filecoin-project/go-address"
	"github.com/gorilla/mux"

	"github.com/filecoin-project/saturn-l2/resources"
)

const (
	PORT_ENV_VAR          = "PORT"
	FIL_ADDRESS_VAR       = "FIL_WALLET_ADDRESS"
	MAX_DISK_SPACE_VAR    = "MAX_DISK_SPACE"
	IPFS_GATEWAY_BASE_URL = "https://ipfs.io/api/v0/dag/export"
	ROOT_DIR_ENV_VAR      = "ROOT_DIR"
)

var (
	defaultMaxSize = uint64(200 * 1073741824) // 200 Gib
)

type config struct {
	Port         int
	FilAddr      string `json:"fil_wallet_address"`
	MaxDiskSpace uint64
	RootDir      string
}

func main() {
	logging.SetAllLoggers(logging.LevelInfo)

	ctx := context.Background()
	cfg, err := mkConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build config: %s", err.Error())
		os.Exit(2)
	}

	cfgJson, err := json.Marshal(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to serialise config: %s\n", err.Error())
		os.Exit(2)
	}

	carserver, err := buildCarServer(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build http car server: %s", err.Error())
		os.Exit(2)
	}
	if err := carserver.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start car server: %s", err.Error())
		os.Exit(2)
	}
	defer carserver.Stop(ctx)

	m := mux.NewRouter()
	m.PathPrefix("/config").Handler(http.HandlerFunc(configHandler(cfgJson)))
	m.PathPrefix("/webui").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webuiHandler(cfg, w, r)
	}))

	carServerHandler := http.TimeoutHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		carserver.server.ServeCARFile(w, r)
	}), 10*time.Minute, "timed out")
	m.PathPrefix("/dag/car").Handler(carServerHandler)

	m.PathPrefix("/stats").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ss, err := carserver.sapi.AllStats(r.Context())
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		bz, err := json.Marshal(ss)
		if err != nil {
			http.Error(w, "failed to marshal stats to json", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bz)
	}))

	srv := &http.Server{
		Handler: m,
	}

	nl, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.Port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot start the webserver: %s\n", err.Error())
		os.Exit(4)
	}

	go func() {
		if err := srv.Serve(nl); err != http.ErrServerClosed {
			panic(err)
		}
	}()

	port := nl.Addr().(*net.TCPAddr).Port
	fmt.Println("Server listening on", nl.Addr())
	fmt.Printf("WebUI: http://localhost:%d/webui\n", port)
	for {

	}
}

func mkConfig() (config, error) {
	// parse port
	var port int
	portStr := os.Getenv(PORT_ENV_VAR)
	if portStr == "" {
		port = 0
	} else {
		var err error
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return config{}, fmt.Errorf("failed to parse port value %s: %w", portStr, err)
		}
	}

	// parse FIL address
	filAddr := os.Getenv(FIL_ADDRESS_VAR)
	if filAddr == "" {
		return config{}, errors.New("No FIL_WALLET_ADDRESS provided. Please set the environment variable.\n")
	}
	if _, err := address.NewFromString(filAddr); err != nil {
		return config{}, fmt.Errorf("Invalid FIL_WALLET_ADDRESS format: %w", err)
	}

	// parse max disk space
	var maxDiskSpace uint64
	maxDiskSpaceStr := os.Getenv(MAX_DISK_SPACE_VAR)
	if maxDiskSpaceStr == "" {
		maxDiskSpace = defaultMaxSize
	} else {
		var err error
		maxDiskSpace, err = strconv.ParseUint(maxDiskSpaceStr, 10, 64)
		if err != nil {
			return config{}, fmt.Errorf("failed to parse max disk size %s: %w", maxDiskSpaceStr, err)
		}
		if maxDiskSpace < defaultMaxSize {
			return config{}, errors.New("max allocated disk space should be atleast 200GiB")
		}
	}

	rootDirStr := os.Getenv(ROOT_DIR_ENV_VAR)
	if rootDirStr == "" {
		return config{}, errors.New("No ROOT_DIR provided. Please set the environment variable.")
	}

	return config{
		Port:         port,
		FilAddr:      filAddr,
		MaxDiskSpace: maxDiskSpace,
		RootDir:      rootDirStr,
	}, nil
}

type CARServer struct {
	server *carserver.HTTPCARServer
	store  *carstore.CarStore
	sapi   station.StationAPI
}

func (cs *CARServer) Start(ctx context.Context) error {
	return cs.store.Start(ctx)
}

func (cs *CARServer) Stop(ctx context.Context) error {
	return cs.store.Close()
}

func buildCarServer(cfg config) (*CARServer, error) {
	logger := logs.NewSaturnLogger()
	dss, err := newDatastore(filepath.Join(cfg.RootDir, "statestore"))
	if err != nil {
		return nil, fmt.Errorf("failed to create state datastore: %w", err)
	}

	sapi := carserver.NewStationAPIImpl(dss, nil)
	gwApi := carstore.NewGatewayAPI(IPFS_GATEWAY_BASE_URL, sapi)
	carStoreConfig := carstore.Config{
		MaxCARFilesDiskSpace: int64(cfg.MaxDiskSpace),
	}
	store, err := carstore.New(cfg.RootDir, gwApi, carStoreConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create car store: %w", err)
	}
	sapi.SetStorageStatsFetcher(store)

	cs := carserver.New(store, logger, sapi)
	return &CARServer{
		server: cs,
		store:  store,
		sapi:   sapi,
	}, nil
}

func webuiHandler(cfg config, w http.ResponseWriter, r *http.Request) {
	rootDir := "webui"
	path := strings.TrimPrefix(r.URL.Path, "/")

	if path == rootDir {
		targetUrl := fmt.Sprintf("/%s/address/%s", rootDir, cfg.FilAddr)
		statusCode := 303 // See Other (a temporary redirect)
		http.Redirect(w, r, targetUrl, statusCode)
		return
	}

	_, err := resources.WebUI.Open(path)
	if path == rootDir || os.IsNotExist(err) {
		// file does not exist, serve index.html
		index, err := resources.WebUI.ReadFile(rootDir + "/index.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(index)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// otherwise, use http.FileServer to serve the static dir
	http.FileServer(http.FS(resources.WebUI)).ServeHTTP(w, r)
}

func configHandler(conf []byte) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(conf)
	}
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
