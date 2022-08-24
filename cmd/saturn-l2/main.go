package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

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
	// PORT_ENV_VAR is the environment variable that determines the port the saturn L2 service will bind to.
	// If this environment variable is not configured, this service will bind to any available port.
	PORT_ENV_VAR = "PORT"

	// ROOT_DIR_ENV_VAR is the environment variable that determines the root directory of the Saturn L2 Node.
	// All persistent state and cached CAR files will be persisted under this directory.
	// Mandatory environment variable -> no default for now.
	ROOT_DIR_ENV_VAR = "ROOT_DIR"

	// MAX_DISK_SPACE_VAR configures the environment variable that determines the maximum disk space the L2 node can use to
	// store cached CAR files. If this env variable is not configured, it defaults to 200GiB.
	MAX_DISK_SPACE_VAR = "MAX_L2_DISK_SPACE"

	FIL_ADDRESS_VAR = "FIL_WALLET_ADDRESS"
)

var (
	gateway_base_url = "https://ipfs.io/api/v0/dag/export"
	defaultMaxSize   = uint64(200 * 1073741824) // 200 Gib
	idFile           = ".l2Id"
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

	// generate L2 UUID if this is the first run
	id, err := readL2IdIfExists(cfg.RootDir)
	if err != nil {
		path := idFilePath(cfg.RootDir)
		_ = os.Remove(path)
		id = uuid.New()
		if err := ioutil.WriteFile(path, []byte(id.String()), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write L2 Id to file: %s", err.Error())
			os.Exit(2)
		}
	}
	fmt.Println("\n L2 Node Id is ", id.String())

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

	port := nl.Addr().(*net.TCPAddr).Port
	fmt.Println("Server listening on", nl.Addr())
	fmt.Printf("WebUI: http://localhost:%d/webui\n", port)

	if err := srv.Serve(nl); err != http.ErrServerClosed {
		panic(err)
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
		return config{}, fmt.Errorf("No %s provided. Please set the environment variable.\n", FIL_ADDRESS_VAR)
	}
	if _, err := address.NewFromString(filAddr); err != nil {
		return config{}, fmt.Errorf("Invalid %s format: %w", FIL_ADDRESS_VAR, err)
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

	rootDirStr, err := getRootDir()
	if err != nil {
		return config{}, err
	}

	if _, err := os.Stat(rootDirStr); err != nil {
		return config{}, fmt.Errorf("root dir %s does NOT exist", rootDirStr)
	}

	fmt.Printf("Using root dir %s\n", rootDirStr)

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
	gwApi := carstore.NewGatewayAPI(gateway_base_url, sapi)
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

func getRootDir() (string, error) {
	if dir := os.Getenv(ROOT_DIR_ENV_VAR); dir != "" {
		abs, _ := filepath.Abs(dir)
		return abs, nil
	}

	if runtime.GOOS == "windows" {
		if localAppData := os.Getenv("LOCALAPPDATA"); localAppData != "" {
			return localAppData + "/saturn", nil
		}

		return "", errors.New("invalid Windows environment: LOCALAPPDATA is not set")
	}

	if home := os.Getenv("HOME"); home != "" {
		return home + "/.saturn", nil
	}

	return "", errors.New("invalid environment: HOME is not set")
}

// returns the l2 id by reading it from the id file if it exists, otherwise returns error.
func readL2IdIfExists(root string) (uuid.UUID, error) {
	path := idFilePath(root)
	_, err := os.Stat(path)
	if err != nil {
		return uuid.UUID{}, err
	}

	bz, err := ioutil.ReadFile(path)
	if err != nil {
		return uuid.UUID{}, err
	}

	u, err := uuid.Parse(string(bz))
	if err != nil {
		return uuid.UUID{}, err
	}

	return u, nil
}

func idFilePath(rootDir string) string {
	return filepath.Join(rootDir, idFile)
}
