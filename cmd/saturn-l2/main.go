package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/filecoin-project/saturn-l2/l1interop"

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

type L1IPAddrs []string

var log = logging.Logger("saturn-l2")

const (
	// PORT_ENV_VAR is the environment variable that determines the port the saturn L2 service will bind to.
	// If this environment variable is not configured, this service will bind to any available port.
	PORT_ENV_VAR = "PORT"

	// ROOT_DIR_ENV_VAR is the environment variable that determines the root directory of the Saturn L2 Node.
	// All persistent state and cached CAR files will be persisted under this directory. Defaults to $HOME/.saturn.
	ROOT_DIR_ENV_VAR = "ROOT_DIR"

	// MAX_DISK_SPACE_VAR configures the environment variable that determines the maximum disk space the L2 node can use to
	// store cached CAR files. If this env variable is not configured, it defaults to 200GiB.
	MAX_DISK_SPACE_VAR = "MAX_L2_DISK_SPACE"

	FIL_ADDRESS_VAR = "FIL_WALLET_ADDRESS"

	// L1_DISCOVERY_URL_VAR configures the environment variable that determines the URL of the L1 Discovery API to invoke to
	// get back the L1 nodes this L2 node will connect and serve CAR files to.
	L1_DISCOVERY_URL_VAR = "L1_DISCOVERY_API_URL"

	// MAX_L1s_VAR configures the environment variable that determines the maximum
	// number of L1s this L2 will connect to and join the swarm for. Defaults to 100.
	MAX_L1s_VAR = "MAX_L1s"

	// MAX_CONCURRENT_L1_REQUESTS_VAR configures the environment variable that determines the maximum
	// number of CAR file requests that will be processed concurrently for a single L1. defaults to 3.
	MAX_CONCURRENT_L1_REQUESTS_VAR = "MAX_CONCURRENT_L1_REQUESTS"
)

var (
	gateway_base_url = "https://ipfs.io/api/v0/dag/export"

	defaultMaxDiskSpace = uint64(200 * 1073741824) // 200 Gib

	// file the L2 Node Id will be persisted to.
	idFile = ".l2Id"

	// 1 MiB
	maxL1DiscoveryResponseSize = int64(1048576)

	defaultMaxL1ConcurrentRequests = uint64(3)

	// default maximum of the number of L1s this L2 node will connect to
	defaultMaxL1s = uint64(100)

	// timeout of the request we make to discover L1s
	l1_discovery_timeout = 5 * time.Minute

	// number of maximum connections to a single L1
	maxConnsPerL1 = 5

	// we are okay having upto 500 long running idle connections with L1s
	totalMaxIdleL1Conns = 500

	// in-activity timeout before we close an idle connection to an L1
	idleL1ConnTimeout = 30 * time.Minute

	// DNS Hostname of Saturn L1 Nodes
	saturn_l1_hostName = "strn.pl"
)

type config struct {
	Port                    int
	FilAddr                 string `json:"fil_wallet_address"`
	MaxDiskSpace            uint64
	RootDir                 string
	L1DiscoveryAPIUrl       string
	MaxL1Connections        int
	MaxConcurrentL1Requests int
}

func main() {
	// build app context
	cleanup := func() {

	}
	defer cleanup()
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cleanup()
	}()

	logging.SetAllLoggers(logging.LevelInfo)
	ctx, cancel := context.WithCancel(context.Background())
	cleanup = updateCleanup(cleanup, func() {
		log.Info("shutting down all threads")
		cancel()
	})

	// build L2 config
	cfg, err := mkConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build config: %s", err.Error())
		os.Exit(2)
	}
	log.Infow("parsed config", "cfg", cfg)

	// generate L2 UUID if this is the first run
	l2Id, err := readL2IdIfExists(cfg.RootDir)
	if err != nil {
		l2Id, err = createAndPersistL2Id(cfg.RootDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write L2 Id to file: %s", err.Error())
			os.Exit(2)
		}
	}
	log.Infow("read l2 node Id", "l2Id", l2Id)

	cfgJson, err := json.Marshal(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to serialise config: %s\n", err.Error())
		os.Exit(2)
	}

	// get the Nearest L1s by talking to the orchestrator
	log.Info("waiting to discover L1s...")
	l1IPAddrs, err := getNearestL1s(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get nearest L1s to connect to: %s\n", err.Error())
		os.Exit(2)
	}
	log.Infow("discovered L1s", "l1 IP Addrs", strings.Join(l1IPAddrs, ", "))

	// build the saturn logger
	logger := logs.NewSaturnLogger()

	// build a robust http client to use to connect and serve CAR files to L1s
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = totalMaxIdleL1Conns
	tr.MaxConnsPerHost = maxConnsPerL1
	tr.MaxIdleConnsPerHost = maxConnsPerL1 // number of maximum idle connections to a single L1
	tr.IdleConnTimeout = idleL1ConnTimeout
	tr.TLSClientConfig.ServerName = saturn_l1_hostName
	l1HttpClient := &http.Client{
		Transport: tr,
	}

	// build and start the CAR server
	carserver, err := buildCarServer(cfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build car server: %s", err.Error())
		os.Exit(2)
	}
	if err := carserver.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start car server: %s", err.Error())
		os.Exit(2)
	}
	cleanup = updateCleanup(cleanup, func() {
		log.Info("shutting down the CAR server")
		carserver.Stop(ctx)
	})

	// Connect and register with all L1s and start serving their requests
	var l1wg sync.WaitGroup
	for _, l1ip := range l1IPAddrs {
		l1ip := l1ip
		l1client := l1interop.New(l2Id.String(), l1HttpClient, logger, carserver.server, l1ip, cfg.MaxConcurrentL1Requests)
		cleanup = updateCleanup(cleanup, func() {
			log.Infow("closing connection with l1", "l1", l1ip)
			l1client.Stop()
			log.Infow("finished closing connection with l1", "l1", l1ip)
		})
		l1wg.Add(1)
		go func() {
			defer l1wg.Done()
			if err := l1client.Start(); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Errorw("failed to connect to L1", "l1", l1ip, "err", err)
				}
			}
		}()
	}
	cleanup = updateCleanup(cleanup, func() {
		log.Info("waiting for all l1 connections to be torn down")
		l1wg.Wait()
		log.Info("finished tearing down all l1 connections")
	})

	m := mux.NewRouter()
	m.PathPrefix("/config").Handler(http.HandlerFunc(configHandler(cfgJson)))
	m.PathPrefix("/webui").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webuiHandler(cfg, w, r)
	}))

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
	cleanup = updateCleanup(cleanup, func() {
		log.Info("shutting down the http server")
		_ = srv.Close()
	})

	nl, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.Port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot start the webserver: %s\n", err.Error())
		os.Exit(4)
	}

	port := nl.Addr().(*net.TCPAddr).Port
	fmt.Println("Server listening on", nl.Addr())
	fmt.Printf("WebUI: http://localhost:%d/webui\n", port)

	if err := srv.Serve(nl); err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "error shutting down the server: %s", err.Error())
	}
}

func updateCleanup(oldFunc func(), newFunc func()) func() {
	return func() {
		oldFunc()
		newFunc()
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
	maxDiskSpace, err := readIntEnvVar(MAX_DISK_SPACE_VAR, defaultMaxDiskSpace)
	if err != nil {
		return config{}, fmt.Errorf("failed to parse max disk space env var: %w", err)
	}
	if maxDiskSpace < defaultMaxDiskSpace {
		return config{}, errors.New("max allocated disk space should be atleast 200GiB")
	}

	// parse root directory
	rootDirStr, err := getRootDir()
	if err != nil {
		return config{}, err
	}
	if _, err := os.Stat(rootDirStr); err != nil {
		return config{}, fmt.Errorf("root dir %s does NOT exist", rootDirStr)
	}
	fmt.Printf("Using root dir %s\n", rootDirStr)

	// parse L1 Discovery API URL
	durl, exists := os.LookupEnv(L1_DISCOVERY_URL_VAR)
	if !exists {
		return config{}, errors.New("please configure the L1_DISCOVERY_API_URL environment variable")
	}
	if _, err := url.Parse(durl); err != nil {
		return config{}, fmt.Errorf("l1 discovery api url is invalid, failed to parse, err=%w", err)
	}

	// parse max number of l1s to connect to
	maxL1Conns, err := readIntEnvVar(MAX_L1s_VAR, defaultMaxL1s)
	if err != nil {
		return config{}, fmt.Errorf("failed to parse MAX_L1_CONNECTIONS_VAR env var: %w", err)
	}

	// parse max number of concurrent L1 requests to serve
	maxConcurrentL1Reqs, err := readIntEnvVar(MAX_CONCURRENT_L1_REQUESTS_VAR, defaultMaxL1ConcurrentRequests)
	if err != nil {
		return config{}, fmt.Errorf("failed to parse MAX_CONCURRENT_L1_REQUESTS_VAR env var: %w", err)
	}

	return config{
		Port:                    port,
		FilAddr:                 filAddr,
		MaxDiskSpace:            maxDiskSpace,
		RootDir:                 rootDirStr,
		L1DiscoveryAPIUrl:       durl,
		MaxL1Connections:        int(maxL1Conns),
		MaxConcurrentL1Requests: int(maxConcurrentL1Reqs),
	}, nil
}

func getNearestL1s(ctx context.Context, cfg config) (L1IPAddrs, error) {
	client := &http.Client{
		Timeout: l1_discovery_timeout,
	}

	req, err := http.NewRequest(http.MethodGet, cfg.L1DiscoveryAPIUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request to L1 Discovery API")
	}
	req = req.WithContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call l1 discovery API: %w", err)
	}
	defer resp.Body.Close()

	rd := io.LimitReader(resp.Body, maxL1DiscoveryResponseSize)
	l1ips, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, fmt.Errorf("failed to read l1 discovery response: %w", err)
	}

	var l1IPAddrs []string
	if err := json.Unmarshal(l1ips, &l1IPAddrs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal l1 addresses: %w", err)
	}

	for _, s := range l1IPAddrs {
		if ip := net.ParseIP(s); ip == nil {
			return nil, fmt.Errorf("l1 IP returned by L1 Discovery API is invalid, ip=%s", ip)
		}
	}

	if cfg.MaxL1Connections < len(l1IPAddrs) {
		l1IPAddrs = l1IPAddrs[:cfg.MaxL1Connections]
	}

	return l1IPAddrs, nil
}

type CARServer struct {
	server *carserver.CarServer
	store  *carstore.CarStore
	sapi   station.StationAPI
}

func (cs *CARServer) Start(ctx context.Context) error {
	return cs.store.Start(ctx)
}

func (cs *CARServer) Stop(_ context.Context) error {
	return cs.store.Stop()
}

func buildCarServer(cfg config, logger *logs.SaturnLogger) (*CARServer, error) {
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

func createAndPersistL2Id(root string) (uuid.UUID, error) {
	path := idFilePath(root)
	_ = os.Remove(path)
	l2Id := uuid.New()
	if err := ioutil.WriteFile(path, []byte(l2Id.String()), 0644); err != nil {
		return uuid.UUID{}, err
	}
	return l2Id, nil
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

func readIntEnvVar(name string, defaultVal uint64) (uint64, error) {
	valStr := os.Getenv(name)
	if valStr == "" {
		return defaultVal, nil
	}

	val, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse environment variable  %s as integer: %w", valStr, err)
	}
	if val <= 0 {
		return 0, errors.New("integer environment variable must be positive")
	}

	return val, nil
}
