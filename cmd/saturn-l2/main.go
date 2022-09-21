package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	"github.com/jpillora/backoff"

	"go.uber.org/atomic"

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
	// Defaults to `defaultL1DiscoveryURL`.
	L1_DISCOVERY_URL_VAR = "L1_DISCOVERY_API_URL"

	// MAX_L1s_VAR configures the environment variable that determines the maximum
	// number of L1s this L2 will connect to and join the swarm for. Defaults to 100.
	MAX_L1s_VAR = "MAX_L1s"

	// MAX_CONCURRENT_L1_REQUESTS_VAR configures the environment variable that determines the maximum
	// number of CAR file requests that will be processed concurrently for a single L1. defaults to 3.
	MAX_CONCURRENT_L1_REQUESTS_VAR = "MAX_CONCURRENT_L1_REQUESTS"

	// TEST_L1_IPS_VAR configures the environment variable that determines the L1 IP Addresses
	// that this L2 node will join the swarm for and serve CAR files to. This environment variable accepts a comma
	// separated list of L1 IP addresses.
	// If this environment variable is set, the `L1_DISCOVERY_URL` environment variable becomes a no-op.
	TEST_L1_IPS_VAR = "TEST_L1_IPS"
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

	// DNS Hostname of Saturn L1 Nodes for the L1 Test network.
	saturn_l1_hostName = "saturn-test.network"

	defaultL1DiscoveryURL = "https://orchestrator.saturn-test.network/nodes/nearby"

	checkL1ConnectivityInterval = 5 * time.Second

	maxL1DiscoveryAttempts = float64(10)
	maxL1DiscoveryBackoff  = 60 * time.Second
	minL1DiscoveryBackoff  = 2 * time.Second

	maxDownloadPerRequest = uint64(2147483648) // 2 Gib
)

type config struct {
	Port                    int
	FilAddr                 string `json:"fil_wallet_address"`
	MaxDiskSpace            uint64
	RootDir                 string
	L1DiscoveryAPIUrl       string
	MaxL1Connections        int
	MaxConcurrentL1Requests int
	UseTestL1IPAddrs        bool
	TestL1IPAddr            L1IPAddrs
	MaxDownloadPerRequest   uint64
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	var carserver *CARServer
	var srv *http.Server
	var l1wg sync.WaitGroup
	var l1Clients []*l1interop.L1SseClient

	cleanup := func() {
		log.Info("shutting down all threads")
		cancel()

		// shut down the car server
		if carserver != nil {
			log.Info("shutting down the CAR server")
			if err := carserver.Stop(ctx); err != nil {
				log.Errorw("failed to stop car server", "err", err)
			}
		}

		// shut down all l1 clients
		for _, lc := range l1Clients {
			lc := lc
			log.Infow("closing connection with l1", "l1", lc.L1Addr)
			lc.Stop()
			log.Infow("finished closing connection with l1", "l1", lc.L1Addr)
		}

		// wait for all l1 connections to be torn down
		log.Info("waiting for all l1 connections to be torn down")
		l1wg.Wait()
		log.Info("finished tearing down all l1 connections")

		// shut down the http server
		if srv != nil {
			log.Info("shutting down the http server")
			_ = srv.Close()
		}
		os.Exit(0)
	}

	logging.SetAllLoggers(logging.LevelInfo)
	if err := logging.SetLogLevel("dagstore", "ERROR"); err != nil {
		panic(err)
	}
	// build app context
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Info("detected shutdown signal, will cleanup...")
		cleanup()
	}()

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
	var l1IPAddrs L1IPAddrs
	log.Info("waiting to discover L1s...")

	if cfg.UseTestL1IPAddrs {
		l1IPAddrs = cfg.TestL1IPAddr
	} else {
		l1IPAddrs, err = getNearestL1sWithRetry(ctx, cfg, maxL1DiscoveryAttempts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get nearest L1s to connect to: %s\n", err.Error())
			os.Exit(2)
		}
	}
	log.Infow("discovered L1s", "l1 IP Addrs", strings.Join(l1IPAddrs, ", "))
	fmt.Println("INFO: Saturn Node was able to connect to the Orchestrator and will now start connecting to the Saturn network...")

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
	carserver, err = buildCarServer(cfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build car server: %s", err.Error())
		os.Exit(2)
	}
	if err := carserver.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start car server: %s", err.Error())
		os.Exit(2)
	}

	// Connect and register with all L1s and start serving their requests
	nConnectedL1s := atomic.NewUint64(0)
	failedL1Ch := make(chan struct{}, len(l1IPAddrs))

	for _, l1ip := range l1IPAddrs {
		l1ip := l1ip
		l1client := l1interop.New(l2Id.String(), l1HttpClient, logger, carserver.server, l1ip, cfg.MaxConcurrentL1Requests)
		l1Clients = append(l1Clients, l1client)

		l1wg.Add(1)
		go func(l1ip string) {
			defer l1wg.Done()
			defer func() {
				failedL1Ch <- struct{}{}
			}()

			if err := l1client.Start(nConnectedL1s); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Errorw("terminated connection attempts with l1", "l1", l1ip, "err", err)
				}
			}
		}(l1ip)
	}

	// if we fail to connect to any of the L1s after exhausting all retries; error out and exit.
	go func() {
		for i := 0; i < len(l1IPAddrs); i++ {
			select {
			case <-failedL1Ch:
			case <-ctx.Done():
				return
			}
		}
		log.Error("failed to connect to any of the L1s after exhausting all attempts; shutting down")
		fmt.Println("ERROR: Saturn node failed to connect to the network and has exhausted all retry attempts")
		os.Exit(2)
	}()

	// start go-routine to log L1 connectivity
	go logL1Connectivity(ctx, nConnectedL1s)

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
		if _, err := w.Write(bz); err != nil {
			http.Error(w, "failed to write stats to response", http.StatusInternalServerError)
		}
	}))

	srv = &http.Server{
		Handler: m,
	}

	nl, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.Port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot start the webserver: %s\n", err.Error())
		os.Exit(4)
	}

	port := nl.Addr().(*net.TCPAddr).Port
	log.Infof("Server listening on %v", nl.Addr())
	fmt.Printf("WebUI: http://localhost:%d/webui\n", port)
	fmt.Printf("API: http://localhost:%d/\n", port)

	if err := srv.Serve(nl); err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "error shutting down the server: %s", err.Error())
	}
}

func logL1Connectivity(ctx context.Context, nConnectedL1s *atomic.Uint64) {
	ticker := time.NewTicker(checkL1ConnectivityInterval)
	defer ticker.Stop()

	lastNConnected := uint64(0)

	// get to the first connectivity event as fast as possible
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			nConnected := nConnectedL1s.Load()
			if nConnected != 0 {
				fmt.Printf("INFO: Saturn Node is online and connected to %d peers\n", nConnected)
				return
			}
		}
	}()

	for {
		select {
		case <-ticker.C:
			nConnected := nConnectedL1s.Load()

			// if we are still not connected to any peers -> log it.
			if nConnected == 0 {
				fmt.Print("ERROR: Saturn Node is not able to connect to the network\n")
			} else {
				if nConnected != lastNConnected {
					fmt.Printf("INFO: Saturn Node is online and connected to %d peers\n", nConnected)
				}
			}

			lastNConnected = nConnected
		case <-ctx.Done():
			return
		}
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
		if err := os.MkdirAll(rootDirStr, 0777); err != nil {
			return config{}, fmt.Errorf("failed to create default root dir %s, err=%w", rootDirStr, err)
		}
		log.Infow("create default l2 root directory", "dir", rootDirStr)
	}
	log.Infof("Using root dir %s\n", rootDirStr)

	var l1IPAddrs L1IPAddrs
	var useL1IPAddrs bool
	var durl string
	l1IpStr, exists := os.LookupEnv(TEST_L1_IPS_VAR)
	if exists {
		ips, err := parseL1IPs(l1IpStr)
		if err != nil {
			return config{}, fmt.Errorf("failed to parse L1 IPs environment variable: %w", err)
		}
		l1IPAddrs = ips
		useL1IPAddrs = true
	} else {
		// parse L1 Discovery API URL
		durl, exists = os.LookupEnv(L1_DISCOVERY_URL_VAR)
		if !exists {
			durl = defaultL1DiscoveryURL
		}
		if _, err := url.Parse(durl); err != nil {
			return config{}, fmt.Errorf("l1 discovery api url is invalid, failed to parse, err=%w", err)
		}
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
		UseTestL1IPAddrs:        useL1IPAddrs,
		TestL1IPAddr:            l1IPAddrs,
		MaxDownloadPerRequest:   uint64(maxDownloadPerRequest),
	}, nil
}

func parseL1IPs(l1IPsStr string) (L1IPAddrs, error) {
	l1IPAddrs := strings.Split(l1IPsStr, ",")
	if len(l1IPAddrs) == 0 || (len(l1IPAddrs) == 1 && len(l1IPAddrs[0]) == 0) {
		return nil, errors.New("need at least one valid L1 IP address")
	}

	for _, s := range l1IPAddrs {
		if ip := net.ParseIP(s); ip == nil {
			return nil, fmt.Errorf("l1 IP is invalid, ip=%s", ip)
		}
	}

	return l1IPAddrs, nil
}

func getNearestL1sWithRetry(ctx context.Context, cfg config, maxL1DiscoveryAttempts float64) (L1IPAddrs, error) {
	backoff := &backoff.Backoff{
		Min:    minL1DiscoveryBackoff,
		Max:    maxL1DiscoveryBackoff,
		Factor: 2,
		Jitter: true,
	}

	fmt.Println("INFO: Saturn Node will try to connect to the Saturn Orchestrator...")

	for {
		l1Addrs, err := getNearestL1s(ctx, cfg)
		if err == nil {
			return l1Addrs, nil
		}

		// if we've exhausted the maximum number of connection attempts with the L1, return.
		if backoff.Attempt() > maxL1DiscoveryAttempts {
			log.Errorw("exhausted all attempts to get L1s from orchestrator; not retrying", "err", err)
			return nil, err
		}

		log.Errorw("failed to get L1s from orchestrator; will retry", "err", err)
		fmt.Println("INFO: Saturn Node is unable to connect to the Orchestrator, retrying....")

		// backoff and wait before making a new request to the orchestrator.
		duration := backoff.Duration()
		bt := time.NewTimer(duration)
		defer bt.Stop()
		select {
		case <-bt.C:
			log.Infow("back-off complete, retrying request to orchestrator",
				"backoff time", duration.String())
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
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
	l1ips, err := io.ReadAll(rd)
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
	gwApi := carstore.NewGatewayAPI(gateway_base_url, sapi, cfg.MaxDownloadPerRequest)
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
		if _, err := w.Write(index); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
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
		if _, err := w.Write(conf); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
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
	if err := os.WriteFile(path, []byte(l2Id.String()), 0644); err != nil {
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

	bz, err := os.ReadFile(path)
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
