package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	address "github.com/filecoin-project/go-address"
	"github.com/gorilla/mux"

	"github.com/filecoin-project/saturn-l2/resources"
)

type config struct {
	FilAddr string `json:"fil_wallet_address"`
}

func main() {
	var port int
	portStr := os.Getenv("PORT")
	if portStr == "" {
		port = 5500
	} else {
		var err error
		port, err = strconv.Atoi(portStr)
		if err != nil {
			panic(fmt.Errorf("invalid PORT value '%s': %s", portStr, err.Error()))
		}
	}

	filAddr := os.Getenv("FIL_WALLET_ADDRESS")
	if filAddr == "" {
		panic(errors.New("no FIL_WALLET_ADDRESS provided"))
	}
	if _, err := address.NewFromString(filAddr); err != nil {
		panic(fmt.Errorf("invalid FIL_WALLET_ADDRESS format: %s", err.Error()))
	}
	conf, err := json.Marshal(config{FilAddr: filAddr})
	if err != nil {
		panic(errors.New("failed to serialize config"))
	}

	m := mux.NewRouter()
	m.PathPrefix("/config").Handler(http.HandlerFunc(configHandler(conf)))
	m.PathPrefix("/webui").Handler(http.HandlerFunc(webuiHandler))

	srv := &http.Server{
		Handler: m,
	}

	nl, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
	defer nl.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := srv.Serve(nl); err != http.ErrServerClosed {
			panic(err)
		}
		defer srv.Close()
		wg.Done()
	}()

	port = nl.Addr().(*net.TCPAddr).Port
	fmt.Println("Server listening on", nl.Addr())
	fmt.Printf("WebUI: http://localhost:%d/webui\n", port)
	wg.Wait()
}

func webuiHandler(w http.ResponseWriter, r *http.Request) {
	rootDir := "webui"
	path := strings.TrimPrefix(r.URL.Path, "/")

	_, pathErr := resources.WebUI.Open(path)
	if path == rootDir || os.IsNotExist(pathErr) {
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
	} else if pathErr != nil {
		http.Error(w, pathErr.Error(), http.StatusInternalServerError)
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
