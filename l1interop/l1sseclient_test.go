package l1interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/stretchr/testify/require"

	cid "github.com/ipfs/go-cid"

	"github.com/filecoin-project/saturn-l2/carstore"

	"github.com/filecoin-project/saturn-l2/types"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"

	"github.com/filecoin-project/saturn-l2/testutils"

	"github.com/filecoin-project/saturn-l2/logs"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

func TestSingleL1Simple(t *testing.T) {
	l2Id := uuid.New().String()

	h := buildHarness(t, l2Id, 1)
	h.Start()
	defer h.Stop()

	req := uuid.New()
	h.sendReq(req, h.cid1, 0)

	require.Eventually(t, func() bool {
		return h.hasRecievedCAR(req.String(), h.cid1.String(), 0)
	}, 5*time.Second, 200*time.Millisecond)

	require.EqualValues(t, 1, h.nL1sConnected.Load())

}

func TestL1ConcurrentRequests(t *testing.T) {
	l2Id := uuid.New().String()

	h := buildHarness(t, l2Id, 1)
	h.Start()
	defer h.Stop()

	// send 50 requests and assert we get back 20 responses
	var wg sync.WaitGroup
	var reqs []string
	for i := 0; i < 50; i++ {
		req := uuid.New()
		reqs = append(reqs, req.String())
		var cid cid.Cid
		if i%2 == 0 {
			cid = h.cid1
		} else {
			cid = h.cid2
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			h.sendReq(req, cid, 0)
		}()
	}

	wg.Wait()

	require.Eventually(t, func() bool {
		for i := 0; i < 50; i++ {
			id := reqs[i]
			var cid cid.Cid
			if i%2 == 0 {
				cid = h.cid1
			} else {
				cid = h.cid2
			}

			ok := h.hasRecievedCAR(id, cid.String(), 0)
			if !ok {
				return false
			}

		}
		return true
	}, 5*time.Second, 200*time.Millisecond)

	require.EqualValues(t, 1, h.nL1sConnected.Load())
}

func TestMultipleL1ConcurrentRequests(t *testing.T) {
	l2Id := uuid.New().String()
	nL1s := 2

	h := buildHarness(t, l2Id, nL1s)
	h.Start()
	defer h.Stop()

	// send 50 requests and assert we get back 20 responses
	var wg sync.WaitGroup
	var reqs []string
	for i := 0; i < 50; i++ {
		i := i
		req := uuid.New()
		reqs = append(reqs, req.String())
		var cid cid.Cid
		if i%2 == 0 {
			cid = h.cid1
		} else {
			cid = h.cid2
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			h.sendReq(req, cid, i%nL1s)
		}()
	}

	wg.Wait()

	require.Eventually(t, func() bool {
		for i := 0; i < 50; i++ {
			id := reqs[i]
			var cid cid.Cid
			if i%2 == 0 {
				cid = h.cid1
			} else {
				cid = h.cid2
			}

			ok := h.hasRecievedCAR(id, cid.String(), i%nL1s)
			if !ok {
				return false
			}

		}
		return true
	}, 10*time.Second, 200*time.Millisecond)

	require.EqualValues(t, 2, h.nL1sConnected.Load())
}

type l1State struct {
	svc *httptest.Server

	requests chan types.CARTransferRequest

	mu       sync.Mutex
	gotPosts map[string][]byte
}

type l1Harness struct {
	cid1 cid.Cid
	cid2 cid.Cid

	emu             sync.Mutex
	expectedContent map[string][]byte

	t       *testing.T
	ctx     context.Context
	cancelF context.CancelFunc

	mu        sync.Mutex
	l1Clients map[int]*l1SseClient
	l1s       map[int]*l1State

	nL1sConnected *atomic.Uint64
}

func (h *l1Harness) Start() {
	for _, l1 := range h.l1s {
		l1 := l1
		l1.svc.StartTLS()
	}

	for _, l1Client := range h.l1Clients {
		l1Client := l1Client
		go l1Client.Start(h.nL1sConnected) // nolint
	}
}

func (h *l1Harness) Stop() {
	h.cancelF()

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, l1 := range h.l1s {
		l1 := l1
		l1.svc.Close()
	}

	for _, l1Client := range h.l1Clients {
		l1Client := l1Client
		l1Client.Stop()
	}
}

func (h *l1Harness) hasRecievedCAR(reqId string, root string, l1 int) bool {
	h.mu.Lock()
	l1S := h.l1s[l1]
	l1S.mu.Lock()

	actualBz, ok := l1S.gotPosts[root+reqId]
	l1S.mu.Unlock()
	h.mu.Unlock()
	if !ok {
		return false
	}

	h.emu.Lock()
	expectedBz, ok := h.expectedContent[root]
	h.emu.Unlock()
	if !ok {
		return false
	}

	return bytes.Equal(expectedBz, actualBz)
}

func (h *l1Harness) sendReq(reqId uuid.UUID, root cid.Cid, l1 int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	l1S := h.l1s[l1]

	l1S.requests <- types.CARTransferRequest{
		RequestId: reqId.String(),
		Root:      root.String(),
	}
}

func buildHarness(t *testing.T, l2Id string, nL1s int) *l1Harness {
	hCtx, cancel := context.WithCancel(context.Background())
	carFile1 := "../testdata/files/sample-v1.car"
	rootcid1, bz1 := testutils.ParseCar(t, hCtx, carFile1)
	carFile2 := "../testdata/files/sample-rw-bs-v2.car"
	rootcid2, bz2 := testutils.ParseCar(t, hCtx, carFile2)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
	}
	lg := logs.NewSaturnLogger()

	h := &l1Harness{
		cancelF:         cancel,
		cid1:            rootcid1,
		cid2:            rootcid2,
		ctx:             hCtx,
		t:               t,
		expectedContent: make(map[string][]byte),
		l1s:             make(map[int]*l1State),
		l1Clients:       make(map[int]*l1SseClient),
		nL1sConnected:   atomic.NewUint64(0),
	}
	h.expectedContent[rootcid1.String()] = bz1
	h.expectedContent[rootcid2.String()] = bz2
	ms := newMockCarServer(t, hCtx)

	// Build the L1 HTTP Server
	for i := 0; i < nL1s; i++ {
		l1 := &l1State{
			gotPosts: make(map[string][]byte),
			requests: make(chan types.CARTransferRequest, 100),
		}

		mu := mux.NewRouter()
		mu.HandleFunc("/data/{root}", func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			rootCid := vars["root"]

			vs := r.URL.Query()
			requestId := vs.Get("requestId")

			bz, err := io.ReadAll(r.Body)
			if err != nil {
				return
			}
			r.Body.Close()
			l1.mu.Lock()
			l1.gotPosts[rootCid+requestId] = bz
			l1.mu.Unlock()
		})

		mu.HandleFunc("/register/{l2Id}", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			vars := mux.Vars(r)
			l2Idr := vars["l2Id"]
			if l2Idr != l2Id {
				return
			}
			for {
				select {
				case cr := <-l1.requests:
					bz, err := json.Marshal(cr)
					if err != nil {
						return
					}
					if _, err := w.Write(bz); err != nil {
						fmt.Println("ERROR when writing to L2", err.Error())
						return
					}
					if _, err := w.Write([]byte("\n")); err != nil {
						fmt.Println("ERROR when writing to L2", err.Error())
						return
					}
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
				case <-hCtx.Done():
					return
				}
			}
		})

		svc := httptest.NewUnstartedServer(mu)
		l1.svc = svc

		h.l1Clients[i] = New(l2Id, client, lg, ms, svc.Listener.Addr().String(), 3)
		h.l1s[i] = l1
	}

	return h
}

type mockCarServer struct {
	cars map[string][]byte
}

func newMockCarServer(t *testing.T, ctx context.Context) *mockCarServer {
	carFile1 := "../testdata/files/sample-v1.car"
	rootcid1, bz1 := testutils.ParseCar(t, ctx, carFile1)
	carFile2 := "../testdata/files/sample-rw-bs-v2.car"
	rootcid2, bz2 := testutils.ParseCar(t, ctx, carFile2)

	m := make(map[string][]byte)

	m[rootcid1.String()] = bz1
	m[rootcid2.String()] = bz2

	return &mockCarServer{
		cars: m,
	}
}

func (mc *mockCarServer) ServeCARFile(ctx context.Context, dr *types.DagTraversalRequest, w io.Writer) error {
	r := dr.Root.String()

	bz, ok := mc.cars[r]
	if !ok {
		return carstore.ErrNotFound
	}
	if _, err := w.Write(bz); err != nil {
		return err
	}
	return nil
}
