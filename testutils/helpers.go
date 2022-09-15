package testutils

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	car "github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/bsadapter"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"

	cid "github.com/ipfs/go-cid"
	carv2bs "github.com/ipld/go-car/v2/blockstore"
	"github.com/stretchr/testify/require"
)

func ParseCar(t *testing.T, ctx context.Context, path string) (cid.Cid, []byte) {
	from, err := carv2bs.OpenReadOnly(path)
	require.NoError(t, err)
	rts, err := from.Roots()
	require.NoError(t, err)

	ls := cidlink.DefaultLinkSystem()
	bsa := bsadapter.Adapter{Wrapped: from}
	ls.SetReadStorage(&bsa)

	w := bytes.NewBuffer(nil)
	_, err = car.TraverseV1(ctx, &ls, rts[0], selectorparse.CommonSelector_ExploreAllRecursively, w)
	require.NoError(t, err)

	require.NoError(t, from.Close())

	return rts[0], w.Bytes()
}

func GetTestServerFor(t *testing.T, ctx context.Context, path string) (cid.Cid, []byte, *httptest.Server) {
	root, contents := ParseCar(t, ctx, path)
	return root, contents, GetTestServer(t, root.String(), contents)
}

func GetTestHangingServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(24 * time.Hour)
	}))
}

func GetTestServerForRoots(t *testing.T, out map[string][]byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		v := q.Get("arg")
		if len(v) == 0 {
			http.Error(w, "invalid arg", http.StatusBadRequest)
			return
		}
		bz, ok := out[v]
		if !ok {
			http.Error(w, "invalid arg", http.StatusBadRequest)
			return
		}
		_, err := w.Write(bz)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
}

func GetTestServer(t *testing.T, root string, out []byte) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		v := q.Get("arg")
		if len(v) == 0 || v != root {
			http.Error(w, "invalid arg", http.StatusBadRequest)
			return
		}
		if _, err := w.Write(out); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
}

func GetTestErrorServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad req", http.StatusInternalServerError)
	}))
}
