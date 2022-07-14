package testutils

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	cid "github.com/ipfs/go-cid"
	carv2bs "github.com/ipld/go-car/v2/blockstore"
	"github.com/stretchr/testify/require"
)

func ParseCar(t *testing.T, path string) (root cid.Cid, contents []byte) {
	from, err := carv2bs.OpenReadOnly(path)
	require.NoError(t, err)
	rts, err := from.Roots()
	require.NoError(t, err)
	require.NoError(t, from.Close())

	out, err := ioutil.ReadFile(path)
	require.NoError(t, err)
	require.NotEmpty(t, out)

	return rts[0], out
}

func GetTestServerFor(t *testing.T, path string) (cid.Cid, []byte, *httptest.Server) {
	root, contents := ParseCar(t, path)
	return root, contents, GetTestServer(t, root.String(), contents)
}

func GetTestHangingServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for {

		}
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
		w.Write(bz)
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
		w.Write(out)
	}))
}

func GetTestErrorServer(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad req", http.StatusInternalServerError)
	}))
}
