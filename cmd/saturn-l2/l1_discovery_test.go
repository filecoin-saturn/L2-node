package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	ip1 = "1.1.1.1"
	ip2 = "2.2.2.2"
	ip3 = "3.3.3.3"
)

func TestL1Discovery(t *testing.T) {
	ctx := context.Background()
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ips := []string{ip1, ip2, ip3}
		bz, _ := json.Marshal(ips)

		_, err := w.Write(bz)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))

	cfg := config{
		L1DiscoveryAPIUrl: svc.URL,
		MaxL1Connections:  100,
	}
	l1s, err := getNearestL1sWithRetry(ctx, cfg, 1)
	require.NoError(t, err)
	require.Len(t, l1s, 3)

	cfg.MaxL1Connections = 1
	l1s, err = getNearestL1sWithRetry(ctx, cfg, 1)
	require.NoError(t, err)
	require.Len(t, l1s, 1)
	require.EqualValues(t, ip1, l1s[0])
}

func TestL1DiscoveryFailure(t *testing.T) {
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte("failure")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))

	cfg := config{
		L1DiscoveryAPIUrl: svc.URL,
		MaxL1Connections:  100,
	}
	l1s, err := getNearestL1sWithRetry(context.Background(), cfg, 1)
	require.Error(t, err)
	require.Empty(t, l1s)
}
