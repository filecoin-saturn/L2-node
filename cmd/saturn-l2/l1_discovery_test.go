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

		w.Write(bz)
	}))

	cfg := config{
		L1DiscoveryAPIUrl: svc.URL,
		MaxL1Connections:  100,
	}
	l1s, err := getNearestL1sWithRetry(ctx, cfg)
	require.NoError(t, err)
	require.Len(t, l1s, 3)

	cfg.MaxL1Connections = 1
	l1s, err = getNearestL1sWithRetry(ctx, cfg)
	require.NoError(t, err)
	require.Len(t, l1s, 1)
	require.EqualValues(t, ip1, l1s[0])
}

func TestL1DiscoveryFailure(t *testing.T) {
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("failure"))
	}))

	cfg := config{
		L1DiscoveryAPIUrl: svc.URL,
		MaxL1Connections:  100,
	}
	l1s, err := getNearestL1sWithRetry(context.Background(), cfg)
	require.Error(t, err)
	require.Empty(t, l1s)
}
