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
	addr1 = &L1Addr{
		Id:       "1",
		Ip:       "1.1.1.1",
		Distance: 1.0,
		Weight:   1,
	}
	addr2 = &L1Addr{
		Id:       "2",
		Ip:       "2.2.2.2",
		Distance: 1.0,
		Weight:   1,
	}
	addr3 = &L1Addr{
		Id:       "3",
		Ip:       "3.3.3.3",
		Distance: 1.0,
		Weight:   1,
	}
)

func TestL1Discovery(t *testing.T) {
	ctx := context.Background()
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		addrs := []L1Addr{*addr1, *addr2, *addr3}
		bz, _ := json.Marshal(addrs)

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
	require.EqualValues(t, addr1.Ip, l1s[0])
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
