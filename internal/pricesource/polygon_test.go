package pricesource

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolygonPriceSource_FetchAndGetPrice(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "/v2/aggs/ticker/AAPL/prev")
		assert.Equal(t, "test-key", r.URL.Query().Get("apiKey"))
		w.Write([]byte(`{"results":[{"c":150.5}]}`))
	}))
	defer srv.Close()

	src := NewPolygonPriceSource(
		PolygonConfig{BaseURL: srv.URL, PollInterval: time.Hour},
		"test-key",
		[]string{"AAPL"},
		slog.Default(),
	)

	_, ok := src.GetPrice("AAPL")
	assert.False(t, ok, "no price before fetch")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src.fetchAll(ctx)

	snap, ok := src.GetPrice("AAPL")
	require.True(t, ok)
	assert.Equal(t, int64(1505000), snap.Price)
	assert.WithinDuration(t, time.Now(), snap.FetchedAt, time.Second)
}

func TestPolygonPriceSource_EmptyResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"results":[]}`))
	}))
	defer srv.Close()

	src := NewPolygonPriceSource(
		PolygonConfig{BaseURL: srv.URL, PollInterval: time.Hour},
		"key",
		[]string{"AAPL"},
		slog.Default(),
	)

	ctx := context.Background()
	src.fetchAll(ctx)

	_, ok := src.GetPrice("AAPL")
	assert.False(t, ok)
}

func TestPolygonPriceSource_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	src := NewPolygonPriceSource(
		PolygonConfig{BaseURL: srv.URL, PollInterval: time.Hour},
		"key",
		[]string{"AAPL"},
		slog.Default(),
	)

	ctx := context.Background()
	src.fetchAll(ctx)

	_, ok := src.GetPrice("AAPL")
	assert.False(t, ok)
}

func TestPolygonPriceSource_PriceConversion(t *testing.T) {
	tests := []struct {
		closePrice float64
		want       int64
	}{
		{150.0, 1500000},
		{150.5, 1505000},
		{150.1234, 1501234},
		{0.01, 100},
		{99999.9999, 999999999},
	}

	for _, tt := range tests {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"results":[{"c":` + formatFloat(tt.closePrice) + `}]}`))
		}))

		src := NewPolygonPriceSource(
			PolygonConfig{BaseURL: srv.URL, PollInterval: time.Hour},
			"key",
			[]string{"TEST"},
			slog.Default(),
		)
		src.fetchAll(context.Background())

		snap, ok := src.GetPrice("TEST")
		require.True(t, ok)
		assert.Equal(t, tt.want, snap.Price, "close=%.4f", tt.closePrice)

		srv.Close()
	}
}

func formatFloat(f float64) string {
	return fmt.Sprintf("%.4f", f)
}
