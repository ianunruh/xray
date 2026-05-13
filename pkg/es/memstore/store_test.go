package memstore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func TestStore_LoadEmpty(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	events, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Empty(t, events)
}

func TestStore_AppendAndLoad(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	events := []es.RawEvent{
		{Type: "OrderPlaced", Data: []byte("data1")},
		{Type: "TradeExecuted", Data: []byte("data2")},
	}

	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 0, events))

	loaded, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.Len(t, loaded, 2)

	assert.Equal(t, 1, loaded[0].Version)
	assert.Equal(t, 2, loaded[1].Version)
	assert.Equal(t, "orderbook:AAPL", loaded[0].AggregateID)
}

func TestStore_OptimisticConcurrency(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	events := []es.RawEvent{
		{Type: "OrderPlaced", Data: []byte("data1")},
	}

	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 0, events))

	// Append with wrong expected version.
	err := store.Append(ctx, "orderbook:AAPL", 0, events)
	assert.ErrorIs(t, err, es.ErrOptimisticConcurrency)

	// Append with correct expected version should succeed.
	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 1, events))
}

func TestStore_IsolatedStreams(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 0, []es.RawEvent{{Type: "A", Data: []byte("a")}}))
	require.NoError(t, store.Append(ctx, "orderbook:GOOG", 0, []es.RawEvent{{Type: "B", Data: []byte("b")}}))

	aapl, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.Len(t, aapl, 1)
	assert.Equal(t, "A", aapl[0].Type)

	goog, err := store.Load(ctx, "orderbook:GOOG")
	require.NoError(t, err)
	require.Len(t, goog, 1)
	assert.Equal(t, "B", goog[0].Type)
}
