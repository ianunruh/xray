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

func TestStore_LoadFrom(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	events := []es.RawEvent{
		{Type: "A", Data: []byte("a")},
		{Type: "B", Data: []byte("b")},
		{Type: "C", Data: []byte("c")},
		{Type: "D", Data: []byte("d")},
	}
	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 0, events))

	// Load from version 3 (inclusive).
	loaded, err := store.LoadFrom(ctx, "orderbook:AAPL", 3)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, 3, loaded[0].Version)
	assert.Equal(t, "C", loaded[0].Type)
	assert.Equal(t, 4, loaded[1].Version)
	assert.Equal(t, "D", loaded[1].Type)

	// Load from version 1 returns all events.
	loaded, err = store.LoadFrom(ctx, "orderbook:AAPL", 1)
	require.NoError(t, err)
	assert.Len(t, loaded, 4)

	// Load from version beyond the stream returns empty.
	loaded, err = store.LoadFrom(ctx, "orderbook:AAPL", 10)
	require.NoError(t, err)
	assert.Empty(t, loaded)
}

func TestStore_LoadSnapshot_None(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	snap, err := store.LoadSnapshot(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Nil(t, snap)
}

func TestStore_SaveAndLoadSnapshot(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	snap := es.Snapshot{
		AggregateID: "orderbook:AAPL",
		Version:     50,
		Data:        []byte("snapshot-data"),
	}
	require.NoError(t, store.SaveSnapshot(ctx, snap))

	loaded, err := store.LoadSnapshot(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, "orderbook:AAPL", loaded.AggregateID)
	assert.Equal(t, 50, loaded.Version)
	assert.Equal(t, []byte("snapshot-data"), loaded.Data)

	// Overwrite with a newer snapshot.
	snap2 := es.Snapshot{
		AggregateID: "orderbook:AAPL",
		Version:     100,
		Data:        []byte("snapshot-data-2"),
	}
	require.NoError(t, store.SaveSnapshot(ctx, snap2))

	loaded, err = store.LoadSnapshot(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Equal(t, 100, loaded.Version)
	assert.Equal(t, []byte("snapshot-data-2"), loaded.Data)

	// Different aggregate has no snapshot.
	other, err := store.LoadSnapshot(ctx, "orderbook:GOOG")
	require.NoError(t, err)
	assert.Nil(t, other)
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
