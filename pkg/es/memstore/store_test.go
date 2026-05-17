package memstore_test

import (
	"context"
	"testing"
	"time"

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

func TestStore_LoadRange(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	events := []es.RawEvent{
		{Type: "A", Data: []byte("a")},
		{Type: "B", Data: []byte("b")},
		{Type: "C", Data: []byte("c")},
		{Type: "D", Data: []byte("d")},
	}
	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 0, events))

	// Closed interval [2, 3].
	loaded, err := store.LoadRange(ctx, "orderbook:AAPL", 2, 3)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, "B", loaded[0].Type)
	assert.Equal(t, "C", loaded[1].Type)

	// fromVersion == toVersion returns single event.
	loaded, err = store.LoadRange(ctx, "orderbook:AAPL", 3, 3)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, "C", loaded[0].Type)

	// toVersion <= 0 means no upper bound — equivalent to LoadFrom.
	loaded, err = store.LoadRange(ctx, "orderbook:AAPL", 3, 0)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, "C", loaded[0].Type)
	assert.Equal(t, "D", loaded[1].Type)

	// Range entirely beyond stream returns empty.
	loaded, err = store.LoadRange(ctx, "orderbook:AAPL", 10, 20)
	require.NoError(t, err)
	assert.Empty(t, loaded)
}

func TestStore_StreamMetadata(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	// Empty stream returns zero values, no error.
	meta, err := store.StreamMetadata(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Equal(t, 0, meta.FirstVersion)
	assert.Equal(t, 0, meta.LastVersion)
	assert.True(t, meta.FirstTimestamp.IsZero())

	t0 := time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC)
	t1 := t0.Add(5 * time.Minute)
	t2 := t0.Add(10 * time.Minute)
	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 0, []es.RawEvent{
		{Type: "A", Timestamp: t0, Data: []byte("a")},
		{Type: "B", Timestamp: t1, Data: []byte("b")},
		{Type: "C", Timestamp: t2, Data: []byte("c")},
	}))

	meta, err = store.StreamMetadata(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Equal(t, 1, meta.FirstVersion)
	assert.Equal(t, 3, meta.LastVersion)
	assert.True(t, meta.FirstTimestamp.Equal(t0))
	assert.True(t, meta.LastTimestamp.Equal(t2))
}

func TestStore_VersionAtTimestamp(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	// Empty stream returns 0.
	v, err := store.VersionAtTimestamp(ctx, "orderbook:AAPL", time.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, v)

	t0 := time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC)
	t1 := t0.Add(5 * time.Minute)
	t2 := t0.Add(10 * time.Minute)
	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 0, []es.RawEvent{
		{Type: "A", Timestamp: t0, Data: []byte("a")},
		{Type: "B", Timestamp: t1, Data: []byte("b")},
		{Type: "C", Timestamp: t2, Data: []byte("c")},
	}))

	// Before the first event.
	v, err = store.VersionAtTimestamp(ctx, "orderbook:AAPL", t0.Add(-time.Second))
	require.NoError(t, err)
	assert.Equal(t, 0, v)

	// Exactly at the first event.
	v, err = store.VersionAtTimestamp(ctx, "orderbook:AAPL", t0)
	require.NoError(t, err)
	assert.Equal(t, 1, v)

	// Between events 2 and 3.
	v, err = store.VersionAtTimestamp(ctx, "orderbook:AAPL", t1.Add(30*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 2, v)

	// After the last event.
	v, err = store.VersionAtTimestamp(ctx, "orderbook:AAPL", t2.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 3, v)
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

func TestStore_LoadAll(t *testing.T) {
	store := memstore.New()
	ctx := context.Background()

	require.NoError(t, store.Append(ctx, "orderbook:AAPL", 0, []es.RawEvent{
		{Type: "A", Data: []byte("a1")},
		{Type: "B", Data: []byte("a2")},
	}))
	require.NoError(t, store.Append(ctx, "orderbook:GOOG", 0, []es.RawEvent{
		{Type: "C", Data: []byte("g1")},
	}))

	all, err := store.LoadAll(ctx)
	require.NoError(t, err)
	require.Len(t, all, 3)

	// Sorted by (AggregateID, Version).
	assert.Equal(t, "orderbook:AAPL", all[0].AggregateID)
	assert.Equal(t, 1, all[0].Version)
	assert.Equal(t, "orderbook:AAPL", all[1].AggregateID)
	assert.Equal(t, 2, all[1].Version)
	assert.Equal(t, "orderbook:GOOG", all[2].AggregateID)
	assert.Equal(t, 1, all[2].Version)
}

func TestStore_LoadAll_Empty(t *testing.T) {
	store := memstore.New()
	all, err := store.LoadAll(context.Background())
	require.NoError(t, err)
	assert.Empty(t, all)
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
