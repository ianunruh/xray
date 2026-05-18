package es_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

// testAggregate is a minimal aggregate for handler integration tests.
type testAggregate struct {
	es.AggregateBase
	orderCount int
}

func (a *testAggregate) Apply(evt es.Event) error {
	switch evt.Data.(type) {
	case *orderbookv1.OrderPlaced:
		a.orderCount++
	}
	a.IncrementVersion()
	return nil
}

type testCommand struct {
	aggregateID string
}

func (c testCommand) AggregateID() string { return c.aggregateID }

// snapshotAggregate is a test aggregate that implements Snapshotable.
type snapshotAggregate struct {
	es.AggregateBase
	orderCount int
}

func (a *snapshotAggregate) Apply(evt es.Event) error {
	switch evt.Data.(type) {
	case *orderbookv1.OrderPlaced:
		a.orderCount++
	}
	a.IncrementVersion()
	return nil
}

func (a *snapshotAggregate) Snapshot() (proto.Message, error) {
	return &orderbookv1.OrderBookSnapshot{
		Symbol: fmt.Sprintf("count:%d", a.orderCount),
	}, nil
}

func (a *snapshotAggregate) RestoreSnapshot(msg proto.Message) error {
	snap := msg.(*orderbookv1.OrderBookSnapshot)
	var count int
	fmt.Sscanf(snap.Symbol, "count:%d", &count)
	a.orderCount = count
	return nil
}

func (a *snapshotAggregate) SnapshotInterval() int {
	return 3
}

func TestHandler_Integration(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default())

	ctx := context.Background()
	now := time.Now()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	// First command: place an order.
	err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		assert.Equal(t, 0, agg.orderCount)
		evt := es.Event{
			AggregateID: agg.AggregateID(),
			Type:        "OrderPlaced",
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 50,
				PlacedAt: timestamppb.New(now),
			},
		}
		require.NoError(t, agg.Apply(evt))
		return []es.Event{evt}, nil
	})
	require.NoError(t, err)

	// Second command: verify aggregate was rehydrated with the first event.
	err = handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		assert.Equal(t, 1, agg.orderCount)
		evt := es.Event{
			AggregateID: agg.AggregateID(),
			Type:        "OrderPlaced",
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-2",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1510000,
				Quantity: 25,
				PlacedAt: timestamppb.New(now),
			},
		}
		require.NoError(t, agg.Apply(evt))
		return []es.Event{evt}, nil
	})
	require.NoError(t, err)

	// Verify event stream has both events.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.Len(t, raw, 2)

	// Verify deserialized events.
	for i, r := range raw {
		evt, err := registry.Deserialize(r)
		require.NoError(t, err, "event %d", i)

		placed, ok := evt.Data.(*orderbookv1.OrderPlaced)
		require.True(t, ok, "event %d: expected *OrderPlaced, got %T", i, evt.Data)
		assert.Equal(t, timestamppb.New(now).AsTime(), placed.PlacedAt.AsTime(), "event %d: timestamp mismatch", i)
	}
}

// seedSnapshot stores a snapshot directly in the snapshot store, mimicking
// what the async snapshotter (pkg/es/snapshotter) would persist. Used by
// LoadAt tests that need a snapshot to exist without depending on the
// snapshotter's projection plumbing.
func seedSnapshot(t *testing.T, store es.SnapshotStore, id string, version, orderCount int) {
	t.Helper()
	agg := &snapshotAggregate{orderCount: orderCount}
	msg, err := agg.Snapshot()
	require.NoError(t, err)
	data, err := proto.Marshal(msg)
	require.NoError(t, err)
	require.NoError(t, store.SaveSnapshot(context.Background(), es.Snapshot{
		AggregateID: id,
		Version:     version,
		Data:        data,
	}))
}

// loadCountingStore wraps a Store and counts Load calls.
type loadCountingStore struct {
	*memstore.Store
	loads   int
	appends int
}

func (s *loadCountingStore) Load(ctx context.Context, aggregateID string) ([]es.RawEvent, error) {
	s.loads++
	return s.Store.Load(ctx, aggregateID)
}

func (s *loadCountingStore) LoadFrom(ctx context.Context, aggregateID string, fromVersion int) ([]es.RawEvent, error) {
	s.loads++
	return s.Store.LoadFrom(ctx, aggregateID, fromVersion)
}

// conflictOnceStore wraps a Store and returns ErrOptimisticConcurrency on the
// first Append call, then delegates to the real store.
type conflictOnceStore struct {
	loadCountingStore
}

func (s *conflictOnceStore) Append(ctx context.Context, aggregateID string, expectedVersion int, events []es.RawEvent) error {
	s.appends++
	if s.appends == 1 {
		return es.ErrOptimisticConcurrency
	}
	return s.Store.Append(ctx, aggregateID, expectedVersion, events)
}

func TestHandler_RetryOnConflict(t *testing.T) {
	registry := newTestRegistry()
	store := &conflictOnceStore{loadCountingStore: loadCountingStore{Store: memstore.New()}}

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default())

	ctx := context.Background()
	now := time.Now()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		evt := es.Event{
			AggregateID: agg.AggregateID(),
			Type:        "OrderPlaced",
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 50,
				PlacedAt: timestamppb.New(now),
			},
		}
		require.NoError(t, agg.Apply(evt))
		return []es.Event{evt}, nil
	})
	require.NoError(t, err)

	// First append conflicted, second succeeded.
	assert.Equal(t, 2, store.appends)

	// Cache was discarded after conflict, so aggregate was reloaded from DB.
	assert.Equal(t, 2, store.loads)

	// Verify the event was persisted.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Len(t, raw, 1)
}

func TestHandler_CacheReuse(t *testing.T) {
	registry := newTestRegistry()
	store := &loadCountingStore{Store: memstore.New()}

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default())

	ctx := context.Background()
	now := time.Now()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	// First command: loads from DB.
	err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		assert.Equal(t, 0, agg.orderCount)
		evt := es.Event{
			AggregateID: agg.AggregateID(),
			Type:        "OrderPlaced",
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 50,
				PlacedAt: timestamppb.New(now),
			},
		}
		require.NoError(t, agg.Apply(evt))
		return []es.Event{evt}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, store.loads, "first handle should load from DB")

	// Second command: uses cached aggregate, no DB load.
	err = handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		assert.Equal(t, 1, agg.orderCount, "cached aggregate should have first event applied")
		evt := es.Event{
			AggregateID: agg.AggregateID(),
			Type:        "OrderPlaced",
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-2",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1510000,
				Quantity: 25,
				PlacedAt: timestamppb.New(now),
			},
		}
		require.NoError(t, agg.Apply(evt))
		return []es.Event{evt}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, store.loads, "second handle should use cache, not load from DB")

	// Verify both events were persisted.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Len(t, raw, 2)
}

func TestHandler_LoadAt_NoSnapshot(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default())

	ctx := context.Background()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	// Append 5 events via the handler.
	for i := 1; i <= 5; i++ {
		err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
			evt := es.Event{
				AggregateID: agg.AggregateID(),
				Type:        "OrderPlaced",
				Timestamp:   time.Now(),
				Data: &orderbookv1.OrderPlaced{
					OrderId:  fmt.Sprintf("order-%d", i),
					Symbol:   "AAPL",
					Side:     orderbookv1.Side_SIDE_BUY,
					Price:    1500000,
					Quantity: 1,
					PlacedAt: timestamppb.Now(),
				},
			}
			require.NoError(t, agg.Apply(evt))
			return []es.Event{evt}, nil
		})
		require.NoError(t, err)
	}

	// LoadAt(3) should yield an aggregate that's seen exactly 3 events.
	agg, err := handler.LoadAt(ctx, "orderbook:AAPL", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, agg.orderCount)
	assert.Equal(t, 3, agg.Version())

	// LoadAt(5) yields head state.
	agg, err = handler.LoadAt(ctx, "orderbook:AAPL", 5)
	require.NoError(t, err)
	assert.Equal(t, 5, agg.orderCount)

	// LoadAt(0) is an error.
	_, err = handler.LoadAt(ctx, "orderbook:AAPL", 0)
	assert.Error(t, err)
}

func TestHandler_LoadAt_SnapshotBeforeTarget(t *testing.T) {
	// With a snapshot persisted at v3, LoadAt(5) should restore from the
	// snapshot and apply events 4-5. Snapshot writes are async and
	// driven externally (the snapshotter projection); this test seeds
	// the snapshot directly so it exercises only the load path.
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *snapshotAggregate {
		a := &snapshotAggregate{}
		a.SetID(id)
		return a
	}, slog.Default()).WithSnapshots(store)

	ctx := context.Background()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	for i := 1; i <= 5; i++ {
		err := handler.Handle(ctx, cmd, func(agg *snapshotAggregate) ([]es.Event, error) {
			evt := es.Event{
				AggregateID: agg.AggregateID(),
				Type:        "OrderPlaced",
				Timestamp:   time.Now(),
				Data: &orderbookv1.OrderPlaced{
					OrderId:  fmt.Sprintf("order-%d", i),
					Symbol:   "AAPL",
					Side:     orderbookv1.Side_SIDE_BUY,
					Price:    1500000,
					Quantity: 1,
					PlacedAt: timestamppb.Now(),
				},
			}
			require.NoError(t, agg.Apply(evt))
			return []es.Event{evt}, nil
		})
		require.NoError(t, err)
	}

	seedSnapshot(t, store, "orderbook:AAPL", 3, 3)

	agg, err := handler.LoadAt(ctx, "orderbook:AAPL", 5)
	require.NoError(t, err)
	assert.Equal(t, 5, agg.orderCount)
	assert.Equal(t, 5, agg.Version())

	// LoadAt at exactly the snapshot version should restore and apply no
	// further events.
	agg, err = handler.LoadAt(ctx, "orderbook:AAPL", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, agg.orderCount)
	assert.Equal(t, 3, agg.Version())
}

func TestHandler_LoadAt_SnapshotAfterTarget(t *testing.T) {
	// When the latest snapshot is newer than atVersion the handler must
	// fall back to a full replay from version 1, not restore the snapshot
	// (which would yield state from a future point).
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *snapshotAggregate {
		a := &snapshotAggregate{}
		a.SetID(id)
		return a
	}, slog.Default()).WithSnapshots(store)

	ctx := context.Background()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	for i := 1; i <= 6; i++ {
		err := handler.Handle(ctx, cmd, func(agg *snapshotAggregate) ([]es.Event, error) {
			evt := es.Event{
				AggregateID: agg.AggregateID(),
				Type:        "OrderPlaced",
				Timestamp:   time.Now(),
				Data: &orderbookv1.OrderPlaced{
					OrderId:  fmt.Sprintf("order-%d", i),
					Symbol:   "AAPL",
					Side:     orderbookv1.Side_SIDE_BUY,
					Price:    1500000,
					Quantity: 1,
					PlacedAt: timestamppb.Now(),
				},
			}
			require.NoError(t, agg.Apply(evt))
			return []es.Event{evt}, nil
		})
		require.NoError(t, err)
	}

	// Latest snapshot is at v6.
	seedSnapshot(t, store, "orderbook:AAPL", 6, 6)

	// Asking for v2 must NOT use the v6 snapshot.
	agg, err := handler.LoadAt(ctx, "orderbook:AAPL", 2)
	require.NoError(t, err)
	assert.Equal(t, 2, agg.orderCount, "expected fall-back to replay from version 1")
	assert.Equal(t, 2, agg.Version())
}

func TestHandler_LoadEvents(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default())

	ctx := context.Background()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	for i := 1; i <= 4; i++ {
		err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
			evt := es.Event{
				AggregateID: agg.AggregateID(),
				Type:        "OrderPlaced",
				Timestamp:   time.Now(),
				Data: &orderbookv1.OrderPlaced{
					OrderId:  fmt.Sprintf("order-%d", i),
					Symbol:   "AAPL",
					Side:     orderbookv1.Side_SIDE_BUY,
					Price:    1500000,
					Quantity: 1,
					PlacedAt: timestamppb.Now(),
				},
			}
			require.NoError(t, agg.Apply(evt))
			return []es.Event{evt}, nil
		})
		require.NoError(t, err)
	}

	events, err := handler.LoadEvents(ctx, "orderbook:AAPL", 2, 3)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, 2, events[0].Version)
	assert.Equal(t, 3, events[1].Version)

	// Verify Data is deserialized.
	placed, ok := events[0].Data.(*orderbookv1.OrderPlaced)
	require.True(t, ok)
	assert.Equal(t, "order-2", placed.OrderId)
}

type recordingPublisher struct {
	published []es.Event
}

func (p *recordingPublisher) Publish(_ context.Context, events []es.Event) error {
	p.published = append(p.published, events...)
	return nil
}

func TestHandler_StampsCausation_Origin(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default())

	ctx := context.Background()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		evt := es.Event{
			AggregateID: agg.AggregateID(),
			Type:        "OrderPlaced",
			Timestamp:   time.Now(),
			Data:        &orderbookv1.OrderPlaced{OrderId: "order-1"},
		}
		require.NoError(t, agg.Apply(evt))
		return []es.Event{evt}, nil
	})
	require.NoError(t, err)

	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.Len(t, raw, 1)

	// Origin: fresh ID, empty causation, fresh correlation.
	assert.NotEmpty(t, raw[0].ID, "framework must mint an event ID")
	assert.Empty(t, raw[0].CausationID, "origin command has no parent event")
	assert.NotEmpty(t, raw[0].CorrelationID, "origin must mint a fresh correlation")
}

func TestHandler_StampsCausation_FromContext(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default())

	parent := es.Event{ID: "parent-evt", CorrelationID: "corr-root"}
	ctx := es.WithCausation(context.Background(), parent)
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		evt := es.Event{
			AggregateID: agg.AggregateID(),
			Type:        "OrderPlaced",
			Timestamp:   time.Now(),
			Data:        &orderbookv1.OrderPlaced{OrderId: "order-1"},
		}
		require.NoError(t, agg.Apply(evt))
		return []es.Event{evt}, nil
	})
	require.NoError(t, err)

	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.Len(t, raw, 1)

	assert.NotEmpty(t, raw[0].ID)
	assert.Equal(t, "parent-evt", raw[0].CausationID, "causation = parent event ID")
	assert.Equal(t, "corr-root", raw[0].CorrelationID, "correlation propagates unchanged")
}

func TestHandler_StampsCausation_BatchSharesParent(t *testing.T) {
	// All events emitted by a single command share the same causation
	// (the parent event from ctx) — flat, not chained. The command is the
	// unit of causation; multi-event splits are a serialization detail.
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default())

	parent := es.Event{ID: "parent-evt", CorrelationID: "corr-root"}
	ctx := es.WithCausation(context.Background(), parent)
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		mk := func(id string) es.Event {
			evt := es.Event{
				AggregateID: agg.AggregateID(),
				Type:        "OrderPlaced",
				Timestamp:   time.Now(),
				Data:        &orderbookv1.OrderPlaced{OrderId: id},
			}
			require.NoError(t, agg.Apply(evt))
			return evt
		}
		return []es.Event{mk("order-1"), mk("order-2")}, nil
	})
	require.NoError(t, err)

	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.Len(t, raw, 2)

	assert.NotEqual(t, raw[0].ID, raw[1].ID, "each event gets a distinct ID")
	assert.Equal(t, "parent-evt", raw[0].CausationID)
	assert.Equal(t, "parent-evt", raw[1].CausationID, "second event also caused by parent, not by first")
	assert.Equal(t, "corr-root", raw[0].CorrelationID)
	assert.Equal(t, "corr-root", raw[1].CorrelationID)
}

func TestHandler_WithPublisher(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	pub := &recordingPublisher{}

	handler := es.NewHandler(store, registry, func(id string) *testAggregate {
		a := &testAggregate{}
		a.SetID(id)
		return a
	}, slog.Default()).WithPublisher(pub)

	ctx := context.Background()
	now := time.Now()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	err := handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		evt := es.Event{
			AggregateID: agg.AggregateID(),
			Type:        "OrderPlaced",
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 50,
				PlacedAt: timestamppb.New(now),
			},
		}
		require.NoError(t, agg.Apply(evt))
		return []es.Event{evt}, nil
	})
	require.NoError(t, err)

	require.Len(t, pub.published, 1)
	placed, ok := pub.published[0].Data.(*orderbookv1.OrderPlaced)
	require.True(t, ok)
	assert.Equal(t, "order-1", placed.OrderId)
}
