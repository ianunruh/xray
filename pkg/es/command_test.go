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

func TestHandler_WithSnapshots(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *snapshotAggregate {
		a := &snapshotAggregate{}
		a.SetID(id)
		return a
	}, slog.Default()).WithSnapshots(store)

	ctx := context.Background()
	now := time.Now()
	cmd := testCommand{aggregateID: "orderbook:AAPL"}

	makeEvent := func(orderID string) es.Event {
		return es.Event{
			AggregateID: "orderbook:AAPL",
			Type:        "OrderPlaced",
			Timestamp:   now,
			Data: &orderbookv1.OrderPlaced{
				OrderId:  orderID,
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 50,
				PlacedAt: timestamppb.New(now),
			},
		}
	}

	// Send 3 commands (SnapshotInterval = 3).
	// Snapshot is captured after the 3rd append (version reaches 3).
	for i := 1; i <= 3; i++ {
		err := handler.Handle(ctx, cmd, func(agg *snapshotAggregate) ([]es.Event, error) {
			evt := makeEvent(fmt.Sprintf("order-%d", i))
			require.NoError(t, agg.Apply(evt))
			return []es.Event{evt}, nil
		})
		require.NoError(t, err)
	}

	// Snapshot was saved after the 3rd handle at version 3.
	snap, err := store.LoadSnapshot(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.NotNil(t, snap, "snapshot should exist after 3rd handle")
	assert.Equal(t, 3, snap.Version)

	// Verify 3 events in store.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Len(t, raw, 3)

	// 4th handle: uses cached aggregate (version 3, orderCount 3).
	err = handler.Handle(ctx, cmd, func(agg *snapshotAggregate) ([]es.Event, error) {
		assert.Equal(t, 3, agg.orderCount, "cached aggregate with 3 events applied")
		return nil, nil
	})
	require.NoError(t, err)
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

type recordingPublisher struct {
	published []es.Event
}

func (p *recordingPublisher) Publish(_ context.Context, events []es.Event) error {
	p.published = append(p.published, events...)
	return nil
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
