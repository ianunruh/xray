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
		return []es.Event{
			{
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
			},
		}, nil
	})
	require.NoError(t, err)

	// Second command: verify aggregate was rehydrated with the first event.
	err = handler.Handle(ctx, cmd, func(agg *testAggregate) ([]es.Event, error) {
		assert.Equal(t, 1, agg.orderCount)
		return []es.Event{
			{
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
			},
		}, nil
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
	// Snapshot is saved during load, so it triggers on the NEXT handle after
	// crossing the threshold.
	for i := 1; i <= 3; i++ {
		err := handler.Handle(ctx, cmd, func(agg *snapshotAggregate) ([]es.Event, error) {
			return []es.Event{makeEvent(fmt.Sprintf("order-%d", i))}, nil
		})
		require.NoError(t, err)
	}

	// No snapshot yet — the snapshot is saved on the next load that sees 3 events.
	snap, err := store.LoadSnapshot(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Nil(t, snap, "snapshot is saved lazily on next load")

	// 4th handle: loads 3 events, which crosses the threshold → saves snapshot at version 3.
	err = handler.Handle(ctx, cmd, func(agg *snapshotAggregate) ([]es.Event, error) {
		assert.Equal(t, 3, agg.orderCount, "loaded from 3 events")
		return []es.Event{makeEvent("order-4")}, nil
	})
	require.NoError(t, err)

	// Verify snapshot was saved at version 3.
	snap, err = store.LoadSnapshot(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.NotNil(t, snap, "snapshot should exist after 4th handle")
	assert.Equal(t, 3, snap.Version)

	// Verify 4 events in store.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	assert.Len(t, raw, 4)

	// 5th handle: restores from snapshot(3) + replays event 4.
	err = handler.Handle(ctx, cmd, func(agg *snapshotAggregate) ([]es.Event, error) {
		assert.Equal(t, 4, agg.orderCount, "snapshot(3) + 1 replayed event")
		return nil, nil
	})
	require.NoError(t, err)
}
