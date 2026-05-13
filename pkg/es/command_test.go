package es_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
