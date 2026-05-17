package es_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

func newTestRegistry() *es.Registry {
	r := es.NewRegistry()
	r.Register("OrderPlaced", func() proto.Message { return new(orderbookv1.OrderPlaced) })
	r.Register("TradeExecuted", func() proto.Message { return new(orderbookv1.TradeExecuted) })
	r.Register("OrderCancelled", func() proto.Message { return new(orderbookv1.OrderCancelled) })
	return r
}

func TestRegistry_RoundTrip(t *testing.T) {
	r := newTestRegistry()

	now := time.Now().Truncate(time.Second)

	original := es.Event{
		ID:            "evt-1",
		CausationID:   "evt-0",
		CorrelationID: "corr-1",
		AggregateID:   "orderbook:AAPL",
		Type:          "OrderPlaced",
		Version:       1,
		Timestamp:     now,
		Data: &orderbookv1.OrderPlaced{
			OrderId:  "order-1",
			Symbol:   "AAPL",
			Side:     orderbookv1.Side_SIDE_BUY,
			Price:    1505000,
			Quantity: 100,
			PlacedAt: timestamppb.New(now),
		},
	}

	raw, err := r.Serialize(original)
	require.NoError(t, err)
	assert.Equal(t, "OrderPlaced", raw.Type)
	assert.NotEmpty(t, raw.Data)
	assert.Equal(t, "evt-1", raw.ID)
	assert.Equal(t, "evt-0", raw.CausationID)
	assert.Equal(t, "corr-1", raw.CorrelationID)

	got, err := r.Deserialize(raw)
	require.NoError(t, err)
	assert.Equal(t, original.Type, got.Type)
	assert.Equal(t, original.AggregateID, got.AggregateID)
	assert.Equal(t, "evt-1", got.ID)
	assert.Equal(t, "evt-0", got.CausationID)
	assert.Equal(t, "corr-1", got.CorrelationID)

	placed, ok := got.Data.(*orderbookv1.OrderPlaced)
	require.True(t, ok, "expected *orderbookv1.OrderPlaced, got %T", got.Data)
	assert.Equal(t, "order-1", placed.OrderId)
	assert.Equal(t, int64(1505000), placed.Price)
}

func TestRegistry_DeserializeUnknownType(t *testing.T) {
	r := newTestRegistry()

	raw := es.RawEvent{
		Type: "UnknownEvent",
		Data: []byte{1, 2, 3},
	}

	_, err := r.Deserialize(raw)
	assert.Error(t, err)
}
