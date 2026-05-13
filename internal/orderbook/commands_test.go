package orderbook_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTestRegistry() *es.Registry {
	r := es.NewRegistry()
	r.Register("OrderPlaced", func() proto.Message { return new(orderbookv1.OrderPlaced) })
	r.Register("TradeExecuted", func() proto.Message { return new(orderbookv1.TradeExecuted) })
	r.Register("OrderCancelled", func() proto.Message { return new(orderbookv1.OrderCancelled) })
	return r
}

func TestPlaceOrder_ThroughHandler(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	})

	ctx := context.Background()

	// Place a sell order.
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1500000,
		Quantity: 100,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Sell,
			Price:    1500000,
			Quantity: 100,
		})
	})
	require.NoError(t, err)

	// Place a matching buy order.
	err = handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 60,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Buy,
			Price:    1500000,
			Quantity: 60,
		})
	})
	require.NoError(t, err)

	// Verify event stream.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)

	// Should have: OrderPlaced(sell), OrderPlaced(buy), TradeExecuted
	require.Len(t, raw, 3)

	evt0, err := registry.Deserialize(raw[0])
	require.NoError(t, err)
	assert.Equal(t, "OrderPlaced", evt0.Type)
	placed0 := evt0.Data.(*orderbookv1.OrderPlaced)
	assert.Equal(t, orderbookv1.Side_SIDE_SELL, placed0.Side)
	assert.Equal(t, int64(100), placed0.Quantity)

	evt1, err := registry.Deserialize(raw[1])
	require.NoError(t, err)
	assert.Equal(t, "OrderPlaced", evt1.Type)
	placed1 := evt1.Data.(*orderbookv1.OrderPlaced)
	assert.Equal(t, orderbookv1.Side_SIDE_BUY, placed1.Side)
	assert.Equal(t, int64(60), placed1.Quantity)

	evt2, err := registry.Deserialize(raw[2])
	require.NoError(t, err)
	assert.Equal(t, "TradeExecuted", evt2.Type)
	trade := evt2.Data.(*orderbookv1.TradeExecuted)
	assert.Equal(t, int64(1500000), trade.Price)
	assert.Equal(t, int64(60), trade.Quantity)
}

func TestCancelOrder_ThroughHandler(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	})

	ctx := context.Background()

	// Place a sell order first.
	var placedOrderID string
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1500000,
		Quantity: 100,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Sell,
			Price:    1500000,
			Quantity: 100,
		})
		if err != nil {
			return nil, err
		}
		placed := events[0].Data.(*orderbookv1.OrderPlaced)
		placedOrderID = placed.OrderId
		return events, nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, placedOrderID)

	// Cancel it.
	err = handler.Handle(ctx, orderbook.CancelOrder{
		Symbol:  "AAPL",
		OrderID: placedOrderID,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, orderbook.CancelOrder{
			Symbol:  "AAPL",
			OrderID: placedOrderID,
		})
	})
	require.NoError(t, err)

	// Verify event stream has OrderPlaced + OrderCancelled.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.Len(t, raw, 2)

	evt1, err := registry.Deserialize(raw[1])
	require.NoError(t, err)
	assert.Equal(t, "OrderCancelled", evt1.Type)
	cancelled := evt1.Data.(*orderbookv1.OrderCancelled)
	assert.Equal(t, placedOrderID, cancelled.OrderId)
}

func TestCancelOrder_NotFound(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecuteCancelOrder(book, orderbook.CancelOrder{
		Symbol:  "AAPL",
		OrderID: "nonexistent",
	})
	assert.Error(t, err)
}

func TestPlaceOrder_InvalidInput(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    0,
		Quantity: 100,
	})
	assert.Error(t, err)

	_, err = orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 0,
	})
	assert.Error(t, err)
}
