package orderbook_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

func TestOrderProjection_Lifecycle(t *testing.T) {
	proj := orderbook.NewOrderProjection()
	ctx := context.Background()

	// Place an order.
	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:     "order-1",
				Symbol:      "AAPL",
				Side:        orderbookv1.Side_SIDE_BUY,
				Price:       1500000,
				Quantity:    100,
				PlacedAt:    timestamppb.Now(),
				OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
				TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
			},
		},
	})
	require.NoError(t, err)

	orders := proj.ListOrders("AAPL")
	require.Len(t, orders, 1)
	assert.Equal(t, "order-1", orders[0].OrderId)
	assert.Equal(t, orderbookv1.OrderStatus_ORDER_STATUS_OPEN, orders[0].Status)
	assert.Equal(t, int64(100), orders[0].RemainingQuantity)

	// Partial fill.
	err = proj.HandleEvents(ctx, []es.Event{
		{
			Type: "TradeExecuted",
			Data: &orderbookv1.TradeExecuted{
				TradeId:     "trade-1",
				Symbol:      "AAPL",
				BuyOrderId:  "order-1",
				SellOrderId: "sell-1",
				Price:       1500000,
				Quantity:    40,
				ExecutedAt:  timestamppb.Now(),
			},
		},
	})
	require.NoError(t, err)

	orders = proj.ListOrders("AAPL")
	require.Len(t, orders, 1)
	assert.Equal(t, orderbookv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED, orders[0].Status)
	assert.Equal(t, int64(60), orders[0].RemainingQuantity)

	// Complete fill.
	err = proj.HandleEvents(ctx, []es.Event{
		{
			Type: "TradeExecuted",
			Data: &orderbookv1.TradeExecuted{
				TradeId:     "trade-2",
				Symbol:      "AAPL",
				BuyOrderId:  "order-1",
				SellOrderId: "sell-2",
				Price:       1500000,
				Quantity:    60,
				ExecutedAt:  timestamppb.Now(),
			},
		},
	})
	require.NoError(t, err)

	orders = proj.ListOrders("AAPL")
	require.Len(t, orders, 1)
	assert.Equal(t, orderbookv1.OrderStatus_ORDER_STATUS_FILLED, orders[0].Status)
	assert.Equal(t, int64(0), orders[0].RemainingQuantity)
}

func TestOrderProjection_Cancelled(t *testing.T) {
	proj := orderbook.NewOrderProjection()
	ctx := context.Background()

	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1510000,
				Quantity: 50,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderCancelled",
			Data: &orderbookv1.OrderCancelled{
				OrderId: "order-1",
				Symbol:  "AAPL",
			},
		},
	})
	require.NoError(t, err)

	orders := proj.ListOrders("AAPL")
	require.Len(t, orders, 1)
	assert.Equal(t, orderbookv1.OrderStatus_ORDER_STATUS_CANCELLED, orders[0].Status)
}

func TestOrderProjection_EmptySymbol(t *testing.T) {
	proj := orderbook.NewOrderProjection()
	assert.Empty(t, proj.ListOrders("AAPL"))
}
