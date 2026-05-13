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

func TestTradeProjection_HandleEvents(t *testing.T) {
	proj := orderbook.NewTradeProjection()
	ctx := context.Background()

	events := []es.Event{
		{
			Type: "TradeExecuted",
			Data: &orderbookv1.TradeExecuted{
				TradeId:     "trade-1",
				Symbol:      "AAPL",
				BuyOrderId:  "buy-1",
				SellOrderId: "sell-1",
				Price:       1500000,
				Quantity:    50,
				ExecutedAt:  timestamppb.Now(),
			},
		},
		{
			Type: "TradeExecuted",
			Data: &orderbookv1.TradeExecuted{
				TradeId:     "trade-2",
				Symbol:      "AAPL",
				BuyOrderId:  "buy-2",
				SellOrderId: "sell-2",
				Price:       1510000,
				Quantity:    25,
				ExecutedAt:  timestamppb.Now(),
			},
		},
	}

	err := proj.HandleEvents(ctx, events)
	require.NoError(t, err)

	trades := proj.ListTrades("AAPL")
	require.Len(t, trades, 2)
	assert.Equal(t, "trade-1", trades[0].TradeId)
	assert.Equal(t, "trade-2", trades[1].TradeId)
	assert.Equal(t, int64(1500000), trades[0].Price)
}

func TestTradeProjection_SymbolFiltering(t *testing.T) {
	proj := orderbook.NewTradeProjection()
	ctx := context.Background()

	events := []es.Event{
		{
			Type: "TradeExecuted",
			Data: &orderbookv1.TradeExecuted{
				TradeId:    "trade-1",
				Symbol:     "AAPL",
				ExecutedAt: timestamppb.Now(),
			},
		},
		{
			Type: "TradeExecuted",
			Data: &orderbookv1.TradeExecuted{
				TradeId:    "trade-2",
				Symbol:     "GOOG",
				ExecutedAt: timestamppb.Now(),
			},
		},
	}

	err := proj.HandleEvents(ctx, events)
	require.NoError(t, err)

	assert.Len(t, proj.ListTrades("AAPL"), 1)
	assert.Len(t, proj.ListTrades("GOOG"), 1)
	assert.Empty(t, proj.ListTrades("MSFT"))
}

func TestTradeProjection_IgnoresNonTradeEvents(t *testing.T) {
	proj := orderbook.NewTradeProjection()
	ctx := context.Background()

	events := []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-1",
				Symbol:   "AAPL",
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
	}

	err := proj.HandleEvents(ctx, events)
	require.NoError(t, err)

	assert.Empty(t, proj.ListTrades("AAPL"))
}
