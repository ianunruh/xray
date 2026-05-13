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

func TestDepthProjection_Empty(t *testing.T) {
	proj := orderbook.NewDepthProjection()
	bids, asks := proj.GetDepth("AAPL", 0)
	assert.Empty(t, bids)
	assert.Empty(t, asks)
}

func TestDepthProjection_Aggregation(t *testing.T) {
	proj := orderbook.NewDepthProjection()
	ctx := context.Background()

	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1500000,
				Quantity: 100,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o2",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1500000,
				Quantity: 50,
				PlacedAt: timestamppb.Now(),
			},
		},
	})
	require.NoError(t, err)

	bids, asks := proj.GetDepth("AAPL", 0)
	assert.Empty(t, bids)
	require.Len(t, asks, 1)
	assert.Equal(t, int64(1500000), asks[0].Price)
	assert.Equal(t, int64(150), asks[0].Quantity)
	assert.Equal(t, int32(2), asks[0].OrderCount)
}

func TestDepthProjection_TradeReducesDepth(t *testing.T) {
	proj := orderbook.NewDepthProjection()
	ctx := context.Background()

	// Place two orders on opposite sides.
	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "buy-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 100,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "sell-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1500000,
				Quantity: 100,
				PlacedAt: timestamppb.Now(),
			},
		},
	})
	require.NoError(t, err)

	// Trade fills both completely.
	err = proj.HandleEvents(ctx, []es.Event{
		{
			Type: "TradeExecuted",
			Data: &orderbookv1.TradeExecuted{
				TradeId:     "t1",
				Symbol:      "AAPL",
				BuyOrderId:  "buy-1",
				SellOrderId: "sell-1",
				Price:       1500000,
				Quantity:    100,
				ExecutedAt:  timestamppb.Now(),
			},
		},
	})
	require.NoError(t, err)

	bids, asks := proj.GetDepth("AAPL", 0)
	assert.Empty(t, bids)
	assert.Empty(t, asks)
}

func TestDepthProjection_PartialFill(t *testing.T) {
	proj := orderbook.NewDepthProjection()
	ctx := context.Background()

	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "sell-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1500000,
				Quantity: 100,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "buy-1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1500000,
				Quantity: 60,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "TradeExecuted",
			Data: &orderbookv1.TradeExecuted{
				TradeId:     "t1",
				Symbol:      "AAPL",
				BuyOrderId:  "buy-1",
				SellOrderId: "sell-1",
				Price:       1500000,
				Quantity:    60,
				ExecutedAt:  timestamppb.Now(),
			},
		},
	})
	require.NoError(t, err)

	bids, asks := proj.GetDepth("AAPL", 0)
	assert.Empty(t, bids) // buy fully filled
	require.Len(t, asks, 1)
	assert.Equal(t, int64(40), asks[0].Quantity) // 100 - 60
	assert.Equal(t, int32(1), asks[0].OrderCount)
}

func TestDepthProjection_Cancel(t *testing.T) {
	proj := orderbook.NewDepthProjection()
	ctx := context.Background()

	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1490000,
				Quantity: 200,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderCancelled",
			Data: &orderbookv1.OrderCancelled{
				OrderId: "o1",
				Symbol:  "AAPL",
			},
		},
	})
	require.NoError(t, err)

	bids, asks := proj.GetDepth("AAPL", 0)
	assert.Empty(t, bids)
	assert.Empty(t, asks)
}

func TestDepthProjection_DepthLimit(t *testing.T) {
	proj := orderbook.NewDepthProjection()
	ctx := context.Background()

	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1500000,
				Quantity: 100,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o2",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1510000,
				Quantity: 50,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o3",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1490000,
				Quantity: 200,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o4",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_BUY,
				Price:    1480000,
				Quantity: 75,
				PlacedAt: timestamppb.Now(),
			},
		},
	})
	require.NoError(t, err)

	bids, asks := proj.GetDepth("AAPL", 1)
	require.Len(t, bids, 1)
	assert.Equal(t, int64(1490000), bids[0].Price) // best bid (highest)
	require.Len(t, asks, 1)
	assert.Equal(t, int64(1500000), asks[0].Price) // best ask (lowest)
}

func TestDepthProjection_SubCentAggregation(t *testing.T) {
	proj := orderbook.NewDepthProjection()
	ctx := context.Background()

	// Two orders at different sub-cent prices that round to the same cent.
	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o1",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1500012, // $150.0012
				Quantity: 100,
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "o2",
				Symbol:   "AAPL",
				Side:     orderbookv1.Side_SIDE_SELL,
				Price:    1500087, // $150.0087
				Quantity: 50,
				PlacedAt: timestamppb.Now(),
			},
		},
	})
	require.NoError(t, err)

	bids, asks := proj.GetDepth("AAPL", 0)
	assert.Empty(t, bids)
	require.Len(t, asks, 1, "sub-cent prices should aggregate to one level")
	assert.Equal(t, int64(1500000), asks[0].Price) // rounded to $150.00
	assert.Equal(t, int64(150), asks[0].Quantity)
	assert.Equal(t, int32(2), asks[0].OrderCount)
}
