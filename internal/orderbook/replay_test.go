package orderbook_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

func TestServer_GetReplayBounds_Empty(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	resp, err := client.GetReplayBounds(ctx, connect.NewRequest(&orderbookv1.GetReplayBoundsRequest{
		Symbol: "AAPL",
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(0), resp.Msg.FirstVersion)
	assert.Equal(t, int32(0), resp.Msg.LastVersion)
	assert.Nil(t, resp.Msg.FirstTimestamp)
}

func TestServer_GetReplayBounds_AfterTrades(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place a sell that rests, then a buy that crosses it — both
	// generate events on the same aggregate.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_SELL, Price: 1500000, Quantity: 100,
	}))
	require.NoError(t, err)
	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_BUY, Price: 1500000, Quantity: 30,
	}))
	require.NoError(t, err)

	resp, err := client.GetReplayBounds(ctx, connect.NewRequest(&orderbookv1.GetReplayBoundsRequest{
		Symbol: "AAPL",
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.Msg.FirstVersion)
	// 2 OrderPlaced + 1 TradeExecuted = 3 events.
	assert.Equal(t, int32(3), resp.Msg.LastVersion)
	require.NotNil(t, resp.Msg.FirstTimestamp)
	require.NotNil(t, resp.Msg.LastTimestamp)
	assert.False(t, resp.Msg.LastTimestamp.AsTime().Before(resp.Msg.FirstTimestamp.AsTime()))
}

func TestServer_ReplayOrderBook_ByVersion(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place a resting sell at 150.00.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_SELL, Price: 1500000, Quantity: 100,
	}))
	require.NoError(t, err)

	// Place a resting sell at 150.50 — book now has two ask levels.
	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_SELL, Price: 1505000, Quantity: 50,
	}))
	require.NoError(t, err)

	// Cross with a buy that fully consumes the first sell.
	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_BUY, Price: 1500000, Quantity: 100,
	}))
	require.NoError(t, err)

	// Replay at version=1: should show only the first sell, no trades.
	replay, err := client.ReplayOrderBook(ctx, connect.NewRequest(&orderbookv1.ReplayOrderBookRequest{
		Symbol: "AAPL",
		At:     &orderbookv1.ReplayOrderBookRequest_AtVersion{AtVersion: 1},
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(1), replay.Msg.AtVersion)
	assert.Empty(t, replay.Msg.Bids)
	require.Len(t, replay.Msg.Asks, 1)
	assert.Equal(t, int64(1500000), replay.Msg.Asks[0].Price)
	assert.Equal(t, int64(100), replay.Msg.Asks[0].Quantity)
	assert.Empty(t, replay.Msg.RecentTrades)

	// Replay at version=2: both asks resting, no trades yet.
	replay, err = client.ReplayOrderBook(ctx, connect.NewRequest(&orderbookv1.ReplayOrderBookRequest{
		Symbol: "AAPL",
		At:     &orderbookv1.ReplayOrderBookRequest_AtVersion{AtVersion: 2},
	}))
	require.NoError(t, err)
	require.Len(t, replay.Msg.Asks, 2)
	assert.Empty(t, replay.Msg.RecentTrades)
}

func TestServer_ReplayOrderBook_ByTimestamp(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place an order, capture the bounds time, then place a second.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_SELL, Price: 1500000, Quantity: 100,
	}))
	require.NoError(t, err)

	bounds1, err := client.GetReplayBounds(ctx, connect.NewRequest(&orderbookv1.GetReplayBoundsRequest{Symbol: "AAPL"}))
	require.NoError(t, err)
	mid := bounds1.Msg.LastTimestamp.AsTime().Add(500 * time.Millisecond)

	// Ensure the next event lands at a strictly later timestamp than `mid`.
	time.Sleep(1500 * time.Millisecond)
	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_SELL, Price: 1505000, Quantity: 50,
	}))
	require.NoError(t, err)

	// Replay at `mid` — between the two placements. Should see only the
	// first ask.
	replay, err := client.ReplayOrderBook(ctx, connect.NewRequest(&orderbookv1.ReplayOrderBookRequest{
		Symbol: "AAPL",
		At:     &orderbookv1.ReplayOrderBookRequest_AtTimestamp{AtTimestamp: timestamppb.New(mid)},
	}))
	require.NoError(t, err)
	assert.Equal(t, int32(1), replay.Msg.AtVersion)
	require.Len(t, replay.Msg.Asks, 1)
	assert.Equal(t, int64(1500000), replay.Msg.Asks[0].Price)
}

func TestServer_ReplayOrderBook_RecentTrades(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Resting sell at 150.00, then two buys that cross it (two trades).
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_SELL, Price: 1500000, Quantity: 100,
	}))
	require.NoError(t, err)
	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_BUY, Price: 1500000, Quantity: 30,
	}))
	require.NoError(t, err)
	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol: "AAPL", Side: orderbookv1.Side_SIDE_BUY, Price: 1500000, Quantity: 20,
	}))
	require.NoError(t, err)

	bounds, err := client.GetReplayBounds(ctx, connect.NewRequest(&orderbookv1.GetReplayBoundsRequest{Symbol: "AAPL"}))
	require.NoError(t, err)

	replay, err := client.ReplayOrderBook(ctx, connect.NewRequest(&orderbookv1.ReplayOrderBookRequest{
		Symbol: "AAPL",
		At:     &orderbookv1.ReplayOrderBookRequest_AtVersion{AtVersion: bounds.Msg.LastVersion},
	}))
	require.NoError(t, err)

	// Two TradeExecuted events should land in recent_trades, in oldest-first order.
	require.Len(t, replay.Msg.RecentTrades, 2)
	assert.Equal(t, int64(30), replay.Msg.RecentTrades[0].Quantity)
	assert.Equal(t, int64(20), replay.Msg.RecentTrades[1].Quantity)

	// The resting sell has 50 remaining — one ask level remains.
	require.Len(t, replay.Msg.Asks, 1)
	assert.Equal(t, int64(50), replay.Msg.Asks[0].Quantity)
}
