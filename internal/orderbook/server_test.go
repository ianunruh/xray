package orderbook_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTestServer(t *testing.T) (orderbookv1connect.OrderBookServiceClient, *httptest.Server) {
	t.Helper()

	registry := newTestRegistry()
	store := memstore.New()

	tradeProjection := orderbook.NewTradeProjection()
	orderProjection := orderbook.NewOrderProjection()

	publisher := es.NewFanOutPublisher(slog.Default(), tradeProjection, orderProjection)

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default()).WithPublisher(publisher)

	srv := orderbook.NewServer(handler, slog.Default(), tradeProjection, orderProjection)

	mux := http.NewServeMux()
	path, h := orderbookv1connect.NewOrderBookServiceHandler(srv)
	mux.Handle(path, h)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	client := orderbookv1connect.NewOrderBookServiceClient(ts.Client(), ts.URL)
	return client, ts
}

func TestServer_PlaceOrder_NoMatch(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	resp, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1505000,
		Quantity: 100,
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Msg.OrderId)
	assert.Empty(t, resp.Msg.Trades)
}

func TestServer_PlaceOrder_WithMatch(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place a sell order.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1500000,
		Quantity: 100,
	}))
	require.NoError(t, err)

	// Place a matching buy order.
	resp, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_BUY,
		Price:    1500000,
		Quantity: 60,
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Msg.OrderId)
	require.Len(t, resp.Msg.Trades, 1)
	assert.Equal(t, int64(1500000), resp.Msg.Trades[0].Price)
	assert.Equal(t, int64(60), resp.Msg.Trades[0].Quantity)
}

func TestServer_PlaceOrder_InvalidInput(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_BUY,
		Price:    0,
		Quantity: 100,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_BUY,
		Price:    1500000,
		Quantity: 0,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestServer_CancelOrder_Success(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place an order first.
	placeResp, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1505000,
		Quantity: 100,
	}))
	require.NoError(t, err)

	// Cancel it.
	_, err = client.CancelOrder(ctx, connect.NewRequest(&orderbookv1.CancelOrderRequest{
		Symbol:  "AAPL",
		OrderId: placeResp.Msg.OrderId,
	}))
	require.NoError(t, err)

	// Verify it's gone from the book.
	bookResp, err := client.GetOrderBook(ctx, connect.NewRequest(&orderbookv1.GetOrderBookRequest{
		Symbol: "AAPL",
	}))
	require.NoError(t, err)
	assert.Empty(t, bookResp.Msg.Asks)
}

func TestServer_CancelOrder_NotFound(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	_, err := client.CancelOrder(ctx, connect.NewRequest(&orderbookv1.CancelOrderRequest{
		Symbol:  "AAPL",
		OrderId: "nonexistent",
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestServer_GetOrderBook_Empty(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	resp, err := client.GetOrderBook(ctx, connect.NewRequest(&orderbookv1.GetOrderBookRequest{
		Symbol: "AAPL",
	}))
	require.NoError(t, err)
	assert.Equal(t, "AAPL", resp.Msg.Symbol)
	assert.Empty(t, resp.Msg.Bids)
	assert.Empty(t, resp.Msg.Asks)
}

func TestServer_GetOrderBook_WithOrders(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place a sell and a buy.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1510000,
		Quantity: 50,
	}))
	require.NoError(t, err)

	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_BUY,
		Price:    1490000,
		Quantity: 200,
	}))
	require.NoError(t, err)

	resp, err := client.GetOrderBook(ctx, connect.NewRequest(&orderbookv1.GetOrderBookRequest{
		Symbol: "AAPL",
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Bids, 1)
	assert.Equal(t, int64(1490000), resp.Msg.Bids[0].Price)
	assert.Equal(t, int64(200), resp.Msg.Bids[0].RemainingQuantity)
	require.Len(t, resp.Msg.Asks, 1)
	assert.Equal(t, int64(1510000), resp.Msg.Asks[0].Price)
	assert.Equal(t, int64(50), resp.Msg.Asks[0].RemainingQuantity)
}

func TestServer_GetOrder_Success(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	placeResp, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_BUY,
		Price:    1495000,
		Quantity: 100,
	}))
	require.NoError(t, err)

	resp, err := client.GetOrder(ctx, connect.NewRequest(&orderbookv1.GetOrderRequest{
		Symbol:  "AAPL",
		OrderId: placeResp.Msg.OrderId,
	}))
	require.NoError(t, err)
	assert.Equal(t, placeResp.Msg.OrderId, resp.Msg.OrderId)
	assert.Equal(t, "AAPL", resp.Msg.Symbol)
	assert.Equal(t, orderbookv1.Side_SIDE_BUY, resp.Msg.Side)
	assert.Equal(t, int64(1495000), resp.Msg.Price)
	assert.Equal(t, int64(100), resp.Msg.Quantity)
	assert.Equal(t, int64(100), resp.Msg.RemainingQuantity)
}

func TestServer_GetOrder_NotFound(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	_, err := client.GetOrder(ctx, connect.NewRequest(&orderbookv1.GetOrderRequest{
		Symbol:  "AAPL",
		OrderId: "nonexistent",
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestServer_PlaceOrder_MarketOrder(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place a resting sell order.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1500000,
		Quantity: 100,
	}))
	require.NoError(t, err)

	// Place a market buy that sweeps it.
	resp, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:    "AAPL",
		Side:      orderbookv1.Side_SIDE_BUY,
		Quantity:  100,
		OrderType: orderbookv1.OrderType_ORDER_TYPE_MARKET,
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Msg.OrderId)
	require.Len(t, resp.Msg.Trades, 1)
	assert.Equal(t, int64(1500000), resp.Msg.Trades[0].Price)
	assert.Equal(t, int64(100), resp.Msg.Trades[0].Quantity)

	// Verify the order is visible via GetOrder.
	orderResp, err := client.GetOrder(ctx, connect.NewRequest(&orderbookv1.GetOrderRequest{
		Symbol:  "AAPL",
		OrderId: resp.Msg.OrderId,
	}))
	require.NoError(t, err)
	assert.Equal(t, orderbookv1.OrderType_ORDER_TYPE_MARKET, orderResp.Msg.OrderType)
	assert.Equal(t, orderbookv1.TimeInForce_TIME_IN_FORCE_IOC, orderResp.Msg.TimeInForce)
}

func TestServer_PlaceOrder_IOC(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place a resting sell order for 50.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1500000,
		Quantity: 50,
	}))
	require.NoError(t, err)

	// IOC buy for 100 — fills 50, cancels remaining 50.
	resp, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:      "AAPL",
		Side:        orderbookv1.Side_SIDE_BUY,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_IOC,
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Trades, 1)
	assert.Equal(t, int64(50), resp.Msg.Trades[0].Quantity)

	// The book should have no bids remaining (IOC cancelled the rest).
	bookResp, err := client.GetOrderBook(ctx, connect.NewRequest(&orderbookv1.GetOrderBookRequest{
		Symbol: "AAPL",
	}))
	require.NoError(t, err)
	assert.Empty(t, bookResp.Msg.Bids)
	assert.Empty(t, bookResp.Msg.Asks)
}

func TestServer_PlaceOrder_FOK_Rejected(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place a resting sell for 50.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1500000,
		Quantity: 50,
	}))
	require.NoError(t, err)

	// FOK buy for 100 — insufficient liquidity.
	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:      "AAPL",
		Side:        orderbookv1.Side_SIDE_BUY,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_FOK,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

func TestServer_PlaceOrder_MarketGTC_Rejected(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:      "AAPL",
		Side:        orderbookv1.Side_SIDE_BUY,
		Quantity:    100,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_MARKET,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestServer_ListTrades(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place a sell then a matching buy to generate a trade.
	_, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1500000,
		Quantity: 100,
	}))
	require.NoError(t, err)

	_, err = client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_BUY,
		Price:    1500000,
		Quantity: 60,
	}))
	require.NoError(t, err)

	resp, err := client.ListTrades(ctx, connect.NewRequest(&orderbookv1.ListTradesRequest{
		Symbol: "AAPL",
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Trades, 1)
	assert.Equal(t, int64(1500000), resp.Msg.Trades[0].Price)
	assert.Equal(t, int64(60), resp.Msg.Trades[0].Quantity)
	assert.Equal(t, "AAPL", resp.Msg.Trades[0].Symbol)

	// Different symbol has no trades.
	resp, err = client.ListTrades(ctx, connect.NewRequest(&orderbookv1.ListTradesRequest{
		Symbol: "GOOG",
	}))
	require.NoError(t, err)
	assert.Empty(t, resp.Msg.Trades)
}

func TestServer_ListOrders(t *testing.T) {
	client, _ := newTestServer(t)
	ctx := context.Background()

	// Place an order.
	placeResp, err := client.PlaceOrder(ctx, connect.NewRequest(&orderbookv1.PlaceOrderRequest{
		Symbol:   "AAPL",
		Side:     orderbookv1.Side_SIDE_SELL,
		Price:    1505000,
		Quantity: 100,
	}))
	require.NoError(t, err)

	resp, err := client.ListOrders(ctx, connect.NewRequest(&orderbookv1.ListOrdersRequest{
		Symbol: "AAPL",
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.Orders, 1)
	assert.Equal(t, placeResp.Msg.OrderId, resp.Msg.Orders[0].OrderId)
	assert.Equal(t, orderbookv1.OrderStatus_ORDER_STATUS_OPEN, resp.Msg.Orders[0].Status)
	assert.Equal(t, int64(100), resp.Msg.Orders[0].RemainingQuantity)

	// Different symbol has no orders.
	resp, err = client.ListOrders(ctx, connect.NewRequest(&orderbookv1.ListOrdersRequest{
		Symbol: "GOOG",
	}))
	require.NoError(t, err)
	assert.Empty(t, resp.Msg.Orders)
}
