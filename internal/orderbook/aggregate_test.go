package orderbook_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ianunruh/xray/internal/orderbook"
)

func placeAsk(t *testing.T, book *orderbook.OrderBook, price, quantity int64) {
	t.Helper()
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    price,
		Quantity: quantity,
	})
	require.NoError(t, err)
}

func TestEstimateMarketBuyCost_EmptyBook(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))

	cost, ok := book.EstimateMarketBuyCost(100)
	assert.False(t, ok)
	assert.Equal(t, int64(0), cost)
}

func TestEstimateMarketBuyCost_NonPositiveQuantity(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	placeAsk(t, book, 1500000, 100)

	cost, ok := book.EstimateMarketBuyCost(0)
	assert.False(t, ok)
	assert.Equal(t, int64(0), cost)
}

func TestEstimateMarketBuyCost_SingleLevel(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	placeAsk(t, book, 1500000, 100)

	cost, ok := book.EstimateMarketBuyCost(40)
	require.True(t, ok)
	assert.Equal(t, int64(60000000), cost) // 40 * 1,500,000
}

func TestEstimateMarketBuyCost_SweepMultipleLevels(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	placeAsk(t, book, 1500000, 50) // best ask: 50 @ $150
	placeAsk(t, book, 1510000, 50) // next:    50 @ $151
	placeAsk(t, book, 1520000, 50) // next:    50 @ $152

	// Take 50 @ 1,500,000 + 50 @ 1,510,000 = 75,000,000 + 75,500,000.
	cost, ok := book.EstimateMarketBuyCost(100)
	require.True(t, ok)
	assert.Equal(t, int64(150500000), cost)
}

func TestEstimateMarketBuyCost_DepthShortfallExtrapolates(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	placeAsk(t, book, 1500000, 30) // only 30 shares of depth
	placeAsk(t, book, 1510000, 20) // and 20 more at the next level

	// Visible: 30 @ 1,500,000 + 20 @ 1,510,000 = 45,000,000 + 30,200,000.
	// Shortfall of 50 extrapolates at the deepest seen price (1,510,000):
	// 50 * 1,510,000 = 75,500,000. Total = 150,700,000.
	cost, ok := book.EstimateMarketBuyCost(100)
	require.True(t, ok)
	assert.Equal(t, int64(150700000), cost)
}

func TestEstimateMarketBuyCost_PartialFillsAtLevel(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	placeAsk(t, book, 1500000, 100)
	placeAsk(t, book, 1510000, 100)

	// Asking for 150 should consume all 100 at 1,500,000 and 50 at 1,510,000.
	// 100 * 1,500,000 + 50 * 1,510,000 = 150,000,000 + 75,500,000.
	cost, ok := book.EstimateMarketBuyCost(150)
	require.True(t, ok)
	assert.Equal(t, int64(225500000), cost)
}
