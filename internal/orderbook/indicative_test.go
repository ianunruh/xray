package orderbook_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
)

func TestComputeIndicative_NilOutsideAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	// Default phase is Continuous — no auction in progress.
	assert.Nil(t, orderbook.ComputeIndicative(book))

	// After a closing uncross flips to Closed, also nil.
	openAuction(t, book)
	runUncross(t, book) // → Continuous
	_, err := orderbook.ExecuteBeginClosingAuction(book, orderbook.BeginClosingAuction{Symbol: "AAPL"})
	require.NoError(t, err)
	runUncross(t, book) // → Closed
	assert.Nil(t, orderbook.ComputeIndicative(book))
}

func TestComputeIndicative_EmptyAuctionBook(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	got := orderbook.ComputeIndicative(book)
	require.NotNil(t, got)
	assert.Equal(t, orderbook.PhaseAuction, got.Phase)
	assert.Equal(t, int64(0), got.ClearingPrice)
	assert.Equal(t, int64(0), got.MatchedQty)
	assert.Equal(t, int64(0), got.ImbalanceQty)
}

func TestComputeIndicative_OneSidedBids(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1500000, 100)
	placeBid(t, book, 1490000, 50)

	got := orderbook.ComputeIndicative(book)
	require.NotNil(t, got)
	assert.Equal(t, int64(0), got.MatchedQty)
	assert.Equal(t, int64(150), got.ImbalanceQty)
	assert.Equal(t, orderbook.Buy, got.ImbalanceSide)
}

func TestComputeIndicative_OneSidedAsks(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeAsk(t, book, 1500000, 80)

	got := orderbook.ComputeIndicative(book)
	require.NotNil(t, got)
	assert.Equal(t, int64(0), got.MatchedQty)
	assert.Equal(t, int64(80), got.ImbalanceQty)
	assert.Equal(t, orderbook.Sell, got.ImbalanceSide)
}

func TestComputeIndicative_CrossedBook_AgreesWithUncross(t *testing.T) {
	// The indicative state computed mid-auction must equal the uncross
	// result if Uncross fired right now. We compute the indicative
	// first, then run the actual uncross and assert they agree.
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1505000, 100)
	placeAsk(t, book, 1500000, 100)

	ind := orderbook.ComputeIndicative(book)
	require.NotNil(t, ind)

	// Now run the real uncross and compare. NB: book mutates here so
	// this must come after the indicative compute. The first event of
	// every uncross batch is the AuctionUncrossed header.
	events := runUncross(t, book)
	header, ok := events[0].Data.(*orderbookv1.AuctionUncrossed)
	require.True(t, ok, "first event must be AuctionUncrossed header")

	assert.Equal(t, ind.ClearingPrice, header.ClearingPrice)
	assert.Equal(t, ind.MatchedQty, header.MatchedQty)
	assert.Equal(t, ind.ImbalanceQty, header.ImbalanceQty)
}

func TestComputeIndicative_TracksClosingAuctionBook(t *testing.T) {
	// Same orders staged via AT_CLOSE during a closing auction should
	// surface on the indicative for the closing book — not the empty
	// opening book.
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	_, err := orderbook.ExecuteBeginClosingAuction(book, orderbook.BeginClosingAuction{Symbol: "AAPL"})
	require.NoError(t, err)

	_, err = orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		OrderType:   orderbook.Limit,
		TimeInForce: orderbook.AtClose,
	})
	require.NoError(t, err)
	_, err = orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Sell,
		Price:       1500000,
		Quantity:    100,
		OrderType:   orderbook.Limit,
		TimeInForce: orderbook.AtClose,
	})
	require.NoError(t, err)

	got := orderbook.ComputeIndicative(book)
	require.NotNil(t, got)
	assert.Equal(t, orderbook.PhaseClosingAuction, got.Phase)
	assert.Equal(t, int64(1500000), got.ClearingPrice)
	assert.Equal(t, int64(100), got.MatchedQty)
}

func TestComputeIndicative_UpdatesAsOrdersArrive(t *testing.T) {
	// Successive calls reflect the current state of the auction book —
	// this is the behavior the stream handler depends on.
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	placeBid(t, book, 1500000, 100)
	first := orderbook.ComputeIndicative(book)
	require.NotNil(t, first)
	assert.Equal(t, int64(100), first.ImbalanceQty)
	assert.Equal(t, orderbook.Buy, first.ImbalanceSide)

	placeAsk(t, book, 1500000, 60)
	second := orderbook.ComputeIndicative(book)
	require.NotNil(t, second)
	assert.Equal(t, int64(60), second.MatchedQty)
	assert.Equal(t, int64(40), second.ImbalanceQty)
	assert.Equal(t, orderbook.Buy, second.ImbalanceSide)

	placeAsk(t, book, 1500000, 50)
	third := orderbook.ComputeIndicative(book)
	require.NotNil(t, third)
	assert.Equal(t, int64(100), third.MatchedQty)
	assert.Equal(t, int64(10), third.ImbalanceQty)
	assert.Equal(t, orderbook.Sell, third.ImbalanceSide)
}

func TestComputeIndicative_NonCrossingBook(t *testing.T) {
	// Best bid below best ask — no equilibrium. Matched 0, imbalance 0.
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1490000, 100)
	placeAsk(t, book, 1510000, 100)

	got := orderbook.ComputeIndicative(book)
	require.NotNil(t, got)
	assert.Equal(t, int64(0), got.MatchedQty)
}
