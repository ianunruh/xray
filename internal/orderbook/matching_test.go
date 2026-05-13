package orderbook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

func placeOrderOnBook(t *testing.T, book *OrderBook, id string, side Side, price, qty int64, placedAt time.Time) {
	t.Helper()
	evt := es.Event{
		Type: "OrderPlaced",
		Data: &orderbookv1.OrderPlaced{
			OrderId:  id,
			Symbol:   book.Symbol,
			Side:     sideToProto(side),
			Price:    price,
			Quantity: qty,
			PlacedAt: timestamppb.New(placedAt),
		},
	}
	require.NoError(t, book.Apply(evt))
}

func newTestBook() *OrderBook {
	book := NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"
	return book
}

func TestMatch_ExactPriceFullFill(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "ask-1", Sell, 1500000, 100, now)

	incoming := book.Orders["ask-1"]
	// Create a buy order that matches exactly
	buyOrder := &Order{
		ID:           "buy-1",
		Side:         Buy,
		Price:        1500000,
		Quantity:     100,
		RemainingQty: 100,
		PlacedAt:     now.Add(time.Second),
	}

	trades := Match(book, buyOrder, now)

	require.Len(t, trades, 1)
	assert.Equal(t, int64(1500000), trades[0].Price)
	assert.Equal(t, int64(100), trades[0].Quantity)
	assert.Equal(t, "buy-1", trades[0].BuyOrderId)
	assert.Equal(t, "ask-1", trades[0].SellOrderId)
	assert.Equal(t, int64(0), buyOrder.RemainingQty)
	assert.Equal(t, int64(0), incoming.RemainingQty)
}

func TestMatch_PriceImprovement(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "ask-1", Sell, 1490000, 50, now)

	buyOrder := &Order{
		ID:           "buy-1",
		Side:         Buy,
		Price:        1500000, // willing to pay more
		Quantity:     50,
		RemainingQty: 50,
		PlacedAt:     now.Add(time.Second),
	}

	trades := Match(book, buyOrder, now)

	require.Len(t, trades, 1)
	assert.Equal(t, int64(1490000), trades[0].Price, "should trade at resting order's price")
}

func TestMatch_PartialFill(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "ask-1", Sell, 1500000, 30, now)

	buyOrder := &Order{
		ID:           "buy-1",
		Side:         Buy,
		Price:        1500000,
		Quantity:     100,
		RemainingQty: 100,
		PlacedAt:     now.Add(time.Second),
	}

	trades := Match(book, buyOrder, now)

	require.Len(t, trades, 1)
	assert.Equal(t, int64(30), trades[0].Quantity)
	assert.Equal(t, int64(70), buyOrder.RemainingQty)
}

func TestMatch_MultipleFills(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "ask-1", Sell, 1490000, 30, now)
	placeOrderOnBook(t, book, "ask-2", Sell, 1500000, 50, now.Add(time.Second))
	placeOrderOnBook(t, book, "ask-3", Sell, 1510000, 40, now.Add(2*time.Second))

	buyOrder := &Order{
		ID:           "buy-1",
		Side:         Buy,
		Price:        1510000, // willing to buy up to 151.00
		Quantity:     100,
		RemainingQty: 100,
		PlacedAt:     now.Add(3 * time.Second),
	}

	trades := Match(book, buyOrder, now)

	require.Len(t, trades, 3)
	// First fill at lowest ask price
	assert.Equal(t, int64(1490000), trades[0].Price)
	assert.Equal(t, int64(30), trades[0].Quantity)
	// Second fill at next ask price
	assert.Equal(t, int64(1500000), trades[1].Price)
	assert.Equal(t, int64(50), trades[1].Quantity)
	// Third fill at highest acceptable price
	assert.Equal(t, int64(1510000), trades[2].Price)
	assert.Equal(t, int64(20), trades[2].Quantity) // only 20 remaining
	assert.Equal(t, int64(0), buyOrder.RemainingQty)
}

func TestMatch_NoMatch(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "ask-1", Sell, 1510000, 100, now)

	buyOrder := &Order{
		ID:           "buy-1",
		Side:         Buy,
		Price:        1500000, // below ask
		Quantity:     100,
		RemainingQty: 100,
		PlacedAt:     now.Add(time.Second),
	}

	trades := Match(book, buyOrder, now)
	assert.Empty(t, trades)
	assert.Equal(t, int64(100), buyOrder.RemainingQty)
}

func TestMatch_SellSide(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "bid-1", Buy, 1500000, 100, now)

	sellOrder := &Order{
		ID:           "sell-1",
		Side:         Sell,
		Price:        1490000, // willing to sell below bid
		Quantity:     60,
		RemainingQty: 60,
		PlacedAt:     now.Add(time.Second),
	}

	trades := Match(book, sellOrder, now)

	require.Len(t, trades, 1)
	assert.Equal(t, int64(1500000), trades[0].Price, "should trade at resting bid's price")
	assert.Equal(t, int64(60), trades[0].Quantity)
	assert.Equal(t, "bid-1", trades[0].BuyOrderId)
	assert.Equal(t, "sell-1", trades[0].SellOrderId)
}
