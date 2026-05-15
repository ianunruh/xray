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
			Side:     SideToProto(side),
			Price:    price,
			Quantity: qty,
			PlacedAt: timestamppb.New(placedAt),
		},
	}
	require.NoError(t, book.Apply(evt))
}

func placeOrderOnBookWithAccount(t *testing.T, book *OrderBook, id, accountID string, side Side, price, qty int64, placedAt time.Time) {
	t.Helper()
	evt := es.Event{
		Type: "OrderPlaced",
		Data: &orderbookv1.OrderPlaced{
			OrderId:   id,
			AccountId: accountID,
			Symbol:    book.Symbol,
			Side:      SideToProto(side),
			Price:     price,
			Quantity:  qty,
			PlacedAt:  timestamppb.New(placedAt),
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

	buyOrder := &Order{
		ID:           "buy-1",
		Side:         Buy,
		Price:        1500000,
		Quantity:     100,
		RemainingQty: 100,
		PlacedAt:     now.Add(time.Second),
	}

	result := Match(book, buyOrder, now)

	require.Len(t, result.Trades, 1)
	assert.False(t, result.SelfTradePrevented)
	assert.Equal(t, int64(1500000), result.Trades[0].Price)
	assert.Equal(t, int64(100), result.Trades[0].Quantity)
	assert.Equal(t, "buy-1", result.Trades[0].BuyOrderId)
	assert.Equal(t, "ask-1", result.Trades[0].SellOrderId)
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

	result := Match(book, buyOrder, now)

	require.Len(t, result.Trades, 1)
	assert.Equal(t, int64(1490000), result.Trades[0].Price, "should trade at resting order's price")
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

	result := Match(book, buyOrder, now)

	require.Len(t, result.Trades, 1)
	assert.Equal(t, int64(30), result.Trades[0].Quantity)
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

	result := Match(book, buyOrder, now)

	require.Len(t, result.Trades, 3)
	// First fill at lowest ask price
	assert.Equal(t, int64(1490000), result.Trades[0].Price)
	assert.Equal(t, int64(30), result.Trades[0].Quantity)
	// Second fill at next ask price
	assert.Equal(t, int64(1500000), result.Trades[1].Price)
	assert.Equal(t, int64(50), result.Trades[1].Quantity)
	// Third fill at highest acceptable price
	assert.Equal(t, int64(1510000), result.Trades[2].Price)
	assert.Equal(t, int64(20), result.Trades[2].Quantity) // only 20 remaining
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

	result := Match(book, buyOrder, now)
	assert.Empty(t, result.Trades)
	assert.False(t, result.SelfTradePrevented)
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

	result := Match(book, sellOrder, now)

	require.Len(t, result.Trades, 1)
	assert.Equal(t, int64(1500000), result.Trades[0].Price, "should trade at resting bid's price")
	assert.Equal(t, int64(60), result.Trades[0].Quantity)
	assert.Equal(t, "bid-1", result.Trades[0].BuyOrderId)
	assert.Equal(t, "sell-1", result.Trades[0].SellOrderId)
}

func TestMatch_MarketBuySweepsAllAsks(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "ask-1", Sell, 1490000, 30, now)
	placeOrderOnBook(t, book, "ask-2", Sell, 1500000, 50, now.Add(time.Second))
	placeOrderOnBook(t, book, "ask-3", Sell, 1600000, 40, now.Add(2*time.Second))

	buyOrder := &Order{
		ID:           "buy-1",
		Side:         Buy,
		Price:        0, // market order — no price limit
		Quantity:     120,
		RemainingQty: 120,
		PlacedAt:     now.Add(3 * time.Second),
		OrderType:    Market,
	}

	result := Match(book, buyOrder, now)

	require.Len(t, result.Trades, 3)
	assert.Equal(t, int64(1490000), result.Trades[0].Price)
	assert.Equal(t, int64(30), result.Trades[0].Quantity)
	assert.Equal(t, int64(1500000), result.Trades[1].Price)
	assert.Equal(t, int64(50), result.Trades[1].Quantity)
	assert.Equal(t, int64(1600000), result.Trades[2].Price)
	assert.Equal(t, int64(40), result.Trades[2].Quantity)
}

func TestMatch_MarketSellSweepsAllBids(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "bid-1", Buy, 1510000, 40, now)
	placeOrderOnBook(t, book, "bid-2", Buy, 1500000, 60, now.Add(time.Second))
	placeOrderOnBook(t, book, "bid-3", Buy, 1400000, 20, now.Add(2*time.Second))

	sellOrder := &Order{
		ID:           "sell-1",
		Side:         Sell,
		Price:        0,
		Quantity:     100,
		RemainingQty: 100,
		PlacedAt:     now.Add(3 * time.Second),
		OrderType:    Market,
	}

	result := Match(book, sellOrder, now)

	require.Len(t, result.Trades, 2)
	assert.Equal(t, int64(1510000), result.Trades[0].Price)
	assert.Equal(t, int64(40), result.Trades[0].Quantity)
	assert.Equal(t, int64(1500000), result.Trades[1].Price)
	assert.Equal(t, int64(60), result.Trades[1].Quantity)
}

// Self-trade prevention tests

func TestMatch_SelfTradePrevention_CancelIncoming(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBookWithAccount(t, book, "ask-1", "acct-1", Sell, 1500000, 100, now)

	buyOrder := &Order{
		ID:           "buy-1",
		AccountID:    "acct-1",
		Side:         Buy,
		Price:        1500000,
		Quantity:     100,
		RemainingQty: 100,
	}

	result := Match(book, buyOrder, now)

	assert.Empty(t, result.Trades)
	assert.True(t, result.SelfTradePrevented)
}

func TestMatch_SelfTradePrevention_PartialFillThenSelfTrade(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBookWithAccount(t, book, "ask-1", "acct-2", Sell, 1490000, 30, now)
	placeOrderOnBookWithAccount(t, book, "ask-2", "acct-1", Sell, 1500000, 50, now.Add(time.Second))
	placeOrderOnBookWithAccount(t, book, "ask-3", "acct-3", Sell, 1510000, 40, now.Add(2*time.Second))

	buyOrder := &Order{
		ID:           "buy-1",
		AccountID:    "acct-1",
		Side:         Buy,
		Price:        1510000,
		Quantity:     100,
		RemainingQty: 100,
	}

	result := Match(book, buyOrder, now)

	require.Len(t, result.Trades, 1)
	assert.True(t, result.SelfTradePrevented)
	assert.Equal(t, "ask-1", result.Trades[0].SellOrderId)
	assert.Equal(t, int64(30), result.Trades[0].Quantity)
}

func TestMatch_SelfTradePrevention_EmptyAccountID_NoEffect(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBook(t, book, "ask-1", Sell, 1500000, 100, now)

	buyOrder := &Order{
		ID:           "buy-1",
		Side:         Buy,
		Price:        1500000,
		Quantity:     100,
		RemainingQty: 100,
	}

	result := Match(book, buyOrder, now)

	require.Len(t, result.Trades, 1)
	assert.False(t, result.SelfTradePrevented)
}

func TestMatch_SelfTradePrevention_DifferentAccounts(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBookWithAccount(t, book, "ask-1", "acct-2", Sell, 1500000, 100, now)

	buyOrder := &Order{
		ID:           "buy-1",
		AccountID:    "acct-1",
		Side:         Buy,
		Price:        1500000,
		Quantity:     100,
		RemainingQty: 100,
	}

	result := Match(book, buyOrder, now)

	require.Len(t, result.Trades, 1)
	assert.False(t, result.SelfTradePrevented)
}

func TestMatch_SelfTradePrevention_SellSide(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBookWithAccount(t, book, "bid-1", "acct-1", Buy, 1500000, 100, now)

	sellOrder := &Order{
		ID:           "sell-1",
		AccountID:    "acct-1",
		Side:         Sell,
		Price:        1500000,
		Quantity:     100,
		RemainingQty: 100,
	}

	result := Match(book, sellOrder, now)

	assert.Empty(t, result.Trades)
	assert.True(t, result.SelfTradePrevented)
}

func TestAvailableQty_ExcludesSameAccount(t *testing.T) {
	book := newTestBook()
	now := time.Now()

	placeOrderOnBookWithAccount(t, book, "ask-1", "acct-2", Sell, 1490000, 30, now)
	placeOrderOnBookWithAccount(t, book, "ask-2", "acct-1", Sell, 1500000, 50, now.Add(time.Second))
	placeOrderOnBookWithAccount(t, book, "ask-3", "acct-3", Sell, 1510000, 40, now.Add(2*time.Second))

	// Without exclusion: all 120 shares available up to 1510000
	avail := AvailableQty(book, Buy, 1510000, false, "")
	assert.Equal(t, int64(120), avail)

	// With exclusion: stops at acct-1's order, only 30 from acct-2
	avail = AvailableQty(book, Buy, 1510000, false, "acct-1")
	assert.Equal(t, int64(30), avail)
}
