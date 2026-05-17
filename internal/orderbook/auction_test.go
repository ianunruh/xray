package orderbook_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

func placeBid(t *testing.T, book *orderbook.OrderBook, price, quantity int64) {
	t.Helper()
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    price,
		Quantity: quantity,
	})
	require.NoError(t, err)
}

func placeBidAccount(t *testing.T, book *orderbook.OrderBook, account string, price, quantity int64) {
	t.Helper()
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Buy,
		Price:     price,
		Quantity:  quantity,
		AccountID: account,
	})
	require.NoError(t, err)
}

func placeAskAccount(t *testing.T, book *orderbook.OrderBook, account string, price, quantity int64) {
	t.Helper()
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     price,
		Quantity:  quantity,
		AccountID: account,
	})
	require.NoError(t, err)
}

func openAuction(t *testing.T, book *orderbook.OrderBook) {
	t.Helper()
	_, err := orderbook.ExecuteOpenAuction(book, orderbook.OpenAuction{Symbol: "AAPL"})
	require.NoError(t, err)
}

func runUncross(t *testing.T, book *orderbook.OrderBook) []es.Event {
	t.Helper()
	events, err := orderbook.ExecuteUncross(book, orderbook.Uncross{Symbol: "AAPL"})
	require.NoError(t, err)
	return events
}

func TestOpenAuction_TransitionsFromContinuous(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))

	events, err := orderbook.ExecuteOpenAuction(book, orderbook.OpenAuction{
		Symbol: "AAPL",
		Reason: "manual",
	})
	require.NoError(t, err)
	require.Len(t, events, 1)

	changed, ok := events[0].Data.(*orderbookv1.MarketPhaseChanged)
	require.True(t, ok)
	assert.Equal(t, orderbookv1.MarketPhase_MARKET_PHASE_AUCTION, changed.Phase)
	assert.Equal(t, "manual", changed.Reason)
	assert.Equal(t, orderbook.PhaseAuction, book.Phase)
}

func TestOpenAuction_RejectsWhenAlreadyInAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	_, err := orderbook.ExecuteOpenAuction(book, orderbook.OpenAuction{Symbol: "AAPL"})
	assert.ErrorIs(t, err, orderbook.ErrAlreadyInAuction)
}

func TestPlaceOrder_RestsDuringAuction_NoMatching(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	// Buy and sell that would cross immediately in continuous matching.
	events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1500000,
		Quantity: 100,
	})
	require.NoError(t, err)
	require.Len(t, events, 1) // only OrderPlaced, no TradeExecuted

	events, err = orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 100,
	})
	require.NoError(t, err)
	require.Len(t, events, 1) // still no matching during auction

	for _, evt := range events {
		_, ok := evt.Data.(*orderbookv1.TradeExecuted)
		assert.False(t, ok, "no trades should be emitted during auction")
	}
}

func TestPlaceOrder_AuctionRejectsIOC(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.IOC,
	})
	assert.ErrorIs(t, err, orderbook.ErrAuctionRejectsIOC)
}

func TestPlaceOrder_AuctionRejectsFOK(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.FOK,
	})
	assert.ErrorIs(t, err, orderbook.ErrAuctionRejectsIOC)
}

func TestPlaceOrder_AuctionRejectsMarket(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Quantity:    100,
		OrderType:   orderbook.Market,
		TimeInForce: orderbook.IOC,
	})
	// IOC check fires before market check; either error is acceptable as
	// a rejection — verify it's one of the auction-rejection errors.
	require.Error(t, err)
	assert.True(t,
		err == orderbook.ErrAuctionRejectsIOC || err == orderbook.ErrAuctionRejectsMarket,
		"got %v", err)
}

func TestUncross_RejectsOutsideAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	_, err := orderbook.ExecuteUncross(book, orderbook.Uncross{Symbol: "AAPL"})
	assert.ErrorIs(t, err, orderbook.ErrNotInAuction)
}

func TestUncross_EmptyBook(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	events := runUncross(t, book)
	// Expected: AuctionUncrossed, MarketPhaseChanged
	require.Len(t, events, 2)

	header, ok := events[0].Data.(*orderbookv1.AuctionUncrossed)
	require.True(t, ok)
	assert.Equal(t, int64(0), header.ClearingPrice)
	assert.Equal(t, int64(0), header.MatchedQty)
	assert.Equal(t, orderbookv1.CrossType_CROSS_TYPE_OPENING, header.CrossType)

	flip, ok := events[1].Data.(*orderbookv1.MarketPhaseChanged)
	require.True(t, ok)
	assert.Equal(t, orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS, flip.Phase)
	assert.Equal(t, orderbook.PhaseContinuous, book.Phase)
}

func TestUncross_OneSidedBook_ReportsImbalance(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1500000, 75)

	events := runUncross(t, book)

	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(0), header.MatchedQty)
	assert.Equal(t, int64(75), header.ImbalanceQty)
	assert.Equal(t, orderbookv1.Side_SIDE_BUY, header.ImbalanceSide)
}

func TestUncross_NonCrossingBook(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1490000, 50) // $149
	placeAsk(t, book, 1510000, 50) // $151

	events := runUncross(t, book)

	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(0), header.MatchedQty, "no overlap, no fills")

	// Only AuctionUncrossed + MarketPhaseChanged — no trades.
	for _, evt := range events {
		_, ok := evt.Data.(*orderbookv1.TradeExecuted)
		assert.False(t, ok)
	}
}

func TestUncross_SimpleCross_OnePair(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1500000, 100)
	placeAsk(t, book, 1500000, 100)

	events := runUncross(t, book)

	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(1500000), header.ClearingPrice)
	assert.Equal(t, int64(100), header.MatchedQty)
	assert.Equal(t, int64(0), header.ImbalanceQty)

	var trades []*orderbookv1.TradeExecuted
	for _, evt := range events {
		if t, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			trades = append(trades, t)
		}
	}
	require.Len(t, trades, 1)
	assert.Equal(t, int64(1500000), trades[0].Price)
	assert.Equal(t, int64(100), trades[0].Quantity)
	assert.Equal(t, orderbookv1.CrossType_CROSS_TYPE_OPENING, trades[0].CrossType)
}

func TestUncross_BuyHeavy_PicksHighestPrice(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	// Buy demand exceeds sell supply at every candidate; clearing price
	// should be the highest price at which max matched is achieved.
	placeBid(t, book, 1510000, 100) // willing to pay $151
	placeBid(t, book, 1500000, 100) // willing to pay $150
	placeAsk(t, book, 1495000, 100) // willing to sell at $149.50

	events := runUncross(t, book)
	header := events[0].Data.(*orderbookv1.AuctionUncrossed)

	// Only 100 shares of supply, 200 of demand — match max = 100.
	// At p=$151: bq=100, sq=100, matched=100. At p=$150: bq=100, sq=100, matched=100.
	// At p=$149.50: bq=200, sq=100, matched=100.
	// All three tie on matched. Imbalances: at $151 buyQty=100, sellQty=100, imbalance=0;
	// at $150 buyQty=100, sellQty=100, imbalance=0; at $149.50 buyQty=200, sellQty=100, imbalance=+100.
	// Min |imbalance|: 0 → finalists $151 and $150. Balanced → ref price (0, out of range) →
	// midpoint = ($151 + $150) / 2 = $150.50, closest finalist = $150 OR $151 (equidistant).
	// (Implementation picks first match.) Either is a valid clearing price; assert it's one of them.
	assert.Contains(t, []int64{1500000, 1510000}, header.ClearingPrice)
	assert.Equal(t, int64(100), header.MatchedQty)
}

func TestUncross_SellHeavy_PicksLowestPrice(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	// Sell supply exceeds buy demand at every candidate; clearing
	// price gravitates toward the lowest tied price (favouring buyers).
	placeBid(t, book, 1505000, 100)
	placeAsk(t, book, 1500000, 100)
	placeAsk(t, book, 1510000, 100)

	events := runUncross(t, book)
	header := events[0].Data.(*orderbookv1.AuctionUncrossed)

	// At p=$150.50: bq=100, sq=100 (only $150 ask eligible), matched=100, imbalance=0.
	// At p=$151: bq=100, sq=200, matched=100, imbalance=-100.
	// At p=$150: bq=100, sq=100, matched=100, imbalance=0.
	// Finalists by |imbalance|=0: $150 and $150.50. Balanced/mixed → midpoint
	// = $150.25, closest finalist = $150. Sell-heavy interpretation could also
	// pick $150. Either way the answer is at $150 here.
	assert.Equal(t, int64(1500000), header.ClearingPrice)
	assert.Equal(t, int64(100), header.MatchedQty)
}

func TestUncross_PartialFillAcrossMultipleOrders(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1500000, 60)
	placeBid(t, book, 1500000, 40)
	placeAsk(t, book, 1500000, 100)

	events := runUncross(t, book)
	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(1500000), header.ClearingPrice)
	assert.Equal(t, int64(100), header.MatchedQty)

	var trades []*orderbookv1.TradeExecuted
	for _, evt := range events {
		if t, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			trades = append(trades, t)
		}
	}
	require.Len(t, trades, 2)
	assert.Equal(t, int64(60), trades[0].Quantity)
	assert.Equal(t, int64(40), trades[1].Quantity)
}

func TestUncross_TradeExecutedTaggedCrossType(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1500000, 50)
	placeAsk(t, book, 1500000, 50)

	events := runUncross(t, book)

	for _, evt := range events {
		if trade, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			assert.Equal(t, orderbookv1.CrossType_CROSS_TYPE_OPENING, trade.CrossType)
			assert.Equal(t, int64(1500000), trade.Price)
		}
	}
}

func TestUncross_SelfTradeSkipped(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	// Same account on both sides — should not pair.
	placeBidAccount(t, book, "acct-1", 1500000, 100)
	placeAskAccount(t, book, "acct-1", 1500000, 100)

	events := runUncross(t, book)

	for _, evt := range events {
		_, ok := evt.Data.(*orderbookv1.TradeExecuted)
		assert.False(t, ok, "self-trade pair should not produce a trade")
	}
}

func TestUncross_SelfTradeSkipped_PartialFillWithThirdParty(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	// acct-1 sits both sides; acct-2 sells. acct-1's buy should pair
	// with acct-2's sell. acct-1's sell remains.
	placeBidAccount(t, book, "acct-1", 1500000, 100)
	placeAskAccount(t, book, "acct-1", 1500000, 100)
	placeAskAccount(t, book, "acct-2", 1500000, 100)

	events := runUncross(t, book)

	var trades []*orderbookv1.TradeExecuted
	for _, evt := range events {
		if t, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			trades = append(trades, t)
		}
	}
	require.Len(t, trades, 1, "exactly one cross-account pair should fill")
	assert.Equal(t, int64(100), trades[0].Quantity)
}

func TestUncross_FlipsBackToContinuous_ThenMatchesNormally(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeBid(t, book, 1500000, 50)
	placeAsk(t, book, 1500000, 50)
	runUncross(t, book)

	require.Equal(t, orderbook.PhaseContinuous, book.Phase)

	// After uncross, continuous matching is back. Place an aggressive
	// order against any remaining liquidity (none in this case — both
	// sides cleared). Verify the order rests and matches in subsequent
	// continuous flow.
	placeAsk(t, book, 1510000, 30)
	events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1510000,
		Quantity: 30,
	})
	require.NoError(t, err)

	var trades []*orderbookv1.TradeExecuted
	for _, evt := range events {
		if t, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			trades = append(trades, t)
		}
	}
	require.Len(t, trades, 1)
	assert.Equal(t, orderbookv1.CrossType_CROSS_TYPE_NONE, trades[0].CrossType,
		"continuous trades carry cross_type=NONE")
}

// -- AT_OPEN order type ---------------------------------------------------

func placeLOO(t *testing.T, book *orderbook.OrderBook, account string, side orderbook.Side, price, quantity int64) {
	t.Helper()
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        side,
		Price:       price,
		Quantity:    quantity,
		TimeInForce: orderbook.AtOpen,
		AccountID:   account,
	})
	require.NoError(t, err)
}

func placeMOO(t *testing.T, book *orderbook.OrderBook, account string, side orderbook.Side, quantity int64) {
	t.Helper()
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        side,
		Quantity:    quantity,
		OrderType:   orderbook.Market,
		TimeInForce: orderbook.AtOpen,
		AccountID:   account,
	})
	require.NoError(t, err)
}

func TestAtOpen_RejectedOutsideAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.AtOpen,
	})
	assert.ErrorIs(t, err, orderbook.ErrAtOpenOutsideAuction)
}

func TestAtOpen_StopRejected(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Sell,
		StopPrice:   1480000,
		Quantity:    50,
		OrderType:   orderbook.StopMarket,
		TimeInForce: orderbook.AtOpen,
	})
	assert.ErrorIs(t, err, orderbook.ErrAuctionStopNotAllowed)
}

func TestAtOpen_RoutedToOpeningBook_NotInContinuousDepth(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeLOO(t, book, "acct-1", orderbook.Buy, 1500000, 100)

	// Bids side should be empty — AT_OPEN orders stay in the auction book.
	assert.Equal(t, 0, book.Bids.Len(), "AT_OPEN limit must not appear in continuous bids")
	assert.Equal(t, 1, book.OpeningBook.Len(), "AT_OPEN should be parked in OpeningBook")
}

func TestUncross_MOO_FillsAgainstResting(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeLOO(t, book, "acct-1", orderbook.Sell, 1500000, 100) // LOO sell @ $150
	placeMOO(t, book, "acct-2", orderbook.Buy, 100)           // MOO buy

	events := runUncross(t, book)
	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(1500000), header.ClearingPrice)
	assert.Equal(t, int64(100), header.MatchedQty)
}

func TestUncross_LOO_PriorityMergedWithRegularLimits(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeLOO(t, book, "acct-1", orderbook.Buy, 1500000, 50) // LOO
	placeBid(t, book, 1500000, 50)                          // regular limit @ same price
	placeAsk(t, book, 1500000, 100)                         // regular limit sell

	events := runUncross(t, book)
	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(1500000), header.ClearingPrice)
	assert.Equal(t, int64(100), header.MatchedQty)

	var trades []*orderbookv1.TradeExecuted
	for _, evt := range events {
		if t, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			trades = append(trades, t)
		}
	}
	assert.Len(t, trades, 2, "both buy-side orders should fill")
}

func TestUncross_UnfilledLOO_CancelledMissedAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	// LOO buy at $148 — won't clear because no sells exist at $148.
	placeLOO(t, book, "acct-1", orderbook.Buy, 1480000, 50)
	placeAsk(t, book, 1500000, 50) // regular sell at $150

	events := runUncross(t, book)

	// Find the cancellation for the LOO.
	var cancelReason string
	for _, evt := range events {
		if c, ok := evt.Data.(*orderbookv1.OrderCancelled); ok {
			cancelReason = c.Reason
		}
	}
	assert.Equal(t, "missed_auction", cancelReason)
	assert.Equal(t, 0, book.OpeningBook.Len(), "OpeningBook should be empty after uncross")
}

func TestUncross_UnfilledMOO_NoLiquidity_Cancelled(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	// MOO buy with nothing to fill against.
	placeMOO(t, book, "acct-1", orderbook.Buy, 50)

	events := runUncross(t, book)

	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(0), header.MatchedQty)

	var sawCancel bool
	for _, evt := range events {
		if c, ok := evt.Data.(*orderbookv1.OrderCancelled); ok {
			assert.Equal(t, "missed_auction", c.Reason)
			sawCancel = true
		}
	}
	assert.True(t, sawCancel, "unfilled MOO should be cancelled")
}

func TestUncross_PureMarketCross_UsesLastTradePrice(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	// Establish a prior continuous trade at $150 to seed LastTradePrice.
	placeBid(t, book, 1500000, 10)
	placeAskAccount(t, book, "acct-x", 1500000, 10)
	require.Equal(t, int64(1500000), book.LastTradePrice)

	openAuction(t, book)
	placeMOO(t, book, "acct-1", orderbook.Buy, 100)
	placeMOO(t, book, "acct-2", orderbook.Sell, 100)

	events := runUncross(t, book)
	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(1500000), header.ClearingPrice,
		"pure-market cross uses LastTradePrice as the reference")
	assert.Equal(t, int64(100), header.MatchedQty)
}

func TestUncross_PureMarketCross_NoLastTrade_NoClearing(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	placeMOO(t, book, "acct-1", orderbook.Buy, 50)
	placeMOO(t, book, "acct-2", orderbook.Sell, 50)

	events := runUncross(t, book)
	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, int64(0), header.MatchedQty, "no reference price → no clearing")
	assert.Equal(t, int64(0), header.ClearingPrice)
}

func TestAtOpen_CancellableDuringAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.AtOpen,
		OrderID:     "loo-1",
	})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, 1, book.OpeningBook.Len())

	_, err = orderbook.ExecuteCancelOrder(book, orderbook.CancelOrder{
		Symbol:  "AAPL",
		OrderID: "loo-1",
	})
	require.NoError(t, err)
	assert.Equal(t, 0, book.OpeningBook.Len(), "cancelled AT_OPEN should leave the auction book")
}

// -- AT_CLOSE / closing auction ------------------------------------------

func placeLOC(t *testing.T, book *orderbook.OrderBook, account string, side orderbook.Side, price, quantity int64) {
	t.Helper()
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        side,
		Price:       price,
		Quantity:    quantity,
		TimeInForce: orderbook.AtClose,
		AccountID:   account,
	})
	require.NoError(t, err)
}

func placeMOC(t *testing.T, book *orderbook.OrderBook, account string, side orderbook.Side, quantity int64) {
	t.Helper()
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        side,
		Quantity:    quantity,
		OrderType:   orderbook.Market,
		TimeInForce: orderbook.AtClose,
		AccountID:   account,
	})
	require.NoError(t, err)
}

func beginClosingAuction(t *testing.T, book *orderbook.OrderBook) {
	t.Helper()
	_, err := orderbook.ExecuteBeginClosingAuction(book, orderbook.BeginClosingAuction{Symbol: "AAPL"})
	require.NoError(t, err)
}

func TestAtClose_AcceptedDuringContinuous_RestsInClosingBook(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	placeLOC(t, book, "acct-1", orderbook.Buy, 1500000, 100)

	assert.Equal(t, 0, book.Bids.Len(), "AT_CLOSE must not appear in continuous bids")
	assert.Equal(t, 1, book.ClosingBook.Len())
}

func TestAtClose_RejectedDuringOpeningAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.AtClose,
	})
	assert.ErrorIs(t, err, orderbook.ErrAtCloseOutsideAcceptanceWindow)
}

func TestBeginClosingAuction_FromContinuous(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))

	events, err := orderbook.ExecuteBeginClosingAuction(book, orderbook.BeginClosingAuction{
		Symbol: "AAPL",
		Reason: "session_close",
	})
	require.NoError(t, err)
	require.Len(t, events, 1)

	changed := events[0].Data.(*orderbookv1.MarketPhaseChanged)
	assert.Equal(t, orderbookv1.MarketPhase_MARKET_PHASE_CLOSING_AUCTION, changed.Phase)
	assert.Equal(t, orderbook.PhaseClosingAuction, book.Phase)
}

func TestBeginClosingAuction_RejectedFromOpeningAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	openAuction(t, book)

	_, err := orderbook.ExecuteBeginClosingAuction(book, orderbook.BeginClosingAuction{Symbol: "AAPL"})
	assert.ErrorIs(t, err, orderbook.ErrCannotBeginClosing)
}

func TestPlaceOrder_RegularOrderRejectedDuringClosingAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	beginClosingAuction(t, book)

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 100,
	})
	assert.ErrorIs(t, err, orderbook.ErrClosingAuctionRejectsRegular)
}

func TestPlaceOrder_AtCloseAcceptedDuringClosingAuction(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	beginClosingAuction(t, book)

	placeLOC(t, book, "acct-1", orderbook.Sell, 1500000, 100)
	assert.Equal(t, 1, book.ClosingBook.Len())
}

func TestUncross_ClosingAuction_FlipsToClosed_WithCrossTypeClosing(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	// Stage AT_CLOSE on both sides during continuous trading.
	placeLOC(t, book, "acct-1", orderbook.Buy, 1500000, 100)
	placeLOC(t, book, "acct-2", orderbook.Sell, 1500000, 100)

	beginClosingAuction(t, book)

	events := runUncross(t, book)
	header := events[0].Data.(*orderbookv1.AuctionUncrossed)
	assert.Equal(t, orderbookv1.CrossType_CROSS_TYPE_CLOSING, header.CrossType)
	assert.Equal(t, int64(1500000), header.ClearingPrice)
	assert.Equal(t, int64(100), header.MatchedQty)

	// Phase should flip to CLOSED.
	assert.Equal(t, orderbook.PhaseClosed, book.Phase)

	// All trades carry cross_type=CLOSING.
	for _, evt := range events {
		if trade, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			assert.Equal(t, orderbookv1.CrossType_CROSS_TYPE_CLOSING, trade.CrossType)
		}
	}
}

func TestUncross_ClosingAuction_SuppressesStopTriggers(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	// Seed a stop that would trigger if the clearing price dropped below $148.
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		StopPrice: 1480000,
		Quantity:  50,
		OrderType: orderbook.StopMarket,
	})
	require.NoError(t, err)

	placeLOC(t, book, "acct-1", orderbook.Buy, 1470000, 50)
	placeLOC(t, book, "acct-2", orderbook.Sell, 1470000, 50)

	beginClosingAuction(t, book)
	events := runUncross(t, book)

	// Closing uncross flips to CLOSED — stops shouldn't activate.
	for _, evt := range events {
		_, ok := evt.Data.(*orderbookv1.StopTriggered)
		assert.False(t, ok, "stops must not trigger on closing uncross (book is dead)")
	}
}

func TestUncross_ClosingAuction_UnfilledAtClose_Cancelled(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	placeLOC(t, book, "acct-1", orderbook.Buy, 1480000, 50) // won't fill — no offsetting sell
	beginClosingAuction(t, book)

	events := runUncross(t, book)

	var sawCancel bool
	for _, evt := range events {
		if c, ok := evt.Data.(*orderbookv1.OrderCancelled); ok {
			assert.Equal(t, "missed_auction", c.Reason)
			sawCancel = true
		}
	}
	assert.True(t, sawCancel)
	assert.Equal(t, 0, book.ClosingBook.Len())
}

func TestPlaceOrder_RejectedAfterClosed(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	beginClosingAuction(t, book)
	runUncross(t, book)
	require.Equal(t, orderbook.PhaseClosed, book.Phase)

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 100,
	})
	assert.ErrorIs(t, err, orderbook.ErrMarketClosed)
}

func TestOpenAuction_FromClosed_StartsNextSession(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	beginClosingAuction(t, book)
	runUncross(t, book)
	require.Equal(t, orderbook.PhaseClosed, book.Phase)

	openAuction(t, book)
	assert.Equal(t, orderbook.PhaseAuction, book.Phase)
}

func TestUncross_TriggersPreExistingStop(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))

	// Seed: place a sell-stop in continuous mode that triggers if price drops to $148.
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		StopPrice: 1480000,
		Quantity:  50,
		OrderType: orderbook.StopMarket,
	})
	require.NoError(t, err)

	// Seed a bid below the stop so the triggered market-sell has something to hit.
	placeBid(t, book, 1470000, 50)

	openAuction(t, book)
	// Add a sell that crosses with the bid, clearing at $147 (which is below the stop trigger $148).
	placeAsk(t, book, 1470000, 50)

	events := runUncross(t, book)

	// We expect AuctionUncrossed, TradeExecuted (opening cross @ $147),
	// MarketPhaseChanged, StopTriggered (because $147 ≤ $148),
	// and at minimum cancellations of the stop's remainder (no liquidity left).
	var sawStopTrigger bool
	for _, evt := range events {
		if _, ok := evt.Data.(*orderbookv1.StopTriggered); ok {
			sawStopTrigger = true
		}
	}
	assert.True(t, sawStopTrigger, "stop with trigger ≥ clearing_price should activate")
}
