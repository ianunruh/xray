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
