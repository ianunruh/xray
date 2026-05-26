package orderbook_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

// setBands applies an LULDBandsSet with ±5% bands around `ref` for tests.
func setBands(t *testing.T, book *orderbook.OrderBook, ref int64) {
	t.Helper()
	upper := ref + ref*5/100
	lower := ref - ref*5/100
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventLULDBandsSet,
		Data: &orderbookv1.LULDBandsSet{
			Symbol:         "AAPL",
			ReferencePrice: ref,
			UpperBand:      upper,
			LowerBand:      lower,
			BandBps:        500,
		},
	}))
}

// TestMatch_TripUpperBand: a buy that would print above the upper band
// is cancelled, no TradeExecuted is emitted, and the symbol enters
// PhaseLimitState with band_side=Buy.
func TestMatch_TripUpperBand(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	setBands(t, book, 1500000) // bands: [1425000, 1575000]

	// Rest an ask above the upper band.
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1580000,
		Quantity:  100,
		AccountID: "mm",
	})
	require.NoError(t, err)

	// Aggressive buy that would trade through the band.
	evts, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Buy,
		Price:     1580000,
		Quantity:  100,
		AccountID: "taker",
	})
	require.NoError(t, err)

	// We expect: OrderPlaced (aggressor) + LULDLimitStateEntered + OrderCancelled.
	// No TradeExecuted.
	require.Len(t, evts, 3)
	assert.Equal(t, orderbook.EventOrderPlaced, evts[0].Type)
	assert.Equal(t, orderbook.EventLULDLimitStateEntered, evts[1].Type)
	assert.Equal(t, orderbook.EventOrderCancelled, evts[2].Type)

	for _, evt := range evts {
		if _, isTrade := evt.Data.(*orderbookv1.TradeExecuted); isTrade {
			t.Fatalf("expected no TradeExecuted, got one at %s", evt.Type)
		}
	}

	entered := evts[1].Data.(*orderbookv1.LULDLimitStateEntered)
	assert.Equal(t, orderbookv1.Side_SIDE_BUY, entered.BandSide)
	assert.Equal(t, int64(1575000), entered.BandPrice)

	cancelled := evts[2].Data.(*orderbookv1.OrderCancelled)
	assert.Equal(t, "luld_band", cancelled.Reason)

	assert.Equal(t, orderbook.PhaseLimitState, book.Phase)
}

// TestMatch_TripLowerBand: a sell at-or-below the lower band trips
// with band_side=Sell.
func TestMatch_TripLowerBand(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	setBands(t, book, 1500000) // bands: [1425000, 1575000]

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Buy,
		Price:     1420000,
		Quantity:  100,
		AccountID: "mm",
	})
	require.NoError(t, err)

	evts, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1420000,
		Quantity:  100,
		AccountID: "taker",
	})
	require.NoError(t, err)

	require.Len(t, evts, 3)
	entered := evts[1].Data.(*orderbookv1.LULDLimitStateEntered)
	assert.Equal(t, orderbookv1.Side_SIDE_SELL, entered.BandSide)
	assert.Equal(t, int64(1425000), entered.BandPrice)
	assert.Equal(t, orderbook.PhaseLimitState, book.Phase)
}

// TestMatch_InBandTradeStillExecutes: when a trade prints inside the
// bands it goes through normally and the symbol stays continuous.
func TestMatch_InBandTradeStillExecutes(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	setBands(t, book, 1500000)

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1510000,
		Quantity:  100,
		AccountID: "mm",
	})
	require.NoError(t, err)

	evts, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Buy,
		Price:     1510000,
		Quantity:  100,
		AccountID: "taker",
	})
	require.NoError(t, err)

	var trades int
	for _, evt := range evts {
		if _, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			trades++
		}
		if evt.Type == orderbook.EventLULDLimitStateEntered {
			t.Fatalf("unexpected LULDLimitStateEntered for in-band trade")
		}
	}
	require.Equal(t, 1, trades)
	assert.Equal(t, orderbook.PhaseContinuous, book.Phase)
}

// TestMatch_NoBandsNoTrip: when bands are unset (LULDUpperBand == 0),
// even an extreme print does not trip.
func TestMatch_NoBandsNoTrip(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	// No setBands call.

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     99999999,
		Quantity:  100,
		AccountID: "mm",
	})
	require.NoError(t, err)

	evts, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Buy,
		Price:     99999999,
		Quantity:  100,
		AccountID: "taker",
	})
	require.NoError(t, err)

	for _, evt := range evts {
		if evt.Type == orderbook.EventLULDLimitStateEntered {
			t.Fatalf("did not expect LULDLimitStateEntered with no bands set")
		}
	}
	assert.Equal(t, orderbook.PhaseContinuous, book.Phase)
}

// TestMatch_PartialFillThenTrip: a multi-level aggressor consumes
// in-band liquidity, then trips on the next level that's through the
// band. Earlier in-band trades stand.
func TestMatch_PartialFillThenTrip(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	setBands(t, book, 1500000)

	// In-band ask first.
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1510000,
		Quantity:  40,
		AccountID: "mm",
	})
	require.NoError(t, err)
	// Through-band ask next.
	_, err = orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1580000,
		Quantity:  60,
		AccountID: "mm",
	})
	require.NoError(t, err)

	evts, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Buy,
		Price:     1580000,
		Quantity:  100,
		AccountID: "taker",
	})
	require.NoError(t, err)

	// Expect: OrderPlaced + 1x TradeExecuted (40 @ 1510000) +
	// LULDLimitStateEntered + OrderCancelled.
	require.Len(t, evts, 4)
	assert.Equal(t, orderbook.EventOrderPlaced, evts[0].Type)
	assert.Equal(t, orderbook.EventTradeExecuted, evts[1].Type)
	trade := evts[1].Data.(*orderbookv1.TradeExecuted)
	assert.Equal(t, int64(1510000), trade.Price)
	assert.Equal(t, int64(40), trade.Quantity)
	assert.Equal(t, orderbook.EventLULDLimitStateEntered, evts[2].Type)
	assert.Equal(t, orderbook.EventOrderCancelled, evts[3].Type)
	assert.Equal(t, orderbook.PhaseLimitState, book.Phase)
}

// TestMatch_PostReopenRearmSuppressesTrip: during the LULDRearmAt
// window, the matcher does not trip even on an out-of-band print.
func TestMatch_PostReopenRearmSuppressesTrip(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	setBands(t, book, 1500000)

	// Simulate a recent halt-reopen by stamping LULDRearmAt in the future.
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventTradingResumed,
		Data: &orderbookv1.TradingResumed{
			Symbol:    "AAPL",
			At:        timestamppb.New(time.Now()),
			CrossType: orderbookv1.CrossType_CROSS_TYPE_HALT_REOPEN,
		},
	}))
	// MarketPhaseChanged → CONTINUOUS would have already preceded; for
	// the test, set Phase explicitly via another MarketPhaseChanged.
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventMarketPhaseChanged,
		Data: &orderbookv1.MarketPhaseChanged{
			Symbol: "AAPL",
			Phase:  orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS,
		},
	}))

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1580000,
		Quantity:  100,
		AccountID: "mm",
	})
	require.NoError(t, err)

	evts, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Buy,
		Price:     1580000,
		Quantity:  100,
		AccountID: "taker",
	})
	require.NoError(t, err)

	// Trade should execute (in-band suppression by rearm window).
	var trades int
	for _, evt := range evts {
		if evt.Type == orderbook.EventLULDLimitStateEntered {
			t.Fatalf("rearm window should suppress trip")
		}
		if _, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			trades++
		}
	}
	require.Equal(t, 1, trades)
	assert.Equal(t, orderbook.PhaseContinuous, book.Phase)
}
