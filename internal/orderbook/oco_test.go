package orderbook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
)

// TestOCO_LimitTradeCancelsStopSibling exercises the bracket exit
// scenario directly at the orderbook layer: a TP limit and an SL stop
// share an OCO group; when the TP fills, the SL must be cancelled
// before any later price move can trigger it.
func TestOCO_LimitTradeCancelsStopSibling(t *testing.T) {
	book := newTestBook()

	// TP: sell limit at 1550000.
	_, err := ExecutePlaceOrder(book, PlaceOrder{
		Symbol:     "AAPL",
		Side:       Sell,
		Price:      1500000,
		Quantity:   100,
		OrderType:  Limit,
		OrderID:    "tp-1",
		OCOGroupID: "oco-1",
	})
	require.NoError(t, err)

	// SL: stop-market sell triggered if price drops below 1450000.
	_, err = ExecutePlaceOrder(book, PlaceOrder{
		Symbol:     "AAPL",
		Side:       Sell,
		StopPrice:  1450000,
		Quantity:   100,
		OrderType:  StopMarket,
		OrderID:    "sl-1",
		OCOGroupID: "oco-1",
	})
	require.NoError(t, err)

	require.Equal(t, 1, book.Asks.Len(), "TP rests in asks")
	require.Equal(t, 1, book.SellStops.Len(), "SL rests in stops")
	require.Len(t, book.OCOGroups["oco-1"], 2, "both legs in OCO group")

	// Aggressor buy hits TP.
	events, err := ExecutePlaceOrder(book, PlaceOrder{
		Symbol:    "AAPL",
		Side:      Buy,
		Price:     1500000,
		Quantity:  100,
		OrderType: Limit,
	})
	require.NoError(t, err)

	// Events: OrderPlaced(buy) + TradeExecuted(tp) + OrderCancelled(sl).
	var sawTrade, sawOcoCancel bool
	for _, e := range events {
		switch d := e.Data.(type) {
		case *orderbookv1.TradeExecuted:
			assert.Equal(t, "tp-1", d.SellOrderId)
			sawTrade = true
		case *orderbookv1.OrderCancelled:
			if d.OrderId == "sl-1" {
				assert.Equal(t, "oco_triggered", d.Reason)
				sawOcoCancel = true
			}
		}
	}
	assert.True(t, sawTrade, "TP traded")
	assert.True(t, sawOcoCancel, "SL cancelled by OCO")

	// SL must be gone from stops and from the group.
	assert.Equal(t, 0, book.SellStops.Len(), "SL removed from stops")
	assert.NotContains(t, book.Orders, "sl-1")
	assert.Empty(t, book.OCOGroups["oco-1"], "OCO group cleared")
}

// TestOCO_StopWouldHaveFiredButWasCancelledFirst verifies the actual
// race: if price moves into the SL trigger zone immediately AFTER the
// TP fills (in the same incoming aggressor), the SL must NOT trigger
// because it was already cancelled.
func TestOCO_StopWouldHaveFiredButWasCancelledFirst(t *testing.T) {
	book := newTestBook()

	// Resting TP sell at 1500000.
	_, err := ExecutePlaceOrder(book, PlaceOrder{
		Symbol: "AAPL", Side: Sell, Price: 1500000, Quantity: 100,
		OrderType: Limit, OrderID: "tp-1", OCOGroupID: "oco-1",
	})
	require.NoError(t, err)

	// Resting SL sell stop-market with stop at 1490000. Any trade at or
	// below 1490000 would trigger it.
	_, err = ExecutePlaceOrder(book, PlaceOrder{
		Symbol: "AAPL", Side: Sell, StopPrice: 1490000, Quantity: 100,
		OrderType: StopMarket, OrderID: "sl-1", OCOGroupID: "oco-1",
	})
	require.NoError(t, err)

	// A counter-bid at 1490000 sits below TP — so no trade with TP.
	// We use this to seed a "low" last-trade price that, without OCO,
	// would let an aggressive sell ladder trigger SL right after TP.
	_, err = ExecutePlaceOrder(book, PlaceOrder{
		Symbol: "AAPL", Side: Buy, Price: 1490000, Quantity: 200,
		OrderType: Limit, OrderID: "bid-1",
	})
	require.NoError(t, err)

	// Now an aggressor buy at TP's price. Trades 100 against TP at
	// 1500000. Without OCO, no trigger fires (1500000 > 1490000 stop).
	// But if anything down-ticks the price afterwards (e.g., another
	// sale at 1490000), the still-resting SL would trigger.
	events, err := ExecutePlaceOrder(book, PlaceOrder{
		Symbol: "AAPL", Side: Buy, Price: 1500000, Quantity: 100,
		OrderType: Limit,
	})
	require.NoError(t, err)

	// SL is cancelled by OCO as part of this same command.
	var slCancelled bool
	for _, e := range events {
		if c, ok := e.Data.(*orderbookv1.OrderCancelled); ok && c.OrderId == "sl-1" {
			slCancelled = true
		}
	}
	require.True(t, slCancelled, "SL must be cancelled in same batch as TP trade")
	require.Equal(t, 0, book.SellStops.Len())

	// Now a sell at 1490000 would have triggered the SL. Issue it.
	events, err = ExecutePlaceOrder(book, PlaceOrder{
		Symbol: "AAPL", Side: Sell, Price: 1490000, Quantity: 100,
		OrderType: Limit,
	})
	require.NoError(t, err)
	// One trade against bid-1; NO stop-trigger event for sl-1.
	for _, e := range events {
		if trig, ok := e.Data.(*orderbookv1.StopTriggered); ok {
			assert.NotEqual(t, "sl-1", trig.OrderId, "SL must not trigger after OCO cancel")
		}
	}
}
