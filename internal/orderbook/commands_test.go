package orderbook_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTestRegistry() *es.Registry {
	r := es.NewRegistry()
	r.Register("OrderPlaced", func() proto.Message { return new(orderbookv1.OrderPlaced) })
	r.Register("TradeExecuted", func() proto.Message { return new(orderbookv1.TradeExecuted) })
	r.Register("OrderCancelled", func() proto.Message { return new(orderbookv1.OrderCancelled) })
	r.Register("StopTriggered", func() proto.Message { return new(orderbookv1.StopTriggered) })
	r.Register("MarketClosed", func() proto.Message { return new(orderbookv1.MarketClosed) })
	return r
}

func TestPlaceOrder_ThroughHandler(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())

	ctx := context.Background()

	// Place a sell order.
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1500000,
		Quantity: 100,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Sell,
			Price:    1500000,
			Quantity: 100,
		})
	})
	require.NoError(t, err)

	// Place a matching buy order.
	err = handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 60,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Buy,
			Price:    1500000,
			Quantity: 60,
		})
	})
	require.NoError(t, err)

	// Verify event stream.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)

	// Should have: OrderPlaced(sell), OrderPlaced(buy), TradeExecuted
	require.Len(t, raw, 3)

	evt0, err := registry.Deserialize(raw[0])
	require.NoError(t, err)
	assert.Equal(t, "OrderPlaced", evt0.Type)
	placed0 := evt0.Data.(*orderbookv1.OrderPlaced)
	assert.Equal(t, orderbookv1.Side_SIDE_SELL, placed0.Side)
	assert.Equal(t, int64(100), placed0.Quantity)

	evt1, err := registry.Deserialize(raw[1])
	require.NoError(t, err)
	assert.Equal(t, "OrderPlaced", evt1.Type)
	placed1 := evt1.Data.(*orderbookv1.OrderPlaced)
	assert.Equal(t, orderbookv1.Side_SIDE_BUY, placed1.Side)
	assert.Equal(t, int64(60), placed1.Quantity)

	evt2, err := registry.Deserialize(raw[2])
	require.NoError(t, err)
	assert.Equal(t, "TradeExecuted", evt2.Type)
	trade := evt2.Data.(*orderbookv1.TradeExecuted)
	assert.Equal(t, int64(1500000), trade.Price)
	assert.Equal(t, int64(60), trade.Quantity)
}

func TestCancelOrder_ThroughHandler(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())

	ctx := context.Background()

	// Place a sell order first.
	var placedOrderID string
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1500000,
		Quantity: 100,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Sell,
			Price:    1500000,
			Quantity: 100,
		})
		if err != nil {
			return nil, err
		}
		placed := events[0].Data.(*orderbookv1.OrderPlaced)
		placedOrderID = placed.OrderId
		return events, nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, placedOrderID)

	// Cancel it.
	err = handler.Handle(ctx, orderbook.CancelOrder{
		Symbol:  "AAPL",
		OrderID: placedOrderID,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, orderbook.CancelOrder{
			Symbol:  "AAPL",
			OrderID: placedOrderID,
		})
	})
	require.NoError(t, err)

	// Verify event stream has OrderPlaced + OrderCancelled.
	raw, err := store.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	require.Len(t, raw, 2)

	evt1, err := registry.Deserialize(raw[1])
	require.NoError(t, err)
	assert.Equal(t, "OrderCancelled", evt1.Type)
	cancelled := evt1.Data.(*orderbookv1.OrderCancelled)
	assert.Equal(t, placedOrderID, cancelled.OrderId)
}

func TestCancelOrder_NotFound(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecuteCancelOrder(book, orderbook.CancelOrder{
		Symbol:  "AAPL",
		OrderID: "nonexistent",
	})
	assert.Error(t, err)
}

func TestPlaceOrder_InvalidInput(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    0,
		Quantity: 100,
	})
	assert.Error(t, err)

	_, err = orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 0,
	})
	assert.Error(t, err)
}

func TestPlaceOrder_MarketGTC_Rejected(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       0,
		Quantity:    100,
		OrderType:   orderbook.Market,
		TimeInForce: orderbook.GTC,
	})
	assert.ErrorIs(t, err, orderbook.ErrMarketGTC)
}

func TestPlaceOrder_MarketWithPrice_Rejected(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		OrderType:   orderbook.Market,
		TimeInForce: orderbook.IOC,
	})
	assert.ErrorIs(t, err, orderbook.ErrMarketRequiresZeroPrice)
}

func TestPlaceOrder_IOC_PartialFillCancelsRemainder(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())

	ctx := context.Background()

	// Place a sell order for 50 shares.
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1500000,
		Quantity: 50,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Sell,
			Price:    1500000,
			Quantity: 50,
		})
	})
	require.NoError(t, err)

	// Place an IOC buy for 100. Only 50 can fill; remainder should be cancelled.
	var produced []es.Event
	err = handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:      "AAPL",
			Side:        orderbook.Buy,
			Price:       1500000,
			Quantity:    100,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	})
	require.NoError(t, err)

	// Events: OrderPlaced, TradeExecuted, OrderCancelled
	require.Len(t, produced, 3)
	assert.Equal(t, "OrderPlaced", produced[0].Type)
	assert.Equal(t, "TradeExecuted", produced[1].Type)
	assert.Equal(t, "OrderCancelled", produced[2].Type)

	trade := produced[1].Data.(*orderbookv1.TradeExecuted)
	assert.Equal(t, int64(50), trade.Quantity)
}

func TestPlaceOrder_FOK_InsufficientLiquidity(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())

	ctx := context.Background()

	// Place a sell order for 50 shares.
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1500000,
		Quantity: 50,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Sell,
			Price:    1500000,
			Quantity: 50,
		})
	})
	require.NoError(t, err)

	// FOK buy for 100 should be rejected — only 50 available.
	err = handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.FOK,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:      "AAPL",
			Side:        orderbook.Buy,
			Price:       1500000,
			Quantity:    100,
			TimeInForce: orderbook.FOK,
		})
	})
	require.Error(t, err)
}

func TestPlaceOrder_FOK_Success(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())

	ctx := context.Background()

	// Place a sell order for 100.
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1500000,
		Quantity: 100,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:   "AAPL",
			Side:     orderbook.Sell,
			Price:    1500000,
			Quantity: 100,
		})
	})
	require.NoError(t, err)

	// FOK buy for 100 should succeed — exact liquidity available.
	var produced []es.Event
	err = handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.FOK,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:      "AAPL",
			Side:        orderbook.Buy,
			Price:       1500000,
			Quantity:    100,
			TimeInForce: orderbook.FOK,
		})
		produced = events
		return events, err
	})
	require.NoError(t, err)

	// Events: OrderPlaced, TradeExecuted (no cancel since fully filled)
	require.Len(t, produced, 2)
	assert.Equal(t, "OrderPlaced", produced[0].Type)
	assert.Equal(t, "TradeExecuted", produced[1].Type)

	trade := produced[1].Data.(*orderbookv1.TradeExecuted)
	assert.Equal(t, int64(100), trade.Quantity)
}

func TestPlaceOrder_StopMarket_Rests(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		StopPrice: 1450000,
		Quantity:  100,
		OrderType: orderbook.StopMarket,
	})
	require.NoError(t, err)

	require.Len(t, events, 1)
	assert.Equal(t, "OrderPlaced", events[0].Type)
	placed := events[0].Data.(*orderbookv1.OrderPlaced)
	assert.Equal(t, int64(1450000), placed.StopPrice)
	assert.Equal(t, orderbookv1.OrderType_ORDER_TYPE_STOP_MARKET, placed.OrderType)
}

func TestPlaceOrder_StopMarket_RequiresStopPrice(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Quantity:  100,
		OrderType: orderbook.StopMarket,
	})
	assert.ErrorIs(t, err, orderbook.ErrStopRequiresStopPrice)
}

func TestPlaceOrder_StopMarket_RequiresZeroPrice(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1450000,
		StopPrice: 1450000,
		Quantity:  100,
		OrderType: orderbook.StopMarket,
	})
	assert.ErrorIs(t, err, orderbook.ErrStopMarketRequiresZeroPrice)
}

func TestPlaceOrder_StopLimit_Rests(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1440000,
		StopPrice: 1450000,
		Quantity:  100,
		OrderType: orderbook.StopLimit,
	})
	require.NoError(t, err)

	require.Len(t, events, 1)
	assert.Equal(t, "OrderPlaced", events[0].Type)
	placed := events[0].Data.(*orderbookv1.OrderPlaced)
	assert.Equal(t, int64(1450000), placed.StopPrice)
	assert.Equal(t, int64(1440000), placed.Price)
	assert.Equal(t, orderbookv1.OrderType_ORDER_TYPE_STOP_LIMIT, placed.OrderType)
}

func TestPlaceOrder_StopLimit_RequiresStopPrice(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		Price:     1440000,
		Quantity:  100,
		OrderType: orderbook.StopLimit,
	})
	assert.ErrorIs(t, err, orderbook.ErrStopRequiresStopPrice)
}

func TestPlaceOrder_StopLimit_RequiresPrice(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:    "AAPL",
		Side:      orderbook.Sell,
		StopPrice: 1450000,
		Quantity:  100,
		OrderType: orderbook.StopLimit,
	})
	assert.ErrorIs(t, err, orderbook.ErrStopLimitRequiresPrice)
}

func TestStopMarket_SellStop_TriggersOnPriceDrop(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	ctx := context.Background()

	// Resting bid at $140 — liquidity for the triggered stop to fill against.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1400000, Quantity: 100,
	})

	// Sell stop-market at $145.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, StopPrice: 1450000, Quantity: 50,
		OrderType: orderbook.StopMarket,
	})

	// Create a trade at $145: ask at $145 (won't match the $140 bid), then buy at $145.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1450000, Quantity: 10,
	})
	produced := placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1450000, Quantity: 10,
	})

	types := eventTypes(produced)
	assert.Contains(t, types, "StopTriggered")

	for _, evt := range produced {
		if st, ok := evt.Data.(*orderbookv1.StopTriggered); ok {
			assert.Equal(t, int64(1450000), st.StopPrice)
			assert.Equal(t, int64(1450000), st.TriggerPrice)
			assert.Equal(t, orderbookv1.OrderType_ORDER_TYPE_MARKET, st.ActivatedAs)
		}
	}
}

func TestStopMarket_BuyStop_TriggersOnPriceRise(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	ctx := context.Background()

	// Resting ask at $160 — liquidity for the triggered buy stop to fill against.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1600000, Quantity: 100,
	})

	// Buy stop-market at $155.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, StopPrice: 1550000, Quantity: 50,
		OrderType: orderbook.StopMarket,
	})

	// Create a trade at $155: resting bid at $155 (won't match the $160 ask), then sell at $155.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1550000, Quantity: 10,
	})
	produced := placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1550000, Quantity: 10,
	})

	types := eventTypes(produced)
	assert.Contains(t, types, "StopTriggered")
}

func TestStopLimit_TriggersAndRests(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	ctx := context.Background()

	// Sell stop-limit: stop at $145, limit at $144. No bids at $144 exist.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1440000, StopPrice: 1450000, Quantity: 50,
		OrderType: orderbook.StopLimit,
	})

	// Create a trade at $145 to trigger the stop.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1450000, Quantity: 10,
	})
	produced := placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1450000, Quantity: 10,
	})

	types := eventTypes(produced)
	assert.Contains(t, types, "StopTriggered")

	// The stop-limit should now be resting as a limit order.
	// No trade from the stop since no bids at $144.
	// Verify by loading the book and checking asks.
	book, err := handler.Load(ctx, orderbook.AggregateID("AAPL"))
	require.NoError(t, err)

	var foundLimit bool
	for ask := range book.Asks.All() {
		if ask.Price == 1440000 && ask.Quantity == 50 {
			foundLimit = true
		}
	}
	assert.True(t, foundLimit, "stop-limit should rest as a limit order on the ask side")
}

func TestStopLimit_TriggersAndFills(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	ctx := context.Background()

	// Resting bid at $145 — liquidity for the stop-limit to match against.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1450000, Quantity: 50,
	})

	// Sell stop-limit: stop at $146, limit at $145.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1450000, StopPrice: 1460000, Quantity: 30,
		OrderType: orderbook.StopLimit,
	})

	// Trade at $146 triggers the stop, which then matches the resting bid at $145.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1460000, Quantity: 10,
	})
	produced := placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1460000, Quantity: 10,
	})

	types := eventTypes(produced)
	assert.Contains(t, types, "StopTriggered")

	// Count TradeExecuted events — should have the trigger trade + the stop fill.
	var tradeCount int
	for _, evt := range produced {
		if _, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
			tradeCount++
		}
	}
	assert.Equal(t, 2, tradeCount, "should have trigger trade + stop fill trade")
}

func TestStop_NoTrigger(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	ctx := context.Background()

	// Sell stop at $145.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, StopPrice: 1450000, Quantity: 50,
		OrderType: orderbook.StopMarket,
	})

	// Trade at $150 — above the stop price, should NOT trigger.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 10,
	})
	produced := placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 10,
	})

	types := eventTypes(produced)
	assert.NotContains(t, types, "StopTriggered")
}

func TestStop_Cascade(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	ctx := context.Background()

	// Resting bids at different levels for stops to fill against.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1450000, Quantity: 100,
	})
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1400000, Quantity: 100,
	})

	// Sell stop A at $148 — triggers when price drops to $148.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, StopPrice: 1480000, Quantity: 50,
		OrderType: orderbook.StopMarket,
	})

	// Sell stop B at $145 — triggers when price drops to $145.
	// Stop A fills at $145 (the resting bid), which should cascade and trigger stop B.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, StopPrice: 1450000, Quantity: 50,
		OrderType: orderbook.StopMarket,
	})

	// Execute a trade at $148 to trigger stop A.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1480000, Quantity: 10,
	})
	produced := placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1480000, Quantity: 10,
	})

	// Count StopTriggered events — should have 2 (cascade).
	var stopCount int
	for _, evt := range produced {
		if _, ok := evt.Data.(*orderbookv1.StopTriggered); ok {
			stopCount++
		}
	}
	assert.Equal(t, 2, stopCount, "should cascade: stop A triggers, its fill at $145 triggers stop B")
}

func TestCancelOrder_StopOrder(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	ctx := context.Background()

	// Place a sell stop.
	var stopOrderID string
	cmd := orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, StopPrice: 1450000, Quantity: 50,
		OrderType: orderbook.StopMarket,
	}
	err := handler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, cmd)
		if err != nil {
			return nil, err
		}
		stopOrderID = events[0].Data.(*orderbookv1.OrderPlaced).OrderId
		return events, nil
	})
	require.NoError(t, err)

	// Cancel it.
	cancelCmd := orderbook.CancelOrder{Symbol: "AAPL", OrderID: stopOrderID}
	err = handler.Handle(ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	require.NoError(t, err)

	// Verify the order is gone.
	book, err := handler.Load(ctx, orderbook.AggregateID("AAPL"))
	require.NoError(t, err)
	assert.Empty(t, book.Orders)
}

func TestPlaceOrder_MarketDay_Rejected(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       0,
		Quantity:    100,
		OrderType:   orderbook.Market,
		TimeInForce: orderbook.Day,
	})
	assert.ErrorIs(t, err, orderbook.ErrMarketGTC)
}

func TestPlaceOrder_Day_RestsOnBook(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    100,
		TimeInForce: orderbook.Day,
	})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "OrderPlaced", events[0].Type)

	placed := events[0].Data.(*orderbookv1.OrderPlaced)
	assert.Equal(t, orderbookv1.TimeInForce_TIME_IN_FORCE_DAY, placed.TimeInForce)

	assert.Len(t, book.Orders, 1)
}

func TestCloseMarket_CancelsDayOrders(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	ctx := context.Background()

	// Place a Day buy order.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 100,
		TimeInForce: orderbook.Day,
	})

	// Place a Day sell order.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1600000, Quantity: 50,
		TimeInForce: orderbook.Day,
	})

	// Place a GTC buy order — should survive market close.
	placeOrder(t, handler, ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1400000, Quantity: 200,
	})

	// Close the market.
	var produced []es.Event
	closeCmd := orderbook.CloseMarket{Symbol: "AAPL"}
	err := handler.Handle(ctx, closeCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecuteCloseMarket(book, closeCmd)
		produced = events
		return events, err
	})
	require.NoError(t, err)

	// Should have: MarketClosed + 2 OrderCancelled (one for each Day order).
	require.Len(t, produced, 3)
	assert.Equal(t, "MarketClosed", produced[0].Type)
	assert.Equal(t, "OrderCancelled", produced[1].Type)
	assert.Equal(t, "OrderCancelled", produced[2].Type)

	// Verify the GTC order is still on the book.
	book, err := handler.Load(ctx, orderbook.AggregateID("AAPL"))
	require.NoError(t, err)
	assert.Len(t, book.Orders, 1)

	for _, order := range book.Orders {
		assert.Equal(t, orderbook.GTC, order.TimeInForce)
	}
}

func TestCloseMarket_NoDayOrders(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	// Place a GTC order.
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 100,
	})
	require.NoError(t, err)

	// Close the market — should only emit MarketClosed, no cancellations.
	events, err := orderbook.ExecuteCloseMarket(book, orderbook.CloseMarket{Symbol: "AAPL"})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "MarketClosed", events[0].Type)

	assert.Len(t, book.Orders, 1)
}

func TestCloseMarket_DayStopOrders(t *testing.T) {
	book := orderbook.NewOrderBook("orderbook:AAPL")
	book.Symbol = "AAPL"

	// Place a Day stop-limit order.
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Sell,
		Price:       1440000,
		StopPrice:   1450000,
		Quantity:    50,
		OrderType:   orderbook.StopLimit,
		TimeInForce: orderbook.Day,
	})
	require.NoError(t, err)

	// Close the market — should cancel the Day stop order.
	events, err := orderbook.ExecuteCloseMarket(book, orderbook.CloseMarket{Symbol: "AAPL"})
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, "MarketClosed", events[0].Type)
	assert.Equal(t, "OrderCancelled", events[1].Type)

	assert.Empty(t, book.Orders)
}

func placeOrder(t *testing.T, handler *es.Handler[*orderbook.OrderBook], ctx context.Context, cmd orderbook.PlaceOrder) []es.Event {
	t.Helper()
	var produced []es.Event
	err := handler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, cmd)
		produced = events
		return events, err
	})
	require.NoError(t, err)
	return produced
}

func eventTypes(events []es.Event) []string {
	types := make([]string, len(events))
	for i, evt := range events {
		types[i] = evt.Type
	}
	return types
}
