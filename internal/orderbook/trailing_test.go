package orderbook_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTrailingHandler(t *testing.T) (*es.Handler[*orderbook.OrderBook], context.Context) {
	t.Helper()
	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)
	store := memstore.New()
	h := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	return h, context.Background()
}

// seedSpread places resting bids and asks so that incoming orders have
// price discovery. Each side has many lots so iterations don't deplete it.
func seedSpread(t *testing.T, handler *es.Handler[*orderbook.OrderBook], bid, ask int64, qty int64) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: bid, Quantity: qty, TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: bid, Quantity: qty, TimeInForce: orderbook.GTC,
		})
	}))
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: ask, Quantity: qty, TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: ask, Quantity: qty, TimeInForce: orderbook.GTC,
		})
	}))
}

// Trailing-stop SELL ratchets UP when the mark rises high enough that
// (mark - trail) exceeds the current stop. Initial stop $149, trail $1,
// trade at $152 -> new stop $151.
func TestTrailingStop_SellRatchetsUp(t *testing.T) {
	handler, ctx := newTrailingHandler(t)

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 50,
		OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
		StopPrice: 1490000, TrailAmount: 10000, OrderID: "trail-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 50,
			OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
			StopPrice: 1490000, TrailAmount: 10000, OrderID: "trail-1",
		})
	}))

	// Resting ask at $152, buy hits → trade at $152.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1520000, Quantity: 100,
		TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1520000, Quantity: 100,
			TimeInForce: orderbook.GTC,
		})
	}))
	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1520000, Quantity: 1,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1520000, Quantity: 1,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	var adjusted *orderbookv1.TrailingStopAdjusted
	for _, e := range produced {
		if a, ok := e.Data.(*orderbookv1.TrailingStopAdjusted); ok {
			adjusted = a
		}
	}
	require.NotNil(t, adjusted)
	assert.Equal(t, int64(1490000), adjusted.PreviousStopPrice)
	assert.Equal(t, int64(1510000), adjusted.NewStopPrice, "mark=$152, trail=$1 -> stop=$151")
	assert.Equal(t, int64(1520000), adjusted.MarkPrice)
}

// Trailing-stop SELL must NOT ratchet on an unfavorable move (mark falls).
func TestTrailingStop_SellNoRatchetOnUnfavorableMark(t *testing.T) {
	handler, ctx := newTrailingHandler(t)

	// Establish a first trade so subsequent ones can be unfavorable.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 10,
		TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 10,
			TimeInForce: orderbook.GTC,
		})
	}))
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 1,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 1,
			TimeInForce: orderbook.IOC,
		})
	}))

	// Trailing-stop SELL with initial stop $149, trail $1.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 50,
		OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
		StopPrice: 1490000, TrailAmount: 10000, OrderID: "trail-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 50,
			OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
			StopPrice: 1490000, TrailAmount: 10000, OrderID: "trail-1",
		})
	}))

	// Trade at $148 (mark falls below initial stop trail target $147).
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1480000, Quantity: 5,
		TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1480000, Quantity: 5,
			TimeInForce: orderbook.GTC,
		})
	}))
	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1480000, Quantity: 2,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1480000, Quantity: 2,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	for _, e := range produced {
		_, ok := e.Data.(*orderbookv1.TrailingStopAdjusted)
		assert.False(t, ok, "must not ratchet on unfavorable mark")
	}
}

// Trailing-stop BUY ratchets DOWN as mark falls. Initial stop $152, trail $1,
// trade at $148 -> stop should drop to $149.
func TestTrailingStop_BuyRatchetsDownOnFavorableMark(t *testing.T) {
	handler, ctx := newTrailingHandler(t)

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Quantity: 30,
		OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
		StopPrice: 1520000, TrailAmount: 10000, OrderID: "trail-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Quantity: 30,
			OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
			StopPrice: 1520000, TrailAmount: 10000, OrderID: "trail-1",
		})
	}))

	// Print at $148.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1480000, Quantity: 10,
		TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1480000, Quantity: 10,
			TimeInForce: orderbook.GTC,
		})
	}))
	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1480000, Quantity: 1,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1480000, Quantity: 1,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	var adjusted *orderbookv1.TrailingStopAdjusted
	for _, e := range produced {
		if a, ok := e.Data.(*orderbookv1.TrailingStopAdjusted); ok {
			adjusted = a
		}
	}
	require.NotNil(t, adjusted)
	assert.Equal(t, int64(1490000), adjusted.NewStopPrice, "mark=$148, trail=$1 -> stop=$149 (lower than initial $152)")
}

// After a ratchet, the stop sits at its new value and triggers when a
// trade prints back through it. Initial $149, ratchets to $151, then
// next trade at $151 should fire the stop.
func TestTrailingStop_FiresAfterRatchet(t *testing.T) {
	handler, ctx := newTrailingHandler(t)

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
		OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
		StopPrice: 1490000, TrailAmount: 10000, OrderID: "trail-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
			OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
			StopPrice: 1490000, TrailAmount: 10000, OrderID: "trail-1",
		})
	}))

	// Provide bid-side liquidity for when the trailing stop fires (sell).
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1505000, Quantity: 100,
		TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1505000, Quantity: 100,
			TimeInForce: orderbook.GTC,
		})
	}))

	// First print at $152 -> ratchet stop to $151.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1520000, Quantity: 5,
		TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1520000, Quantity: 5,
			TimeInForce: orderbook.GTC,
		})
	}))
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1520000, Quantity: 1,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1520000, Quantity: 1,
			TimeInForce: orderbook.IOC,
		})
	}))

	// Second print at $151 — below new stop -> trigger.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1510000, Quantity: 5,
		TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1510000, Quantity: 5,
			TimeInForce: orderbook.GTC,
		})
	}))
	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1510000, Quantity: 1,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1510000, Quantity: 1,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	var stopFired bool
	for _, e := range produced {
		if st, ok := e.Data.(*orderbookv1.StopTriggered); ok && st.OrderId == "trail-1" {
			stopFired = true
			assert.Equal(t, orderbookv1.OrderType_ORDER_TYPE_MARKET, st.ActivatedAs)
		}
	}
	assert.True(t, stopFired, "trailing stop should fire after ratchet when mark falls back through new stop")
}

// Bps trail: 100bps = 1% of mark. At mark=$200, trail = $2.
func TestTrailingStop_BpsTrail(t *testing.T) {
	handler, ctx := newTrailingHandler(t)

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
		OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
		StopPrice: 1900000, TrailOffsetBps: 100, OrderID: "trail-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
			OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
			StopPrice: 1900000, TrailOffsetBps: 100, OrderID: "trail-1",
		})
	}))

	// Print at $200.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 2000000, Quantity: 10,
		TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 2000000, Quantity: 10,
			TimeInForce: orderbook.GTC,
		})
	}))
	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 2000000, Quantity: 1,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 2000000, Quantity: 1,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	var adjusted *orderbookv1.TrailingStopAdjusted
	for _, e := range produced {
		if a, ok := e.Data.(*orderbookv1.TrailingStopAdjusted); ok {
			adjusted = a
		}
	}
	require.NotNil(t, adjusted)
	// $200 * 100bps = $2, so stop = $200 - $2 = $198.
	assert.Equal(t, int64(1980000), adjusted.NewStopPrice)
}

func TestTrailingStop_RejectedWithoutTrail(t *testing.T) {
	handler, ctx := newTrailingHandler(t)
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
		OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
		StopPrice: 1500000,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
			OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
			StopPrice: 1500000,
		})
	})
	assert.ErrorIs(t, err, orderbook.ErrTrailingStopRequiresTrail)
}

func TestTrailingStop_RejectedWithBothTrailParams(t *testing.T) {
	handler, ctx := newTrailingHandler(t)
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
		OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
		StopPrice: 1500000, TrailAmount: 10000, TrailOffsetBps: 50,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
			OrderType: orderbook.TrailingStopMarket, TimeInForce: orderbook.GTC,
			StopPrice: 1500000, TrailAmount: 10000, TrailOffsetBps: 50,
		})
	})
	assert.ErrorIs(t, err, orderbook.ErrTrailingStopAmbiguousTrail)
}

func TestTrailingStop_LimitRequiresOffset(t *testing.T) {
	handler, ctx := newTrailingHandler(t)
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
		OrderType: orderbook.TrailingStopLimit, TimeInForce: orderbook.GTC,
		StopPrice: 1500000, TrailAmount: 10000,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 10,
			OrderType: orderbook.TrailingStopLimit, TimeInForce: orderbook.GTC,
			StopPrice: 1500000, TrailAmount: 10000,
		})
	})
	assert.ErrorIs(t, err, orderbook.ErrTrailingStopLimitRequiresOffset)
}

// Snapshot round-trip must preserve trailing state.
func TestTrailingStop_SnapshotRoundTrip(t *testing.T) {
	handler, ctx := newTrailingHandler(t)

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 50,
		OrderType: orderbook.TrailingStopLimit, TimeInForce: orderbook.GTC,
		StopPrice: 1490000, TrailAmount: 10000, LimitOffset: 5000, OrderID: "trail-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 50,
			OrderType: orderbook.TrailingStopLimit, TimeInForce: orderbook.GTC,
			StopPrice: 1490000, TrailAmount: 10000, LimitOffset: 5000, OrderID: "trail-1",
		})
	}))

	original, err := handler.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	snap, err := original.Snapshot()
	require.NoError(t, err)

	restored := orderbook.NewOrderBook("orderbook:AAPL")
	require.NoError(t, restored.RestoreSnapshot(snap))

	o := restored.Orders["trail-1"]
	require.NotNil(t, o)
	assert.Equal(t, orderbook.TrailingStopLimit, o.OrderType)
	assert.Equal(t, int64(1490000), o.StopPrice)
	assert.Equal(t, int64(10000), o.TrailAmount)
	assert.Equal(t, int64(5000), o.LimitOffset)
}
