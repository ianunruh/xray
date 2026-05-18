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

func newIcebergHandler(t *testing.T) (*es.Handler[*orderbook.OrderBook], context.Context) {
	t.Helper()
	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)
	store := memstore.New()
	h := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default())
	return h, context.Background()
}

// Rest an iceberg ask: only display_quantity shows on depth; the
// hidden reserve isn't matchable until the displayed slice fills.
func TestIceberg_PartialFillBelowDisplayedDoesNotReplenish(t *testing.T) {
	handler, ctx := newIcebergHandler(t)

	// Iceberg sell: 1000 total, 100 displayed @ $150.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Sell,
		Price:       1500000,
		Quantity:    1000,
		DisplayQty:  100,
		TimeInForce: orderbook.GTC,
		OrderID:     "iceberg-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:      "AAPL",
			Side:        orderbook.Sell,
			Price:       1500000,
			Quantity:    1000,
			DisplayQty:  100,
			TimeInForce: orderbook.GTC,
			OrderID:     "iceberg-1",
		})
	}))

	// Buy 60 — fills 60 of the displayed slice; no replenish yet.
	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Price:       1500000,
		Quantity:    60,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol:      "AAPL",
			Side:        orderbook.Buy,
			Price:       1500000,
			Quantity:    60,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	for _, e := range produced {
		_, ok := e.Data.(*orderbookv1.IcebergSliceReplenished)
		assert.False(t, ok, "should not replenish when slice not exhausted")
	}
}

// Hitting an iceberg's displayed slice exactly produces a replenish
// event with hidden_remaining decremented by display_quantity, and
// the next slice becomes visible at the back of the price queue.
func TestIceberg_ExhaustingSliceReplenishesAndReseats(t *testing.T) {
	handler, ctx := newIcebergHandler(t)

	// Iceberg ask: 250 total, 100 displayed @ $150.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 250,
		DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 250,
			DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
		})
	}))

	// Buy exactly 100 (one full slice).
	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 100,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 100,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	// Expect: OrderPlaced (incoming) → TradeExecuted (100) → IcebergSliceReplenished.
	require.Len(t, produced, 3)
	assert.Equal(t, "OrderPlaced", produced[0].Type)
	assert.Equal(t, "TradeExecuted", produced[1].Type)
	assert.Equal(t, "IcebergSliceReplenished", produced[2].Type)

	rep := produced[2].Data.(*orderbookv1.IcebergSliceReplenished)
	assert.Equal(t, "iceberg-1", rep.OrderId)
	assert.Equal(t, int64(100), rep.NewDisplayedQty, "next slice should be full display")
	assert.Equal(t, int64(50), rep.HiddenRemaining, "250-100-100 = 50 left in reserve")
	assert.Equal(t, orderbookv1.Side_SIDE_SELL, rep.Side)
	assert.Equal(t, int64(1500000), rep.Price)
}

// When the buy quantity exceeds one slice, the engine should replenish
// mid-pass and keep consuming the same iceberg (assuming no other
// resters at that price). Total filled = buy qty.
func TestIceberg_LargeBuyConsumesMultipleSlices(t *testing.T) {
	handler, ctx := newIcebergHandler(t)

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 500,
		DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 500,
			DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
		})
	}))

	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 350,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 350,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	var totalFilled int64
	var replenishes int
	for _, e := range produced {
		switch d := e.Data.(type) {
		case *orderbookv1.TradeExecuted:
			totalFilled += d.Quantity
		case *orderbookv1.IcebergSliceReplenished:
			replenishes++
		}
	}
	assert.Equal(t, int64(350), totalFilled, "buyer should be fully filled across slices")
	assert.Equal(t, 3, replenishes, "100/100/100 displayed slices = 3 replenishments, then 50 fills the new slice but does not exhaust it")
}

// Replenish reseats at the back of the queue: a regular order placed
// at the same price after the iceberg jumps ahead once the iceberg's
// current slice is exhausted.
func TestIceberg_LosesPriorityOnReplenish(t *testing.T) {
	handler, ctx := newIcebergHandler(t)

	// First: iceberg ask (gets time priority).
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 300,
		DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 300,
			DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
		})
	}))

	// Then: regular ask at the same price (behind in queue).
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 50,
		TimeInForce: orderbook.GTC, OrderID: "regular-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 50,
			TimeInForce: orderbook.GTC, OrderID: "regular-1",
		})
	}))

	// Buy 160: first 100 from iceberg slice → replenish → iceberg reseated
	// behind regular-1 → 50 from regular → final 10 from new iceberg slice.
	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 160,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 160,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	var trades []*orderbookv1.TradeExecuted
	for _, e := range produced {
		if t, ok := e.Data.(*orderbookv1.TradeExecuted); ok {
			trades = append(trades, t)
		}
	}
	require.Len(t, trades, 3, "expected three fills: iceberg slice, regular, iceberg new slice")
	assert.Equal(t, "iceberg-1", trades[0].SellOrderId)
	assert.Equal(t, int64(100), trades[0].Quantity)
	assert.Equal(t, "regular-1", trades[1].SellOrderId, "after replenish, regular-1 has priority")
	assert.Equal(t, int64(50), trades[1].Quantity)
	assert.Equal(t, "iceberg-1", trades[2].SellOrderId, "remainder consumes new iceberg slice")
	assert.Equal(t, int64(10), trades[2].Quantity)
}

// Final slice when reserve is smaller than display_qty: slice size
// shrinks to whatever's left.
func TestIceberg_TailSliceSmallerThanDisplay(t *testing.T) {
	handler, ctx := newIcebergHandler(t)

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 130,
		DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 130,
			DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
		})
	}))

	var produced []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 100,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 100,
			TimeInForce: orderbook.IOC,
		})
		produced = events
		return events, err
	}))

	var rep *orderbookv1.IcebergSliceReplenished
	for _, e := range produced {
		if r, ok := e.Data.(*orderbookv1.IcebergSliceReplenished); ok {
			rep = r
		}
	}
	require.NotNil(t, rep)
	assert.Equal(t, int64(30), rep.NewDisplayedQty, "only 30 left after slice 1 (130-100)")
	assert.Equal(t, int64(0), rep.HiddenRemaining)
}

func TestIceberg_RejectedWithMarket(t *testing.T) {
	handler, ctx := newIcebergHandler(t)
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Quantity: 100, DisplayQty: 10,
		OrderType: orderbook.Market, TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Quantity: 100, DisplayQty: 10,
			OrderType: orderbook.Market, TimeInForce: orderbook.IOC,
		})
	})
	assert.ErrorIs(t, err, orderbook.ErrIcebergRequiresLimit)
}

func TestIceberg_RejectedWithIOC(t *testing.T) {
	handler, ctx := newIcebergHandler(t)
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 100,
		DisplayQty: 10, TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 100,
			DisplayQty: 10, TimeInForce: orderbook.IOC,
		})
	})
	assert.ErrorIs(t, err, orderbook.ErrIcebergRequiresRestingTIF)
}

func TestIceberg_RejectedWhenDisplayExceedsQuantity(t *testing.T) {
	handler, ctx := newIcebergHandler(t)
	err := handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 100,
		DisplayQty: 150, TimeInForce: orderbook.GTC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 100,
			DisplayQty: 150, TimeInForce: orderbook.GTC,
		})
	})
	assert.ErrorIs(t, err, orderbook.ErrIcebergDisplayExceedsQuantity)
}

// Depth projection must only expose the displayed slice — hidden
// reserve must not leak into GetMarketDepth.
func TestIceberg_DepthOnlyShowsDisplayedQty(t *testing.T) {
	handler, ctx := newIcebergHandler(t)
	depth := orderbook.NewDepthProjection()

	var captured []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 1000,
		DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 1000,
			DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
		})
		captured = events
		return events, err
	}))
	require.NoError(t, depth.HandleEvents(ctx, captured))

	_, asks := depth.GetDepth("AAPL", 0)
	require.Len(t, asks, 1)
	assert.Equal(t, int64(100), asks[0].Quantity, "depth must hide reserve, only show displayed slice")
}

// After replenish, the depth projection must reflect the new slice (not
// continue showing 0 from the just-filled prior slice).
func TestIceberg_DepthRefreshesOnReplenish(t *testing.T) {
	handler, ctx := newIcebergHandler(t)
	depth := orderbook.NewDepthProjection()

	var captured []es.Event
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 300,
		DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 300,
			DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
		})
		captured = append(captured, events...)
		return events, err
	}))

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 100,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 100,
			TimeInForce: orderbook.IOC,
		})
		captured = append(captured, events...)
		return events, err
	}))
	require.NoError(t, depth.HandleEvents(ctx, captured))

	_, asks := depth.GetDepth("AAPL", 0)
	require.Len(t, asks, 1, "iceberg should still be on the book with a fresh slice")
	assert.Equal(t, int64(100), asks[0].Quantity, "fresh slice of 100 visible after replenish")
}

// Snapshot round-trip must preserve iceberg state (display_qty,
// displayed_remaining) so restored books pick up matching where they
// left off.
func TestIceberg_SnapshotRoundTrip(t *testing.T) {
	handler, ctx := newIcebergHandler(t)

	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 500,
		DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Sell, Price: 1500000, Quantity: 500,
			DisplayQty: 100, TimeInForce: orderbook.GTC, OrderID: "iceberg-1",
		})
	}))
	// Partially consume the first slice.
	require.NoError(t, handler.Handle(ctx, orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 30,
		TimeInForce: orderbook.IOC,
	}, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
			Symbol: "AAPL", Side: orderbook.Buy, Price: 1500000, Quantity: 30,
			TimeInForce: orderbook.IOC,
		})
	}))

	original, err := handler.Load(ctx, "orderbook:AAPL")
	require.NoError(t, err)
	snap, err := original.Snapshot()
	require.NoError(t, err)

	restored := orderbook.NewOrderBook("orderbook:AAPL")
	require.NoError(t, restored.RestoreSnapshot(snap))

	o := restored.Orders["iceberg-1"]
	require.NotNil(t, o)
	assert.Equal(t, int64(500), o.Quantity)
	assert.Equal(t, int64(470), o.RemainingQty, "30 of 500 consumed")
	assert.Equal(t, int64(100), o.DisplayQty)
	assert.Equal(t, int64(70), o.Displayed, "100 displayed - 30 traded")
}
