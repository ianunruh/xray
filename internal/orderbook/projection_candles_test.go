package orderbook_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

func TestCandleProjection_SingleTrade(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	ts := time.Date(2024, 1, 15, 10, 3, 24, 0, time.UTC)

	err := proj.HandleEvents(ctx, []es.Event{
		tradeEvent("t1", "AAPL", 1505000, 100, ts),
	})
	require.NoError(t, err)

	from := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 15, 10, 5, 0, 0, time.UTC)

	candles := proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to)
	require.Len(t, candles, 1)

	c := candles[0]
	assert.Equal(t, "AAPL", c.Symbol)
	assert.Equal(t, orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, c.Interval)
	assert.Equal(t, time.Date(2024, 1, 15, 10, 3, 0, 0, time.UTC), c.OpenTime.AsTime())
	assert.Equal(t, int64(1505000), c.Open)
	assert.Equal(t, int64(1505000), c.High)
	assert.Equal(t, int64(1505000), c.Low)
	assert.Equal(t, int64(1505000), c.Close)
	assert.Equal(t, int64(100), c.Volume)
}

func TestCandleProjection_MultipleTradesInSameBucket(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	base := time.Date(2024, 1, 15, 10, 0, 30, 0, time.UTC)

	err := proj.HandleEvents(ctx, []es.Event{
		tradeEvent("t1", "AAPL", 1500000, 10, base),
		tradeEvent("t2", "AAPL", 1520000, 20, base.Add(10*time.Second)),
		tradeEvent("t3", "AAPL", 1490000, 15, base.Add(20*time.Second)),
	})
	require.NoError(t, err)

	from := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 15, 10, 1, 0, 0, time.UTC)
	candles := proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to)
	require.Len(t, candles, 1)

	c := candles[0]
	assert.Equal(t, int64(1500000), c.Open)
	assert.Equal(t, int64(1520000), c.High)
	assert.Equal(t, int64(1490000), c.Low)
	assert.Equal(t, int64(1490000), c.Close)
	assert.Equal(t, int64(45), c.Volume)
}

func TestCandleProjection_TradesCrossIntervalBoundary(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	t1 := time.Date(2024, 1, 15, 10, 0, 30, 0, time.UTC)
	t2 := time.Date(2024, 1, 15, 10, 1, 15, 0, time.UTC)

	err := proj.HandleEvents(ctx, []es.Event{
		tradeEvent("t1", "AAPL", 1500000, 10, t1),
		tradeEvent("t2", "AAPL", 1510000, 20, t2),
	})
	require.NoError(t, err)

	from := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 15, 10, 5, 0, 0, time.UTC)

	// 1m interval: two separate candles
	candles1m := proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to)
	require.Len(t, candles1m, 2)
	assert.Equal(t, int64(1500000), candles1m[0].Close)
	assert.Equal(t, int64(1510000), candles1m[1].Close)

	// 5m interval: same candle
	candles5m := proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_5M, from, to)
	require.Len(t, candles5m, 1)
	assert.Equal(t, int64(1500000), candles5m[0].Open)
	assert.Equal(t, int64(1510000), candles5m[0].High)
	assert.Equal(t, int64(1510000), candles5m[0].Close)
	assert.Equal(t, int64(30), candles5m[0].Volume)
}

func TestCandleProjection_OutOfOrderTrades(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	earlier := time.Date(2024, 1, 15, 10, 0, 10, 0, time.UTC)
	later := time.Date(2024, 1, 15, 10, 0, 30, 0, time.UTC)

	// Process the later trade first, then the earlier one.
	err := proj.HandleEvents(ctx, []es.Event{
		tradeEvent("t1", "AAPL", 1510000, 10, later),
		tradeEvent("t2", "AAPL", 1500000, 20, earlier),
	})
	require.NoError(t, err)

	from := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 15, 10, 1, 0, 0, time.UTC)

	candles := proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to)
	require.Len(t, candles, 1)

	// Close should reflect the chronologically latest trade, not the last-processed one.
	assert.Equal(t, int64(1510000), candles[0].Close)
	assert.Equal(t, int64(1510000), candles[0].Open)
	assert.Equal(t, int64(30), candles[0].Volume)
}

func TestCandleProjection_SymbolFiltering(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	err := proj.HandleEvents(ctx, []es.Event{
		tradeEvent("t1", "AAPL", 1500000, 10, ts),
		tradeEvent("t2", "GOOG", 2800000, 5, ts),
	})
	require.NoError(t, err)

	from := ts
	to := ts.Add(time.Minute)

	aapl := proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to)
	assert.Len(t, aapl, 1)
	assert.Equal(t, int64(1500000), aapl[0].Open)

	goog := proj.GetCandles("GOOG", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to)
	assert.Len(t, goog, 1)
	assert.Equal(t, int64(2800000), goog[0].Open)

	msft := proj.GetCandles("MSFT", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to)
	assert.Empty(t, msft)
}

func TestCandleProjection_IgnoresNonTradeEvents(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	err := proj.HandleEvents(ctx, []es.Event{
		{
			Type: "OrderPlaced",
			Data: &orderbookv1.OrderPlaced{
				OrderId:  "order-1",
				Symbol:   "AAPL",
				PlacedAt: timestamppb.Now(),
			},
		},
		{
			Type: "OrderCancelled",
			Data: &orderbookv1.OrderCancelled{
				OrderId: "order-1",
				Symbol:  "AAPL",
			},
		},
	})
	require.NoError(t, err)

	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Empty(t, proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to))
}

func TestCandleProjection_GetCandlesTimeRange(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	t1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 15, 10, 1, 0, 0, time.UTC)
	t3 := time.Date(2024, 1, 15, 10, 2, 0, 0, time.UTC)

	err := proj.HandleEvents(ctx, []es.Event{
		tradeEvent("t1", "AAPL", 1500000, 10, t1),
		tradeEvent("t2", "AAPL", 1510000, 20, t2),
		tradeEvent("t3", "AAPL", 1520000, 30, t3),
	})
	require.NoError(t, err)

	// Query only the middle candle.
	candles := proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, t2, t2)
	require.Len(t, candles, 1)
	assert.Equal(t, int64(1510000), candles[0].Open)
}

func TestCandleProjection_GetLatestCandle(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	t1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 15, 10, 1, 0, 0, time.UTC)

	err := proj.HandleEvents(ctx, []es.Event{
		tradeEvent("t1", "AAPL", 1500000, 10, t1),
		tradeEvent("t2", "AAPL", 1510000, 20, t2),
	})
	require.NoError(t, err)

	latest := proj.GetLatestCandle("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M)
	require.NotNil(t, latest)
	assert.Equal(t, time.Date(2024, 1, 15, 10, 1, 0, 0, time.UTC), latest.OpenTime.AsTime())
	assert.Equal(t, int64(1510000), latest.Open)

	// No candles for unknown symbol.
	assert.Nil(t, proj.GetLatestCandle("MSFT", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M))
}

func TestCandleProjection_AllIntervals(t *testing.T) {
	proj := orderbook.NewCandleProjection()
	ctx := context.Background()

	ts := time.Date(2024, 1, 15, 10, 3, 24, 0, time.UTC)

	err := proj.HandleEvents(ctx, []es.Event{
		tradeEvent("t1", "AAPL", 1500000, 10, ts),
	})
	require.NoError(t, err)

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

	intervals := []orderbookv1.CandleInterval{
		orderbookv1.CandleInterval_CANDLE_INTERVAL_1M,
		orderbookv1.CandleInterval_CANDLE_INTERVAL_5M,
		orderbookv1.CandleInterval_CANDLE_INTERVAL_15M,
		orderbookv1.CandleInterval_CANDLE_INTERVAL_1H,
		orderbookv1.CandleInterval_CANDLE_INTERVAL_1D,
	}

	for _, interval := range intervals {
		candles := proj.GetCandles("AAPL", interval, from, to)
		require.Len(t, candles, 1, "expected 1 candle for interval %v", interval)
		assert.Equal(t, int64(1500000), candles[0].Open)
		assert.Equal(t, int64(10), candles[0].Volume)
	}
}

func TestCandleProjection_Empty(t *testing.T) {
	proj := orderbook.NewCandleProjection()

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	assert.Empty(t, proj.GetCandles("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M, from, to))
	assert.Nil(t, proj.GetLatestCandle("AAPL", orderbookv1.CandleInterval_CANDLE_INTERVAL_1M))
}

func tradeEvent(id, symbol string, price, quantity int64, executedAt time.Time) es.Event {
	return es.Event{
		Type: "TradeExecuted",
		Data: &orderbookv1.TradeExecuted{
			TradeId:    id,
			Symbol:     symbol,
			Price:      price,
			Quantity:   quantity,
			ExecutedAt: timestamppb.New(executedAt),
		},
	}
}
