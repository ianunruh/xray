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

func TestMarkProjection_TradeUpdatesMark(t *testing.T) {
	proj := orderbook.NewMarkProjection()
	ctx := context.Background()

	t0 := time.Now()
	require.NoError(t, proj.HandleEvents(ctx, []es.Event{
		{Type: "TradeExecuted", Data: &orderbookv1.TradeExecuted{
			TradeId: "t1", Symbol: "AAPL", Price: 1500000, Quantity: 10,
			ExecutedAt: timestamppb.New(t0),
		}},
	}))

	m, ok := proj.GetMark("AAPL")
	require.True(t, ok)
	assert.Equal(t, int64(1500000), m.Price)
	assert.Equal(t, orderbook.MarkSourceTrade, m.Source)
}

func TestMarkProjection_LaterTradeWins(t *testing.T) {
	proj := orderbook.NewMarkProjection()
	ctx := context.Background()
	t0 := time.Now()

	require.NoError(t, proj.HandleEvents(ctx, []es.Event{
		{Type: "TradeExecuted", Data: &orderbookv1.TradeExecuted{
			TradeId: "t1", Symbol: "AAPL", Price: 1500000,
			ExecutedAt: timestamppb.New(t0),
		}},
		{Type: "TradeExecuted", Data: &orderbookv1.TradeExecuted{
			TradeId: "t2", Symbol: "AAPL", Price: 1520000,
			ExecutedAt: timestamppb.New(t0.Add(time.Second)),
		}},
	}))

	m, _ := proj.GetMark("AAPL")
	assert.Equal(t, int64(1520000), m.Price)
}

func TestMarkProjection_EarlierTradeIgnored(t *testing.T) {
	proj := orderbook.NewMarkProjection()
	ctx := context.Background()
	t0 := time.Now()

	require.NoError(t, proj.HandleEvents(ctx, []es.Event{
		{Type: "TradeExecuted", Data: &orderbookv1.TradeExecuted{
			TradeId: "t1", Symbol: "AAPL", Price: 1520000,
			ExecutedAt: timestamppb.New(t0.Add(time.Second)),
		}},
		// Out-of-order delivery — older trade should not overwrite.
		{Type: "TradeExecuted", Data: &orderbookv1.TradeExecuted{
			TradeId: "t0", Symbol: "AAPL", Price: 1500000,
			ExecutedAt: timestamppb.New(t0),
		}},
	}))

	m, _ := proj.GetMark("AAPL")
	assert.Equal(t, int64(1520000), m.Price)
}

func TestMarkProjection_OfficialCloseOverridesIntradayTrade(t *testing.T) {
	proj := orderbook.NewMarkProjection()
	ctx := context.Background()
	t0 := time.Now()

	require.NoError(t, proj.HandleEvents(ctx, []es.Event{
		{Type: "TradeExecuted", Data: &orderbookv1.TradeExecuted{
			TradeId: "t1", Symbol: "AAPL", Price: 1500000,
			ExecutedAt: timestamppb.New(t0),
		}},
		{Type: "OfficialCloseSet", Data: &orderbookv1.OfficialCloseSet{
			Symbol: "AAPL", SessionDate: "2026-05-16", ClosePrice: 1525000,
			At: timestamppb.New(t0.Add(time.Hour)),
		}},
	}))

	m, _ := proj.GetMark("AAPL")
	assert.Equal(t, int64(1525000), m.Price)
	assert.Equal(t, orderbook.MarkSourceClose, m.Source)
}

func TestMarkProjection_MissingSymbolReturnsFalse(t *testing.T) {
	proj := orderbook.NewMarkProjection()
	_, ok := proj.GetMark("NVDA")
	assert.False(t, ok)
}

func TestMarkProjection_GetMarkPrice(t *testing.T) {
	proj := orderbook.NewMarkProjection()
	ctx := context.Background()
	t0 := time.Now()

	require.NoError(t, proj.HandleEvents(ctx, []es.Event{
		{Type: "TradeExecuted", Data: &orderbookv1.TradeExecuted{
			TradeId: "t1", Symbol: "AAPL", Price: 1500000,
			ExecutedAt: timestamppb.New(t0),
		}},
	}))

	price, _, ok := proj.GetMarkPrice("AAPL")
	require.True(t, ok)
	assert.Equal(t, int64(1500000), price)

	_, _, ok = proj.GetMarkPrice("NVDA")
	assert.False(t, ok)
}
