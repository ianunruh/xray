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

func tradeEvt(sym string, price, qty int64, at time.Time, cross orderbookv1.CrossType) es.Event {
	return es.Event{
		Type: orderbook.EventTradeExecuted,
		Data: &orderbookv1.TradeExecuted{
			Symbol:     sym,
			Price:      price,
			Quantity:   qty,
			ExecutedAt: timestamppb.New(at),
			CrossType:  cross,
		},
	}
}

func TestLULDReference_EmptyReturnsNotOk(t *testing.T) {
	p := orderbook.NewLULDReferenceProjection(5 * time.Minute)
	_, ok := p.GetReference("AAPL", time.Now())
	assert.False(t, ok)
}

func TestLULDReference_VolumeWeightedAverage(t *testing.T) {
	p := orderbook.NewLULDReferenceProjection(5 * time.Minute)
	now := time.Now()

	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		tradeEvt("AAPL", 1500000, 100, now.Add(-3*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
		tradeEvt("AAPL", 1520000, 50, now.Add(-2*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
		tradeEvt("AAPL", 1480000, 50, now.Add(-1*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
	}))

	ref, ok := p.GetReference("AAPL", now)
	require.True(t, ok)
	// VWAP: (1500000*100 + 1520000*50 + 1480000*50) / 200 = 1500000.
	assert.Equal(t, int64(1500000), ref)
}

func TestLULDReference_EvictsOldSamples(t *testing.T) {
	p := orderbook.NewLULDReferenceProjection(5 * time.Minute)
	now := time.Now()

	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		// Old sample — outside window.
		tradeEvt("AAPL", 1000000, 100, now.Add(-10*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
		// In-window samples.
		tradeEvt("AAPL", 1500000, 100, now.Add(-1*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
	}))

	ref, ok := p.GetReference("AAPL", now)
	require.True(t, ok)
	assert.Equal(t, int64(1500000), ref) // old sample evicted
}

func TestLULDReference_SkipsAuctionCrosses(t *testing.T) {
	p := orderbook.NewLULDReferenceProjection(5 * time.Minute)
	now := time.Now()

	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		tradeEvt("AAPL", 1500000, 100, now.Add(-2*time.Minute), orderbookv1.CrossType_CROSS_TYPE_OPENING),
		tradeEvt("AAPL", 1600000, 100, now.Add(-1*time.Minute), orderbookv1.CrossType_CROSS_TYPE_CLOSING),
		tradeEvt("AAPL", 1700000, 100, now.Add(-30*time.Second), orderbookv1.CrossType_CROSS_TYPE_HALT_REOPEN),
	}))

	_, ok := p.GetReference("AAPL", now)
	assert.False(t, ok, "non-continuous crosses must not produce a reference")
}

func TestLULDReference_HaltClearsWindow(t *testing.T) {
	p := orderbook.NewLULDReferenceProjection(5 * time.Minute)
	now := time.Now()

	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		tradeEvt("AAPL", 1500000, 100, now.Add(-1*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
	}))
	_, ok := p.GetReference("AAPL", now)
	require.True(t, ok)

	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		{
			Type: orderbook.EventTradingHalted,
			Data: &orderbookv1.TradingHalted{
				Symbol:   "AAPL",
				At:       timestamppb.New(now),
				ReopenAt: timestamppb.New(now.Add(5 * time.Minute)),
			},
		},
	}))
	_, ok = p.GetReference("AAPL", now)
	assert.False(t, ok, "halt should clear stored samples")

	// Post-reopen continuous trade re-anchors the reference.
	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		tradeEvt("AAPL", 1300000, 100, now.Add(6*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
	}))
	ref, ok := p.GetReference("AAPL", now.Add(6*time.Minute))
	require.True(t, ok)
	assert.Equal(t, int64(1300000), ref)
}

// TestLULDReference_SplitReanchorClearsWindow: a SetLULDBands with
// reason="split_reanchor" must clear the per-symbol sample buffer so
// the next continuous trade re-anchors the VWAP at the new price level
// (pre-split prices are at the wrong scale).
func TestLULDReference_SplitReanchorClearsWindow(t *testing.T) {
	p := orderbook.NewLULDReferenceProjection(5 * time.Minute)
	now := time.Now()

	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		tradeEvt("AAPL", 1500000, 100, now.Add(-1*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
	}))
	_, ok := p.GetReference("AAPL", now)
	require.True(t, ok)

	// A non-reanchor bands event must NOT clear the window.
	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		{
			Type: orderbook.EventLULDBandsSet,
			Data: &orderbookv1.LULDBandsSet{
				Symbol: "AAPL", ReferencePrice: 1500000,
				UpperBand: 1575000, LowerBand: 1425000,
				BandBps: 500, Reason: "reference_update",
			},
		},
	}))
	_, ok = p.GetReference("AAPL", now)
	assert.True(t, ok, "reference_update must not clear samples")

	// The split_reanchor reason must clear the window.
	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		{
			Type: orderbook.EventLULDBandsSet,
			Data: &orderbookv1.LULDBandsSet{
				Symbol: "AAPL", ReferencePrice: 750000,
				UpperBand: 787500, LowerBand: 712500,
				BandBps: 500, Reason: "split_reanchor",
			},
		},
	}))
	_, ok = p.GetReference("AAPL", now)
	assert.False(t, ok, "split_reanchor must clear samples so the next trade anchors fresh")
}

func TestLULDReference_PerSymbol(t *testing.T) {
	p := orderbook.NewLULDReferenceProjection(5 * time.Minute)
	now := time.Now()

	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		tradeEvt("AAPL", 1500000, 100, now.Add(-1*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
		tradeEvt("MSFT", 4200000, 100, now.Add(-1*time.Minute), orderbookv1.CrossType_CROSS_TYPE_NONE),
	}))

	aapl, ok := p.GetReference("AAPL", now)
	require.True(t, ok)
	assert.Equal(t, int64(1500000), aapl)

	msft, ok := p.GetReference("MSFT", now)
	require.True(t, ok)
	assert.Equal(t, int64(4200000), msft)
}
