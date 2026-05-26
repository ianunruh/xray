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

// TestMarketStatusProjection_TracksLULD verifies the in-memory status
// projection mirrors the aggregate's LULD fields, so GetMarketStatus
// can serve banner state without loading the aggregate.
func TestMarketStatusProjection_TracksLULD(t *testing.T) {
	p := orderbook.NewMarketStatusProjection()
	now := time.Now().UTC().Truncate(time.Microsecond)

	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		{
			Type: orderbook.EventLULDBandsSet,
			Data: &orderbookv1.LULDBandsSet{
				Symbol:         "AAPL",
				ReferencePrice: 1500000,
				UpperBand:      1575000,
				LowerBand:      1425000,
				BandBps:        500,
			},
		},
	}))
	luld := p.GetLULDStatus("AAPL")
	assert.Equal(t, int64(1500000), luld.ReferencePrice)
	assert.Equal(t, int64(1575000), luld.UpperBand)
	assert.Equal(t, int64(1425000), luld.LowerBand)
	assert.Equal(t, int32(500), luld.BandBps)

	// Limit state stamps a halt deadline and flips phase.
	deadline := now.Add(15 * time.Second)
	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		{
			Type: orderbook.EventLULDLimitStateEntered,
			Data: &orderbookv1.LULDLimitStateEntered{
				Symbol:       "AAPL",
				BandSide:     orderbookv1.Side_SIDE_BUY,
				BandPrice:    1575000,
				At:           timestamppb.New(now),
				HaltDeadline: timestamppb.New(deadline),
			},
		},
	}))
	phase, _, _ := p.GetStatus("AAPL")
	assert.Equal(t, orderbookv1.MarketPhase_MARKET_PHASE_LIMIT_STATE, phase)
	luld = p.GetLULDStatus("AAPL")
	assert.Equal(t, deadline, luld.HaltDeadline)

	// Escalation: exit + halt. Phase becomes HALTED, reopen-at is set.
	reopen := now.Add(5 * time.Minute)
	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		{
			Type: orderbook.EventLULDLimitStateExited,
			Data: &orderbookv1.LULDLimitStateExited{Symbol: "AAPL", Reason: "halt_triggered"},
		},
		{
			Type: orderbook.EventTradingHalted,
			Data: &orderbookv1.TradingHalted{
				Symbol:   "AAPL",
				Reason:   "luld_limit_state_expired",
				At:       timestamppb.New(now),
				ReopenAt: timestamppb.New(reopen),
			},
		},
	}))
	phase, _, _ = p.GetStatus("AAPL")
	assert.Equal(t, orderbookv1.MarketPhase_MARKET_PHASE_HALTED, phase)
	luld = p.GetLULDStatus("AAPL")
	assert.True(t, luld.HaltDeadline.IsZero(), "limit-state deadline clears on exit")
	assert.Equal(t, reopen, luld.ReopenAt)

	// Resume clears halt timers; phase flip back to CONTINUOUS comes
	// via the MarketPhaseChanged event in the reopen uncross batch.
	require.NoError(t, p.HandleEvents(context.Background(), []es.Event{
		{
			Type: orderbook.EventMarketPhaseChanged,
			Data: &orderbookv1.MarketPhaseChanged{
				Symbol: "AAPL",
				Phase:  orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS,
			},
		},
		{
			Type: orderbook.EventTradingResumed,
			Data: &orderbookv1.TradingResumed{
				Symbol:    "AAPL",
				At:        timestamppb.New(now.Add(5 * time.Minute)),
				CrossType: orderbookv1.CrossType_CROSS_TYPE_HALT_REOPEN,
			},
		},
	}))
	phase, _, _ = p.GetStatus("AAPL")
	assert.Equal(t, orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS, phase)
	luld = p.GetLULDStatus("AAPL")
	assert.True(t, luld.ReopenAt.IsZero(), "reopen-at clears on resume")
}
