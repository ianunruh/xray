package orderbook_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

// TestApply_LULDBandsSet stamps reference + band fields onto the aggregate.
func TestApply_LULDBandsSet(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))

	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventLULDBandsSet,
		Data: &orderbookv1.LULDBandsSet{
			Symbol:         "AAPL",
			ReferencePrice: 1500000,
			UpperBand:      1575000, // +5%
			LowerBand:      1425000, // -5%
			BandBps:        500,
			Reason:         "initial",
			At:             timestamppb.New(time.Now()),
		},
	}))

	assert.Equal(t, int64(1500000), book.LULDReferencePrice)
	assert.Equal(t, int64(1575000), book.LULDUpperBand)
	assert.Equal(t, int64(1425000), book.LULDLowerBand)
	assert.Equal(t, int32(500), book.LULDBandBps)
}

// TestApply_LULDLimitStateEntered transitions to PhaseLimitState and
// stamps the grace deadline.
func TestApply_LULDLimitStateEntered(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	at := time.Now().UTC().Truncate(time.Microsecond)
	deadline := at.Add(15 * time.Second)

	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventLULDLimitStateEntered,
		Data: &orderbookv1.LULDLimitStateEntered{
			Symbol:       "AAPL",
			BandSide:     orderbookv1.Side_SIDE_BUY,
			BandPrice:    1575000,
			At:           timestamppb.New(at),
			HaltDeadline: timestamppb.New(deadline),
		},
	}))

	assert.Equal(t, orderbook.PhaseLimitState, book.Phase)
	assert.Equal(t, at, book.LULDLimitStateStartedAt)
	assert.Equal(t, deadline, book.LULDHaltDeadline)
}

// TestApply_TradingHalted_ThenResumed cycles into HALTED and back out
// via TradingResumed, verifying that the re-arm window is set.
func TestApply_TradingHalted_ThenResumed(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))

	at := time.Now().UTC().Truncate(time.Microsecond)
	reopen := at.Add(5 * time.Minute)
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventTradingHalted,
		Data: &orderbookv1.TradingHalted{
			Symbol:   "AAPL",
			Reason:   "luld_limit_state_expired",
			At:       timestamppb.New(at),
			ReopenAt: timestamppb.New(reopen),
		},
	}))

	assert.Equal(t, orderbook.PhaseHalted, book.Phase)
	assert.Equal(t, at, book.LULDHaltStartedAt)
	assert.Equal(t, reopen, book.LULDReopenAt)

	// TradingResumed follows the uncross's MarketPhaseChanged(CONTINUOUS).
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventMarketPhaseChanged,
		Data: &orderbookv1.MarketPhaseChanged{
			Symbol: "AAPL",
			Phase:  orderbookv1.MarketPhase_MARKET_PHASE_CONTINUOUS,
		},
	}))
	resumedAt := at.Add(5 * time.Minute)
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventTradingResumed,
		Data: &orderbookv1.TradingResumed{
			Symbol:    "AAPL",
			At:        timestamppb.New(resumedAt),
			CrossType: orderbookv1.CrossType_CROSS_TYPE_HALT_REOPEN,
		},
	}))

	assert.Equal(t, orderbook.PhaseContinuous, book.Phase)
	assert.True(t, book.LULDHaltStartedAt.IsZero())
	assert.True(t, book.LULDReopenAt.IsZero())
	assert.Equal(t, resumedAt.Add(orderbook.LULDPostReopenRearm), book.LULDRearmAt)
}

// TestPlaceOrder_HaltedRejectsEverything verifies orders against a
// halted book are rejected with ErrSymbolHalted.
func TestPlaceOrder_HaltedRejectsEverything(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventTradingHalted,
		Data: &orderbookv1.TradingHalted{
			Symbol:   "AAPL",
			Reason:   "luld_limit_state_expired",
			At:       timestamppb.New(time.Now()),
			ReopenAt: timestamppb.New(time.Now().Add(5 * time.Minute)),
		},
	}))

	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1500000,
		Quantity: 100,
	})
	require.ErrorIs(t, err, orderbook.ErrSymbolHalted)
}

// TestPlaceOrder_LimitState_AcceptsAtBand_RejectsThroughBand drives the
// place-order phase guard: in PhaseLimitState a buy at-or-below the
// upper band rests; a buy through the upper band returns
// ErrLULDPriceOutsideBand.
func TestPlaceOrder_LimitState_AcceptsAtBand_RejectsThroughBand(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	// Seed bands + enter limit state.
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventLULDBandsSet,
		Data: &orderbookv1.LULDBandsSet{
			Symbol:         "AAPL",
			ReferencePrice: 1500000,
			UpperBand:      1575000,
			LowerBand:      1425000,
			BandBps:        500,
		},
	}))
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventLULDLimitStateEntered,
		Data: &orderbookv1.LULDLimitStateEntered{
			Symbol:       "AAPL",
			BandSide:     orderbookv1.Side_SIDE_BUY,
			BandPrice:    1575000,
			At:           timestamppb.New(time.Now()),
			HaltDeadline: timestamppb.New(time.Now().Add(15 * time.Second)),
		},
	}))

	// Market orders are rejected outright.
	_, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:      "AAPL",
		Side:        orderbook.Buy,
		Quantity:    50,
		OrderType:   orderbook.Market,
		TimeInForce: orderbook.IOC,
	})
	require.ErrorIs(t, err, orderbook.ErrLULDMarketRejected)

	// Buy through the upper band: rejected.
	_, err = orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1580000, // above upper band
		Quantity: 50,
	})
	require.ErrorIs(t, err, orderbook.ErrLULDPriceOutsideBand)

	// Buy at the upper band: accepted (rests).
	evts, err := orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Buy,
		Price:    1575000,
		Quantity: 50,
	})
	require.NoError(t, err)
	require.Len(t, evts, 1) // OrderPlaced only — no matching mid-limit-state
	assert.Equal(t, orderbook.EventOrderPlaced, evts[0].Type)

	// Sell below lower band: rejected.
	_, err = orderbook.ExecutePlaceOrder(book, orderbook.PlaceOrder{
		Symbol:   "AAPL",
		Side:     orderbook.Sell,
		Price:    1420000, // below lower band
		Quantity: 50,
	})
	require.ErrorIs(t, err, orderbook.ErrLULDPriceOutsideBand)
}

// TestOrderBook_Snapshot_RoundTrip_LULD verifies the LULD fields round
// trip through snapshot serialization, including timestamps.
func TestOrderBook_Snapshot_RoundTrip_LULD(t *testing.T) {
	book := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	now := time.Now().UTC().Truncate(time.Microsecond)

	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventLULDBandsSet,
		Data: &orderbookv1.LULDBandsSet{
			Symbol:         "AAPL",
			ReferencePrice: 1500000,
			UpperBand:      1575000,
			LowerBand:      1425000,
			BandBps:        500,
		},
	}))
	require.NoError(t, book.Apply(es.Event{
		Type: orderbook.EventLULDLimitStateEntered,
		Data: &orderbookv1.LULDLimitStateEntered{
			Symbol:       "AAPL",
			BandSide:     orderbookv1.Side_SIDE_BUY,
			BandPrice:    1575000,
			At:           timestamppb.New(now),
			HaltDeadline: timestamppb.New(now.Add(15 * time.Second)),
		},
	}))

	snapMsg, err := book.Snapshot()
	require.NoError(t, err)
	data, err := proto.Marshal(snapMsg)
	require.NoError(t, err)

	restored := orderbook.NewOrderBook(orderbook.AggregateID("AAPL"))
	var snap orderbookv1.OrderBookSnapshot
	require.NoError(t, proto.Unmarshal(data, &snap))
	require.NoError(t, restored.RestoreSnapshot(&snap))

	assert.Equal(t, orderbook.PhaseLimitState, restored.Phase)
	assert.Equal(t, int64(1500000), restored.LULDReferencePrice)
	assert.Equal(t, int64(1575000), restored.LULDUpperBand)
	assert.Equal(t, int64(1425000), restored.LULDLowerBand)
	assert.Equal(t, int32(500), restored.LULDBandBps)
	assert.Equal(t, now, restored.LULDLimitStateStartedAt)
	assert.Equal(t, now.Add(15*time.Second), restored.LULDHaltDeadline)
	// Unset fields restore as zero time.
	assert.True(t, restored.LULDHaltStartedAt.IsZero())
	assert.True(t, restored.LULDReopenAt.IsZero())
	assert.True(t, restored.LULDRearmAt.IsZero())
}
