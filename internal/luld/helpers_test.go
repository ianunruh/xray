package luld_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

func tradeEvent(symbol string, price, qty int64) es.Event {
	return es.Event{
		AggregateID: orderbook.AggregateID(symbol),
		Type:        orderbook.EventTradeExecuted,
		Timestamp:   time.Now(),
		Data: &orderbookv1.TradeExecuted{
			Symbol:     symbol,
			Price:      price,
			Quantity:   qty,
			ExecutedAt: timestamppb.New(time.Now()),
			CrossType:  orderbookv1.CrossType_CROSS_TYPE_NONE,
		},
	}
}

func limitStateEntered(symbol string) es.Event {
	at := time.Now()
	return es.Event{
		AggregateID: orderbook.AggregateID(symbol),
		Type:        orderbook.EventLULDLimitStateEntered,
		Timestamp:   at,
		Data: &orderbookv1.LULDLimitStateEntered{
			Symbol:       symbol,
			BandSide:     orderbookv1.Side_SIDE_BUY,
			At:           timestamppb.New(at),
			HaltDeadline: timestamppb.New(at.Add(15 * time.Second)),
		},
	}
}

func limitStateExited(symbol, reason string) es.Event {
	return es.Event{
		AggregateID: orderbook.AggregateID(symbol),
		Type:        orderbook.EventLULDLimitStateExited,
		Timestamp:   time.Now(),
		Data: &orderbookv1.LULDLimitStateExited{
			Symbol: symbol,
			Reason: reason,
			At:     timestamppb.New(time.Now()),
		},
	}
}

func tradingHalted(symbol string) es.Event {
	at := time.Now()
	return es.Event{
		AggregateID: orderbook.AggregateID(symbol),
		Type:        orderbook.EventTradingHalted,
		Timestamp:   at,
		Data: &orderbookv1.TradingHalted{
			Symbol:   symbol,
			Reason:   "luld_limit_state_expired",
			At:       timestamppb.New(at),
			ReopenAt: timestamppb.New(at.Add(5 * time.Minute)),
		},
	}
}

func tradingResumed(symbol string) es.Event {
	return es.Event{
		AggregateID: orderbook.AggregateID(symbol),
		Type:        orderbook.EventTradingResumed,
		Timestamp:   time.Now(),
		Data: &orderbookv1.TradingResumed{
			Symbol:    symbol,
			At:        timestamppb.New(time.Now()),
			CrossType: orderbookv1.CrossType_CROSS_TYPE_HALT_REOPEN,
		},
	}
}

// seedAsk places a sell-side limit on the orderbook via the handler.
func seedAsk(t *testing.T, e *env, symbol string, price, qty int64) error {
	t.Helper()
	cmd := orderbook.PlaceOrder{
		Symbol:    symbol,
		Side:      orderbook.Sell,
		Price:     price,
		Quantity:  qty,
		AccountID: "seed",
	}
	return e.obHandler.Handle(e.ctx, cmd, func(b *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(b, cmd)
	})
}

// seedTrippedLimitState forces the orderbook into PhaseLimitState by
// applying LULDBandsSet + LULDLimitStateEntered directly via the
// handler. `now` controls the trip / deadline timestamps so the test
// can advance past LULDLimitStateGrace cleanly.
func seedTrippedLimitState(t *testing.T, e *env, symbol string, reference int64, now time.Time) error {
	t.Helper()
	upper := reference + reference*5/100
	lower := reference - reference*5/100

	// Step 1: stamp bands.
	bandsCmd := orderbook.SetLULDBands{
		Symbol:         symbol,
		ReferencePrice: reference,
		UpperBand:      upper,
		LowerBand:      lower,
		BandBps:        500,
		Reason:         "initial",
	}
	require.NoError(t, e.obHandler.Handle(e.ctx, bandsCmd, func(b *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteSetLULDBands(b, bandsCmd)
	}))

	// Step 2: directly write a LULDLimitStateEntered event by going
	// through the handler with a hand-crafted command. The cleanest
	// way is to use es.Event append; for a test we use the handler's
	// SaveEvents path indirectly via a no-op command-wrapper.
	deadline := now.Add(orderbook.LULDLimitStateGrace)
	enterEvt := es.Event{
		AggregateID: orderbook.AggregateID(symbol),
		Type:        orderbook.EventLULDLimitStateEntered,
		Timestamp:   now,
		Data: &orderbookv1.LULDLimitStateEntered{
			Symbol:       symbol,
			BandSide:     orderbookv1.Side_SIDE_BUY,
			BandPrice:    upper,
			At:           timestamppb.New(now),
			HaltDeadline: timestamppb.New(deadline),
		},
	}
	return e.obHandler.Handle(e.ctx, fakeCmd{aggregateID: orderbook.AggregateID(symbol)}, func(b *orderbook.OrderBook) ([]es.Event, error) {
		// Match the convention used by every real Execute*: apply the
		// event to the in-memory aggregate before returning it so the
		// handler's per-aggregate cache stays in sync with the store.
		if err := b.Apply(enterEvt); err != nil {
			return nil, err
		}
		return []es.Event{enterEvt}, nil
	})
}

// fakeCmd lets a test inject events without a real command type.
type fakeCmd struct{ aggregateID string }

func (c fakeCmd) AggregateID() string { return c.aggregateID }
