package trend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStrategy_NoSignalBeforePrimed(t *testing.T) {
	s := &Strategy{FastPeriod: 3, SlowPeriod: 5}
	state := &EMAState{}

	for i := range 4 {
		signal := s.Update(state, 1000000)
		assert.Equal(t, SignalNone, signal, "trade %d should not produce a signal", i)
	}
	assert.False(t, state.Primed)
}

func TestStrategy_BuySignalOnCrossover(t *testing.T) {
	s := &Strategy{FastPeriod: 3, SlowPeriod: 5}
	state := &EMAState{}

	// Prime with stable prices
	for range 5 {
		s.Update(state, 1000000)
	}

	// Inject rising prices to push fast EMA above slow EMA
	var gotBuy bool
	for range 10 {
		signal := s.Update(state, 1200000)
		if signal == SignalBuy {
			gotBuy = true
			break
		}
	}
	assert.True(t, gotBuy, "expected a buy signal from rising prices")
}

func TestStrategy_SellSignalOnCrossover(t *testing.T) {
	s := &Strategy{FastPeriod: 3, SlowPeriod: 5}
	state := &EMAState{}

	// Prime with stable prices
	for range 5 {
		s.Update(state, 1000000)
	}

	// Push fast above slow first
	for range 10 {
		s.Update(state, 1200000)
	}

	// Now inject falling prices
	var gotSell bool
	for range 10 {
		signal := s.Update(state, 800000)
		if signal == SignalSell {
			gotSell = true
			break
		}
	}
	assert.True(t, gotSell, "expected a sell signal from falling prices")
}

func TestStrategy_NoRepeatedSignal(t *testing.T) {
	s := &Strategy{FastPeriod: 3, SlowPeriod: 5}
	state := &EMAState{}

	// Prime
	for range 5 {
		s.Update(state, 1000000)
	}

	// Trigger buy signal
	var buyCount int
	for range 20 {
		signal := s.Update(state, 1200000)
		if signal == SignalBuy {
			buyCount++
		}
	}
	assert.Equal(t, 1, buyCount, "buy signal should fire exactly once")
}

func TestStrategy_AlternatingSignals(t *testing.T) {
	s := &Strategy{FastPeriod: 3, SlowPeriod: 5}
	state := &EMAState{}

	// Prime
	for range 5 {
		s.Update(state, 1000000)
	}

	// Trigger buy
	var signals []Signal
	for range 10 {
		if signal := s.Update(state, 1200000); signal != SignalNone {
			signals = append(signals, signal)
		}
	}

	// Trigger sell
	for range 10 {
		if signal := s.Update(state, 800000); signal != SignalNone {
			signals = append(signals, signal)
		}
	}

	assert.Equal(t, []Signal{SignalBuy, SignalSell}, signals)
}

func TestStrategy_EMAValues(t *testing.T) {
	s := &Strategy{FastPeriod: 3, SlowPeriod: 5}
	state := &EMAState{}

	// First trade seeds both EMAs
	s.Update(state, 1000000)
	assert.InDelta(t, 1000000.0, state.FastEMA, 0.01)
	assert.InDelta(t, 1000000.0, state.SlowEMA, 0.01)

	// Second trade: fast alpha = 2/4 = 0.5, slow alpha = 2/6 = 0.333...
	s.Update(state, 1100000)
	assert.InDelta(t, 1050000.0, state.FastEMA, 0.01) // 1100000*0.5 + 1000000*0.5
	assert.InDelta(t, 1033333.33, state.SlowEMA, 1.0)  // 1100000*0.333 + 1000000*0.667
}
