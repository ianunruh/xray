package trend

type Signal int

const (
	SignalNone Signal = iota
	SignalBuy
	SignalSell
)

type EMAState struct {
	FastEMA    float64
	SlowEMA    float64
	TradeCount int
	LastSignal Signal
	Primed     bool
}

type Strategy struct {
	FastPeriod int
	SlowPeriod int
}

func NewStrategy(cfg SymbolConfig) *Strategy {
	return &Strategy{
		FastPeriod: cfg.FastPeriod,
		SlowPeriod: cfg.SlowPeriod,
	}
}

func (s *Strategy) Update(state *EMAState, price int64) Signal {
	p := float64(price)
	fastAlpha := 2.0 / float64(s.FastPeriod+1)
	slowAlpha := 2.0 / float64(s.SlowPeriod+1)

	if state.TradeCount == 0 {
		state.FastEMA = p
		state.SlowEMA = p
	} else {
		state.FastEMA = p*fastAlpha + state.FastEMA*(1-fastAlpha)
		state.SlowEMA = p*slowAlpha + state.SlowEMA*(1-slowAlpha)
	}
	state.TradeCount++

	if state.TradeCount < s.SlowPeriod {
		return SignalNone
	}
	state.Primed = true

	var current Signal
	if state.FastEMA > state.SlowEMA {
		current = SignalBuy
	} else {
		current = SignalSell
	}

	if current != state.LastSignal {
		state.LastSignal = current
		return current
	}
	return SignalNone
}
