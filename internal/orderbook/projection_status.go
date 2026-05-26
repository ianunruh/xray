package orderbook

import (
	"context"
	"sync"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// marketStatus holds the per-symbol session metadata exposed by
// GetMarketStatus. phase is the domain enum whose zero value is
// PhaseContinuous, so an unseen symbol reports MARKET_PHASE_CONTINUOUS —
// matching what GetOrderBook returns for a freshly-created aggregate.
type marketStatus struct {
	phase          MarketPhase
	lastTradePrice int64
	sessionVolume  int64
	// LULD state — kept in sync with the aggregate via the LULD
	// lifecycle events so GetMarketStatus can serve halt banner /
	// band shading data without loading the aggregate.
	luldReferencePrice int64
	luldUpperBand      int64
	luldLowerBand      int64
	luldBandBps        int32
	luldHaltDeadline   time.Time
	luldReopenAt       time.Time
}

// LULDStatus bundles the LULD fields a status reader returns alongside
// phase / last-trade / session-volume.
type LULDStatus struct {
	ReferencePrice int64
	UpperBand      int64
	LowerBand      int64
	BandBps        int32
	HaltDeadline   time.Time
	ReopenAt       time.Time
}

// MarketStatusProjection maintains lightweight session metadata (phase,
// last trade price, session volume) per symbol, updated incrementally
// from the same events the aggregate's Apply uses for these fields. It
// lets GetMarketStatus answer in O(1) without replaying the aggregate or
// enumerating resting orders.
type MarketStatusProjection struct {
	mu      sync.RWMutex
	symbols map[string]*marketStatus
}

// NewMarketStatusProjection creates a new MarketStatusProjection.
func NewMarketStatusProjection() *MarketStatusProjection {
	return &MarketStatusProjection{
		symbols: make(map[string]*marketStatus),
	}
}

// HandleEvents processes events to maintain per-symbol session metadata.
func (p *MarketStatusProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.TradeExecuted:
			s := p.statusFor(data.Symbol)
			s.lastTradePrice = data.Price
			s.sessionVolume += data.Quantity
		case *orderbookv1.MarketPhaseChanged:
			p.statusFor(data.Symbol).phase = MarketPhaseFromProto(data.Phase)
		case *orderbookv1.OfficialCloseSet:
			p.statusFor(data.Symbol).sessionVolume = 0
		case *orderbookv1.SymbolRenamed:
			// Mirrors the aggregate: a renamed symbol's book is dead.
			p.statusFor(data.OldSymbol).phase = PhaseClosed
		case *orderbookv1.LULDBandsSet:
			s := p.statusFor(data.Symbol)
			s.luldReferencePrice = data.ReferencePrice
			s.luldUpperBand = data.UpperBand
			s.luldLowerBand = data.LowerBand
			s.luldBandBps = data.BandBps
		case *orderbookv1.LULDLimitStateEntered:
			s := p.statusFor(data.Symbol)
			s.phase = PhaseLimitState
			s.luldHaltDeadline = data.HaltDeadline.AsTime()
		case *orderbookv1.LULDLimitStateExited:
			s := p.statusFor(data.Symbol)
			s.luldHaltDeadline = time.Time{}
			// Phase mutation is handled by the subsequent
			// MarketPhaseChanged or TradingHalted event.
		case *orderbookv1.TradingHalted:
			s := p.statusFor(data.Symbol)
			s.phase = PhaseHalted
			s.luldReopenAt = data.ReopenAt.AsTime()
		case *orderbookv1.TradingResumed:
			s := p.statusFor(data.Symbol)
			s.luldReopenAt = time.Time{}
			s.luldHaltDeadline = time.Time{}
			// MarketPhaseChanged(CONTINUOUS) precedes this event in the
			// uncross batch and has already flipped the phase back.
		}
	}

	return nil
}

func (p *MarketStatusProjection) statusFor(symbol string) *marketStatus {
	s := p.symbols[symbol]
	if s == nil {
		s = &marketStatus{}
		p.symbols[symbol] = s
	}
	return s
}

// GetStatus returns the session metadata for a symbol. An unknown symbol
// reports the same defaults as a freshly-created aggregate: continuous
// phase, zero last-trade price, zero session volume.
func (p *MarketStatusProjection) GetStatus(symbol string) (phase orderbookv1.MarketPhase, lastTradePrice, sessionVolume int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	s := p.symbols[symbol]
	if s == nil {
		return MarketPhaseToProto(PhaseContinuous), 0, 0
	}
	return MarketPhaseToProto(s.phase), s.lastTradePrice, s.sessionVolume
}

// GetLULDStatus returns the LULD bands + timers for symbol. Zero
// values mean the relevant state is unset (no bands computed yet, no
// halt active).
func (p *MarketStatusProjection) GetLULDStatus(symbol string) LULDStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s := p.symbols[symbol]
	if s == nil {
		return LULDStatus{}
	}
	return LULDStatus{
		ReferencePrice: s.luldReferencePrice,
		UpperBand:      s.luldUpperBand,
		LowerBand:      s.luldLowerBand,
		BandBps:        s.luldBandBps,
		HaltDeadline:   s.luldHaltDeadline,
		ReopenAt:       s.luldReopenAt,
	}
}
