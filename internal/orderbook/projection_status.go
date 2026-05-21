package orderbook

import (
	"context"
	"sync"

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
