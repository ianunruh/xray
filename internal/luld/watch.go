package luld

import (
	"context"
	"sort"
	"sync"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// ActiveKind classifies what a symbol is doing right now.
type ActiveKind int

const (
	KindLimitState ActiveKind = 1
	KindHalted     ActiveKind = 2
)

// ActiveSymbol is what the reconciler needs to evaluate timer-based
// LULD transitions: which symbol, what kind of pause, ordered for
// stable iteration.
type ActiveSymbol struct {
	Symbol string
	Kind   ActiveKind
}

// ActiveSymbolsTracker is the read interface the reconciler consumes
// to iterate symbols that need a EvaluateLULDExpiry pass.
type ActiveSymbolsTracker interface {
	ListActiveSymbols(ctx context.Context) []ActiveSymbol
}

// InMemoryActiveSymbols maintains the set of symbols currently in
// PhaseLimitState or PhaseHalted, updated from LULD lifecycle events.
// Ephemeral — rebuilds from event-stream replay on each boot.
type InMemoryActiveSymbols struct {
	mu     sync.RWMutex
	active map[string]ActiveKind
}

func NewInMemoryActiveSymbols() *InMemoryActiveSymbols {
	return &InMemoryActiveSymbols{active: make(map[string]ActiveKind)}
}

func (p *InMemoryActiveSymbols) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.LULDLimitStateEntered:
			p.active[data.Symbol] = KindLimitState
		case *orderbookv1.LULDLimitStateExited:
			// If the exit was the "halt_triggered" path, the next event
			// (TradingHalted) will promote it back to KindHalted. If
			// the exit was "price_returned_in_band", removing here is
			// final — the subsequent MarketPhaseChanged is informational.
			if data.Reason == "price_returned_in_band" || data.Reason == "manual" {
				delete(p.active, data.Symbol)
			}
		case *orderbookv1.TradingHalted:
			p.active[data.Symbol] = KindHalted
		case *orderbookv1.TradingResumed:
			delete(p.active, data.Symbol)
		}
	}
	return nil
}

func (p *InMemoryActiveSymbols) ListActiveSymbols(_ context.Context) []ActiveSymbol {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]ActiveSymbol, 0, len(p.active))
	for sym, kind := range p.active {
		out = append(out, ActiveSymbol{Symbol: sym, Kind: kind})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}
