package orderbook

import (
	"context"
	"sync"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// LULDReferenceProjection maintains a rolling volume-weighted-average
// trade price per symbol over a sliding window (real LULD is 5 minutes).
// The LULD reactor reads this to compute the active price bands.
//
// Only continuous-matching trades (CrossType_NONE) are sampled — auction
// uncrosses and halt-reopen crosses are excluded per the real LULD spec
// (they're price-discovery events, not reference observations). When a
// symbol halts the reactor invokes OnHalt to clear stale samples so the
// reopening cross becomes the next reference; OnResume is a no-op for
// symmetry but reserved for future use.
//
// The projection is ephemeral: it rebuilds from the start of the event
// stream on every boot, like MarkProjection. No persistence.
type LULDReferenceProjection struct {
	mu      sync.RWMutex
	window  time.Duration
	samples map[string]*refWindow
}

type refSample struct {
	price int64
	qty   int64
	at    time.Time
}

// refWindow is a per-symbol sliding window of trade samples. samples
// is kept in append order (oldest first); evictAndTrim drops samples
// older than the cutoff before any read.
type refWindow struct {
	samples []refSample
}

// NewLULDReferenceProjection returns a projection with the given
// rolling-window duration (5 * time.Minute for production).
func NewLULDReferenceProjection(window time.Duration) *LULDReferenceProjection {
	return &LULDReferenceProjection{
		window:  window,
		samples: make(map[string]*refWindow),
	}
}

// HandleEvents samples qualifying TradeExecuted events into per-symbol
// sliding windows. Non-qualifying trades (auction crosses) are skipped.
func (p *LULDReferenceProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.TradeExecuted:
			if data.CrossType != orderbookv1.CrossType_CROSS_TYPE_NONE {
				continue
			}
			w := p.samples[data.Symbol]
			if w == nil {
				w = &refWindow{}
				p.samples[data.Symbol] = w
			}
			w.samples = append(w.samples, refSample{
				price: data.Price,
				qty:   data.Quantity,
				at:    data.ExecutedAt.AsTime(),
			})
		case *orderbookv1.TradingHalted:
			// Drop samples so the reopening cross becomes the next anchor.
			delete(p.samples, data.Symbol)
		}
	}
	return nil
}

// GetReference returns the current volume-weighted-average reference
// price for symbol relative to `now`, computed across the sliding
// window. Returns ok=false when no qualifying samples are in-window.
func (p *LULDReferenceProjection) GetReference(symbol string, now time.Time) (int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	w := p.samples[symbol]
	if w == nil {
		return 0, false
	}
	cutoff := now.Add(-p.window)
	// Find the first in-window sample. Samples are ordered by append,
	// which matches event-time order for a single-writer projection.
	idx := 0
	for idx < len(w.samples) && w.samples[idx].at.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		// Compact: trim oldest evicted samples so the window doesn't
		// grow without bound for hot symbols.
		w.samples = append(w.samples[:0], w.samples[idx:]...)
	}
	if len(w.samples) == 0 {
		return 0, false
	}
	var qtySum, weighted int64
	for _, s := range w.samples {
		weighted += s.price * s.qty
		qtySum += s.qty
	}
	if qtySum == 0 {
		return 0, false
	}
	return weighted / qtySum, true
}

// Reset clears all samples — used in tests and on the rare full-replay
// rebuild.
func (p *LULDReferenceProjection) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.samples = make(map[string]*refWindow)
}
