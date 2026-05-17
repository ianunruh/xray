package orderbook

import (
	"context"
	"sync"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// Mark is a per-symbol "last known price" used for mark-to-market and
// margin computation. Source distinguishes intraday trade prints from
// session-end official closes so consumers can decide how stale a mark
// is allowed to be.
type Mark struct {
	Price  int64
	At     time.Time
	Source MarkSource
}

type MarkSource int

const (
	MarkSourceUnknown MarkSource = iota
	MarkSourceTrade              // intraday TradeExecuted
	MarkSourceClose              // OfficialCloseSet at session end
)

// MarkProjection maintains the most recent observed price per symbol.
// Updates from TradeExecuted (intraday) and OfficialCloseSet (session
// end); the close takes precedence over any earlier intraday trade
// prints with timestamps before the close.
type MarkProjection struct {
	mu    sync.RWMutex
	marks map[string]Mark
}

func NewMarkProjection() *MarkProjection {
	return &MarkProjection{marks: make(map[string]Mark)}
}

func (p *MarkProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.TradeExecuted:
			at := data.ExecutedAt.AsTime()
			existing, ok := p.marks[data.Symbol]
			// Newer events win. Equal-timestamp ties go to whichever
			// event arrived later — fine for a single-writer setup.
			if !ok || !at.Before(existing.At) {
				p.marks[data.Symbol] = Mark{
					Price:  data.Price,
					At:     at,
					Source: MarkSourceTrade,
				}
			}
		case *orderbookv1.OfficialCloseSet:
			at := data.At.AsTime()
			existing, ok := p.marks[data.Symbol]
			if !ok || !at.Before(existing.At) {
				p.marks[data.Symbol] = Mark{
					Price:  data.ClosePrice,
					At:     at,
					Source: MarkSourceClose,
				}
			}
		}
	}
	return nil
}

// GetMark returns the most recent mark for symbol. ok=false when no
// trade or close has been seen yet for that symbol.
func (p *MarkProjection) GetMark(symbol string) (Mark, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	m, ok := p.marks[symbol]
	return m, ok
}

// GetMarkPrice is the tuple-shaped variant of GetMark used by the
// portfolio.Marker interface — callers that don't need MarkSource
// avoid leaking the orderbook.Mark struct into their package.
func (p *MarkProjection) GetMarkPrice(symbol string) (int64, time.Time, bool) {
	m, ok := p.GetMark(symbol)
	if !ok {
		return 0, time.Time{}, false
	}
	return m.Price, m.At, true
}

// GetMarks fetches marks for a batch of symbols. Symbols without a
// mark are omitted from the result map.
func (p *MarkProjection) GetMarks(symbols []string) map[string]Mark {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]Mark, len(symbols))
	for _, s := range symbols {
		if m, ok := p.marks[s]; ok {
			out[s] = m
		}
	}
	return out
}
