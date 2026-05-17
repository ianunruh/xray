package portfolio

import (
	"context"
	"sort"
	"sync"
	"time"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// OpenCall is the bit of margin-call state the margincall reactor and
// reconciler need to know about a currently-active call: which account
// and when it was issued. Sized for a small population — the
// projection is in-memory because at any given moment we expect very
// few accounts under call simultaneously.
type OpenCall struct {
	AccountID string
	CallID    string
	IssuedAt  time.Time
}

// ActiveMarginCallsTracker is the read interface the reconciler uses
// to iterate currently-open calls without scanning every portfolio.
type ActiveMarginCallsTracker interface {
	ListOpenCalls(ctx context.Context) []OpenCall
}

// InMemoryActiveMarginCalls maintains the set of open calls keyed by
// account ID. Updated from MarginCallIssued / MarginCallCovered events.
// In-memory + ephemeral; rebuilds from event-stream replay on each boot.
type InMemoryActiveMarginCalls struct {
	mu    sync.RWMutex
	calls map[string]OpenCall
}

func NewInMemoryActiveMarginCalls() *InMemoryActiveMarginCalls {
	return &InMemoryActiveMarginCalls{calls: make(map[string]OpenCall)}
}

func (p *InMemoryActiveMarginCalls) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.MarginCallIssued:
			p.calls[data.AccountId] = OpenCall{
				AccountID: data.AccountId,
				CallID:    data.CallId,
				IssuedAt:  data.IssuedAt.AsTime(),
			}
		case *portfoliov1.MarginCallCovered:
			delete(p.calls, data.AccountId)
		}
	}
	return nil
}

func (p *InMemoryActiveMarginCalls) ListOpenCalls(_ context.Context) []OpenCall {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]OpenCall, 0, len(p.calls))
	for _, c := range p.calls {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AccountID < out[j].AccountID })
	return out
}
