package portfolio

import (
	"context"
	"sort"
	"sync"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// AccruableAccountsTracker is the read interface the fees accruer
// depends on — returns every accountID that has ever taken on a
// short or a long-on-margin position. Append-only: an account that
// previously had a liability stays in the set even after fully
// closing out, so the accruer can still tick a clock through dormant
// periods (the accruer skips cycles with zero amounts, but loads the
// portfolio to check).
type AccruableAccountsTracker interface {
	AccruableAccounts(ctx context.Context) ([]string, error)
}

// InMemoryAccruableAccounts is the in-memory, append-only set of
// accounts that have ever opened a short or settled a long buy.
// Rebuilt from the event stream on each consumer attach.
type InMemoryAccruableAccounts struct {
	mu  sync.RWMutex
	set map[string]struct{}
}

func NewInMemoryAccruableAccounts() *InMemoryAccruableAccounts {
	return &InMemoryAccruableAccounts{set: make(map[string]struct{})}
}

func (p *InMemoryAccruableAccounts) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.ShortOpened:
			p.set[data.AccountId] = struct{}{}
		case *portfoliov1.CashSettled:
			// Long-buy fill — could be a margin-loan buy. Add to the
			// set regardless; the accruer checks current loan state
			// on tick. Cheap to over-include.
			p.set[data.AccountId] = struct{}{}
		}
	}
	return nil
}

func (p *InMemoryAccruableAccounts) AccruableAccounts(_ context.Context) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.set))
	for id := range p.set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}
