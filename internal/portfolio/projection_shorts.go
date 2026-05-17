package portfolio

import (
	"context"
	"sort"
	"sync"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// ShortsBySymbolProjection maintains the set of accounts that currently
// hold an open short in each symbol. The margin-call reactor uses it
// to identify which accounts to recheck when a mark moves: scanning
// every portfolio on every trade is O(accounts), this is O(accounts
// short in symbol).
//
// In-memory; rebuilds from event-stream replay on each boot. Tracks
// per-account quantity so the same projection can answer "who holds
// the largest short in X" if needed.
type ShortsBySymbolProjection struct {
	mu sync.RWMutex
	// symbol -> account -> quantity
	by map[string]map[string]int64
}

func NewShortsBySymbolProjection() *ShortsBySymbolProjection {
	return &ShortsBySymbolProjection{by: make(map[string]map[string]int64)}
}

func (p *ShortsBySymbolProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.ShortOpened:
			p.adjust(data.Symbol, data.AccountId, data.Quantity)
		case *portfoliov1.ShortCovered:
			p.adjust(data.Symbol, data.AccountId, -data.Quantity)
		}
	}
	return nil
}

func (p *ShortsBySymbolProjection) adjust(symbol, accountID string, delta int64) {
	accounts := p.by[symbol]
	if accounts == nil {
		accounts = make(map[string]int64)
		p.by[symbol] = accounts
	}
	accounts[accountID] += delta
	if accounts[accountID] <= 0 {
		delete(accounts, accountID)
	}
	if len(accounts) == 0 {
		delete(p.by, symbol)
	}
}

// AccountsWithShort returns account IDs holding any open short in
// symbol, sorted for deterministic iteration.
func (p *ShortsBySymbolProjection) AccountsWithShort(symbol string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	accounts := p.by[symbol]
	out := make([]string, 0, len(accounts))
	for a := range accounts {
		out = append(out, a)
	}
	sort.Strings(out)
	return out
}

// ShortQuantity returns the open short quantity for (symbol, account),
// or 0 if none.
func (p *ShortsBySymbolProjection) ShortQuantity(symbol, accountID string) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if accounts, ok := p.by[symbol]; ok {
		return accounts[accountID]
	}
	return 0
}
