package portfolio

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// ShortsTracker is the read side the margin-call reactor depends on.
// Implemented by InMemoryShortsBySymbol (for tests) and
// PgShortsBySymbolProjection (for production).
type ShortsTracker interface {
	AccountsWithShort(ctx context.Context, symbol string) ([]string, error)
}

// InMemoryShortsBySymbol maintains the set of accounts that currently
// hold an open short in each symbol. In-memory; rebuilds from
// event-stream replay on each boot. Useful in tests; production wires
// PgShortsBySymbolProjection instead so the reactor can be persistent
// without a state-rebuild race.
type InMemoryShortsBySymbol struct {
	mu sync.RWMutex
	// symbol -> account -> quantity
	by map[string]map[string]int64
}

func NewInMemoryShortsBySymbol() *InMemoryShortsBySymbol {
	return &InMemoryShortsBySymbol{by: make(map[string]map[string]int64)}
}

func (p *InMemoryShortsBySymbol) HandleEvents(_ context.Context, events []es.Event) error {
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

func (p *InMemoryShortsBySymbol) adjust(symbol, accountID string, delta int64) {
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

func (p *InMemoryShortsBySymbol) AccountsWithShort(_ context.Context, symbol string) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	accounts := p.by[symbol]
	out := make([]string, 0, len(accounts))
	for a := range accounts {
		out = append(out, a)
	}
	sort.Strings(out)
	return out, nil
}

// PgShortsBySymbolProjection persists per-symbol short positions to
// Postgres so the margin-call reactor can be persistent (the projection
// and reactor share a checkpoint when colocated in the same consumer,
// guaranteeing the table reflects every event the reactor has seen).
type PgShortsBySymbolProjection struct {
	pool *pgxpool.Pool
}

func NewPgShortsBySymbolProjection(pool *pgxpool.Pool) *PgShortsBySymbolProjection {
	return &PgShortsBySymbolProjection{pool: pool}
}

func (p *PgShortsBySymbolProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.ShortOpened:
			batch.Queue(
				`INSERT INTO projection_shorts_by_symbol (symbol, account_id, quantity)
				VALUES ($1, $2, $3)
				ON CONFLICT (symbol, account_id) DO UPDATE SET
					quantity = projection_shorts_by_symbol.quantity + $3`,
				data.Symbol, data.AccountId, data.Quantity,
			)
		case *portfoliov1.ShortCovered:
			batch.Queue(
				`UPDATE projection_shorts_by_symbol
				SET quantity = quantity - $1
				WHERE symbol = $2 AND account_id = $3`,
				data.Quantity, data.Symbol, data.AccountId,
			)
			batch.Queue(
				`DELETE FROM projection_shorts_by_symbol
				WHERE symbol = $1 AND account_id = $2 AND quantity <= 0`,
				data.Symbol, data.AccountId,
			)
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	br := p.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range batch.Len() {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("shorts projection: %w", err)
		}
	}
	return nil
}

func (p *PgShortsBySymbolProjection) AccountsWithShort(ctx context.Context, symbol string) ([]string, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT account_id FROM projection_shorts_by_symbol
		WHERE symbol = $1 AND quantity > 0
		ORDER BY account_id`,
		symbol,
	)
	if err != nil {
		return nil, fmt.Errorf("query shorts: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, fmt.Errorf("scan account: %w", err)
		}
		out = append(out, a)
	}
	return out, nil
}

// SymbolShortInterest is the venue-wide aggregate for one symbol —
// every account's open short position summed, plus the count of
// accounts contributing.
type SymbolShortInterest struct {
	Symbol       string
	TotalQty     int64
	AccountCount int32
}

// ListShortInterest returns one row per symbol with at least one open
// short, sorted by symbol. Used by the venue-wide short-interest panel.
func (p *PgShortsBySymbolProjection) ListShortInterest(ctx context.Context) ([]*SymbolShortInterest, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT symbol, SUM(quantity), COUNT(*)
		FROM projection_shorts_by_symbol
		WHERE quantity > 0
		GROUP BY symbol
		ORDER BY symbol`,
	)
	if err != nil {
		return nil, fmt.Errorf("query short interest: %w", err)
	}
	defer rows.Close()
	var out []*SymbolShortInterest
	for rows.Next() {
		var (
			sym    string
			total  int64
			count  int32
		)
		if err := rows.Scan(&sym, &total, &count); err != nil {
			return nil, fmt.Errorf("scan short interest: %w", err)
		}
		out = append(out, &SymbolShortInterest{
			Symbol:       sym,
			TotalQty:     total,
			AccountCount: count,
		})
	}
	return out, rows.Err()
}
