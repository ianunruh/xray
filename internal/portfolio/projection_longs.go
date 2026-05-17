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

// LongsTracker is the read interface — mirrors ShortsTracker but for
// longs. Used by the margin-call reactor to identify accounts with
// long-on-margin exposure when a mark moves.
type LongsTracker interface {
	AccountsWithLong(ctx context.Context, symbol string) ([]string, error)
}

// InMemoryLongsBySymbol mirrors InMemoryShortsBySymbol for longs.
// Used in tests; production wires the PG-backed version.
type InMemoryLongsBySymbol struct {
	mu sync.RWMutex
	by map[string]map[string]int64
}

func NewInMemoryLongsBySymbol() *InMemoryLongsBySymbol {
	return &InMemoryLongsBySymbol{by: make(map[string]map[string]int64)}
}

func (p *InMemoryLongsBySymbol) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.CashSettled:
			p.adjust(data.Symbol, data.AccountId, data.Quantity)
		case *portfoliov1.SharesCredited:
			p.adjust(data.Symbol, data.AccountId, data.Quantity)
		case *portfoliov1.SharesSettled:
			p.adjust(data.Symbol, data.AccountId, -data.Quantity)
		case *portfoliov1.SharesDebited:
			p.adjust(data.Symbol, data.AccountId, -data.Quantity)
		}
	}
	return nil
}

func (p *InMemoryLongsBySymbol) adjust(symbol, accountID string, delta int64) {
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

func (p *InMemoryLongsBySymbol) AccountsWithLong(_ context.Context, symbol string) ([]string, error) {
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

// PgLongsBySymbolProjection persists per-symbol long positions so the
// margincall reactor can be persistent (lives in the same consumer as
// this projection).
type PgLongsBySymbolProjection struct {
	pool *pgxpool.Pool
}

func NewPgLongsBySymbolProjection(pool *pgxpool.Pool) *PgLongsBySymbolProjection {
	return &PgLongsBySymbolProjection{pool: pool}
}

func (p *PgLongsBySymbolProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.CashSettled:
			batch.Queue(longUpsertSQL,
				data.Symbol, data.AccountId, data.Quantity)
		case *portfoliov1.SharesCredited:
			batch.Queue(longUpsertSQL,
				data.Symbol, data.AccountId, data.Quantity)
		case *portfoliov1.SharesSettled:
			batch.Queue(longDeltaSQL,
				-data.Quantity, data.Symbol, data.AccountId)
			batch.Queue(longCleanupSQL, data.Symbol, data.AccountId)
		case *portfoliov1.SharesDebited:
			batch.Queue(longDeltaSQL,
				-data.Quantity, data.Symbol, data.AccountId)
			batch.Queue(longCleanupSQL, data.Symbol, data.AccountId)
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	br := p.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range batch.Len() {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("longs projection: %w", err)
		}
	}
	return nil
}

const (
	longUpsertSQL = `INSERT INTO projection_longs_by_symbol (symbol, account_id, quantity)
		VALUES ($1, $2, $3)
		ON CONFLICT (symbol, account_id) DO UPDATE SET
			quantity = projection_longs_by_symbol.quantity + $3`
	longDeltaSQL = `UPDATE projection_longs_by_symbol
		SET quantity = quantity + $1
		WHERE symbol = $2 AND account_id = $3`
	longCleanupSQL = `DELETE FROM projection_longs_by_symbol
		WHERE symbol = $1 AND account_id = $2 AND quantity <= 0`
)

func (p *PgLongsBySymbolProjection) AccountsWithLong(ctx context.Context, symbol string) ([]string, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT account_id FROM projection_longs_by_symbol
		WHERE symbol = $1 AND quantity > 0
		ORDER BY account_id`,
		symbol,
	)
	if err != nil {
		return nil, fmt.Errorf("query longs: %w", err)
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
