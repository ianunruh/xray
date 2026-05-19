package portfolio

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// PendingTotals is the per-account aggregation the GetPortfolio path
// consumes — credits and debits returned as positive magnitudes so
// the proto fields can carry them directly without a sign convention.
type PendingTotals struct {
	Credits int64
	Debits  int64
}

// PendingSettlementsReader is the read interface for the
// pending-settlements projection. Implemented by PgPendingProjection.
type PendingSettlementsReader interface {
	// AccountsWithDueSettlements returns distinct account IDs that
	// have at least one pending leg with settles_at <= before.
	AccountsWithDueSettlements(ctx context.Context, before time.Time) ([]string, error)
	// DueLegs returns every pending leg for the account that is due
	// for clearing as of `before`, oldest-first.
	DueLegs(ctx context.Context, accountID string, before time.Time) ([]PendingLegRow, error)
	// PendingTotals returns the per-account credit/debit sums for
	// every pending leg, signed by leg.CashAmount.
	PendingTotals(ctx context.Context, accountID string) (PendingTotals, error)
}

// PendingLegRow is the projected row the reactor consumes; mirrors
// the aggregate's PendingLeg with the addition of AccountID (the
// aggregate already knows its own).
type PendingLegRow struct {
	AccountID   string
	OrderSagaID string
	TradeID     string
	Kind        portfoliov1.SettlementLegKind
	Symbol      string
	CashAmount  int64
	SettlesAt   time.Time
}

// PgPendingProjection materializes one row per in-flight settlement
// leg from the four trade-date events and removes the row on
// SettlementCleared. Lives in its own consumer
// ("pending-settlements") so a rebuild here doesn't fight the main
// portfolio projection's cursor.
type PgPendingProjection struct {
	pool *pgxpool.Pool
}

func NewPgPendingProjection(pool *pgxpool.Pool) *PgPendingProjection {
	return &PgPendingProjection{pool: pool}
}

func (p *PgPendingProjection) Reset(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `TRUNCATE projection_pending_settlements`); err != nil {
		return fmt.Errorf("truncate projection_pending_settlements: %w", err)
	}
	return nil
}

func (p *PgPendingProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.CashSettled:
			if IsInstantSettlement(data.SettlesAt, data.SettledAt) {
				continue
			}
			p.queueInsert(batch, pendingInsert{
				AccountID:   data.AccountId,
				OrderSagaID: data.OrderSagaId,
				TradeID:     data.TradeId,
				Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_DEBIT,
				Symbol:      data.Symbol,
				CashAmount:  -data.Amount,
				SettlesAt:   data.SettlesAt.AsTime(),
				EmittedAt:   data.SettledAt.AsTime(),
			})
		case *portfoliov1.SharesSettled:
			if IsInstantSettlement(data.SettlesAt, data.SettledAt) {
				continue
			}
			p.queueInsert(batch, pendingInsert{
				AccountID:   data.AccountId,
				OrderSagaID: data.OrderSagaId,
				TradeID:     data.TradeId,
				Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT,
				Symbol:      data.Symbol,
				CashAmount:  data.Proceeds,
				SettlesAt:   data.SettlesAt.AsTime(),
				EmittedAt:   data.SettledAt.AsTime(),
			})
		case *portfoliov1.ShortOpened:
			if IsInstantSettlement(data.SettlesAt, data.OpenedAt) {
				continue
			}
			p.queueInsert(batch, pendingInsert{
				AccountID:   data.AccountId,
				OrderSagaID: data.OrderSagaId,
				TradeID:     data.TradeId,
				Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_SHORT_OPEN,
				Symbol:      data.Symbol,
				CashAmount:  0,
				SettlesAt:   data.SettlesAt.AsTime(),
				EmittedAt:   data.OpenedAt.AsTime(),
			})
		case *portfoliov1.ShortCovered:
			if IsInstantSettlement(data.SettlesAt, data.CoveredAt) {
				continue
			}
			delta := data.ProceedsReleased + data.CollateralReleased - data.Cost
			p.queueInsert(batch, pendingInsert{
				AccountID:   data.AccountId,
				OrderSagaID: data.OrderSagaId,
				TradeID:     data.TradeId,
				Kind:        portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_SHORT_COVER,
				Symbol:      data.Symbol,
				CashAmount:  delta,
				SettlesAt:   data.SettlesAt.AsTime(),
				EmittedAt:   data.CoveredAt.AsTime(),
			})
		case *portfoliov1.SettlementCleared:
			batch.Queue(
				`DELETE FROM projection_pending_settlements
				 WHERE account_id = $1 AND trade_id = $2 AND kind = $3`,
				data.AccountId, data.TradeId, int32(data.Kind),
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
			return fmt.Errorf("pending settlements projection: %w", err)
		}
	}
	return nil
}

type pendingInsert struct {
	AccountID   string
	OrderSagaID string
	TradeID     string
	Kind        portfoliov1.SettlementLegKind
	Symbol      string
	CashAmount  int64
	SettlesAt   time.Time
	EmittedAt   time.Time
}

func (p *PgPendingProjection) queueInsert(batch *pgx.Batch, ins pendingInsert) {
	// ON CONFLICT DO NOTHING: replays should be no-ops, the original
	// row already captures the leg correctly.
	batch.Queue(
		`INSERT INTO projection_pending_settlements
		 (account_id, trade_id, kind, order_saga_id, symbol, cash_amount, settles_at, emitted_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (account_id, trade_id, kind) DO NOTHING`,
		ins.AccountID, ins.TradeID, int32(ins.Kind), ins.OrderSagaID,
		ins.Symbol, ins.CashAmount, ins.SettlesAt, ins.EmittedAt,
	)
}

func (p *PgPendingProjection) AccountsWithDueSettlements(ctx context.Context, before time.Time) ([]string, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT DISTINCT account_id FROM projection_pending_settlements
		 WHERE settles_at <= $1 ORDER BY account_id`,
		before,
	)
	if err != nil {
		return nil, fmt.Errorf("query due accounts: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan due account: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (p *PgPendingProjection) DueLegs(ctx context.Context, accountID string, before time.Time) ([]PendingLegRow, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT account_id, order_saga_id, trade_id, kind, symbol, cash_amount, settles_at
		 FROM projection_pending_settlements
		 WHERE account_id = $1 AND settles_at <= $2
		 ORDER BY settles_at ASC`,
		accountID, before,
	)
	if err != nil {
		return nil, fmt.Errorf("query due legs: %w", err)
	}
	defer rows.Close()
	var out []PendingLegRow
	for rows.Next() {
		var (
			leg  PendingLegRow
			kind int32
		)
		if err := rows.Scan(&leg.AccountID, &leg.OrderSagaID, &leg.TradeID, &kind, &leg.Symbol, &leg.CashAmount, &leg.SettlesAt); err != nil {
			return nil, fmt.Errorf("scan due leg: %w", err)
		}
		leg.Kind = portfoliov1.SettlementLegKind(kind)
		out = append(out, leg)
	}
	return out, rows.Err()
}

func (p *PgPendingProjection) PendingTotals(ctx context.Context, accountID string) (PendingTotals, error) {
	var totals PendingTotals
	err := p.pool.QueryRow(ctx,
		`SELECT
			COALESCE(SUM(CASE WHEN cash_amount > 0 THEN cash_amount ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN cash_amount < 0 THEN -cash_amount ELSE 0 END), 0)
		 FROM projection_pending_settlements
		 WHERE account_id = $1`,
		accountID,
	).Scan(&totals.Credits, &totals.Debits)
	if err != nil {
		return PendingTotals{}, fmt.Errorf("query pending totals: %w", err)
	}
	return totals, nil
}
