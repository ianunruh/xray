package portfolio

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// FeesReader is the read interface for the fee-history projection,
// satisfied by PgFeesProjection.
type FeesReader interface {
	ListFeeHistory(ctx context.Context, accountID string, limit int32) ([]*portfoliov1.FeeRecord, error)
}

// PgFeesProjection writes one row per fee-bearing portfolio event
// across the three sources: transaction fees on every fill, periodic
// margin-interest accruals, and periodic short-borrow-fee accruals.
// Lives in its own consumer ("fees-history") so a rebuild or stall
// here doesn't block the other portfolio read paths.
type PgFeesProjection struct {
	pool *pgxpool.Pool
}

func NewPgFeesProjection(pool *pgxpool.Pool) *PgFeesProjection {
	return &PgFeesProjection{pool: pool}
}

func (p *PgFeesProjection) Reset(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `TRUNCATE projection_fees`); err != nil {
		return fmt.Errorf("truncate projection_fees: %w", err)
	}
	return nil
}

func (p *PgFeesProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.TransactionFeeCharged:
			batch.Queue(
				`INSERT INTO projection_fees
				 (account_id, kind, amount, symbol, charged_at, related_id, notional)
				 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				data.AccountId,
				int32(portfoliov1.FeeKind_FEE_KIND_TRANSACTION),
				data.Amount, data.Symbol, data.ChargedAt.AsTime(),
				data.TradeId, data.Notional,
			)
		case *portfoliov1.MarginInterestAccrued:
			batch.Queue(
				`INSERT INTO projection_fees
				 (account_id, kind, amount, charged_at, rate_bps, period_start)
				 VALUES ($1, $2, $3, $4, $5, $6)`,
				data.AccountId,
				int32(portfoliov1.FeeKind_FEE_KIND_MARGIN_INTEREST),
				data.Amount, data.PeriodEnd.AsTime(),
				data.RateBps, data.PeriodStart.AsTime(),
			)
		case *portfoliov1.ShortBorrowFeeAccrued:
			batch.Queue(
				`INSERT INTO projection_fees
				 (account_id, kind, amount, symbol, charged_at, rate_bps, period_start)
				 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				data.AccountId,
				int32(portfoliov1.FeeKind_FEE_KIND_SHORT_BORROW),
				data.Amount, data.Symbol, data.PeriodEnd.AsTime(),
				data.RateBps, data.PeriodStart.AsTime(),
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
			return fmt.Errorf("fees projection: %w", err)
		}
	}
	return nil
}

// ListFeeHistory returns up to `limit` fee rows for the account,
// newest-first. limit = 0 disables the cap.
func (p *PgFeesProjection) ListFeeHistory(ctx context.Context, accountID string, limit int32) ([]*portfoliov1.FeeRecord, error) {
	q := `SELECT account_id, kind, amount, symbol, charged_at, related_id, rate_bps, notional, period_start
		FROM projection_fees
		WHERE account_id = $1
		ORDER BY charged_at DESC, id DESC`
	args := []any{accountID}
	if limit > 0 {
		q += " LIMIT $2"
		args = append(args, limit)
	}
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query fees: %w", err)
	}
	defer rows.Close()

	var out []*portfoliov1.FeeRecord
	for rows.Next() {
		var (
			rec         portfoliov1.FeeRecord
			kind        int32
			symbol      *string
			chargedAt   time.Time
			relatedID   *string
			rateBps     *int64
			notional    *int64
			periodStart *time.Time
		)
		if err := rows.Scan(&rec.AccountId, &kind, &rec.Amount,
			&symbol, &chargedAt, &relatedID, &rateBps, &notional,
			&periodStart); err != nil {
			return nil, fmt.Errorf("scan fee row: %w", err)
		}
		rec.Kind = portfoliov1.FeeKind(kind)
		if symbol != nil {
			rec.Symbol = *symbol
		}
		rec.ChargedAt = timestamppb.New(chargedAt)
		if relatedID != nil {
			rec.RelatedId = *relatedID
		}
		if rateBps != nil {
			rec.RateBps = *rateBps
		}
		if notional != nil {
			rec.Notional = *notional
		}
		if periodStart != nil {
			rec.PeriodStart = timestamppb.New(*periodStart)
		}
		out = append(out, &rec)
	}
	return out, rows.Err()
}
