package corpaction

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// Reader is the read interface for the corporate-action ledger.
// Satisfied by PgProjection.
type Reader interface {
	// DueActions returns Declared actions whose trigger date has
	// passed as of `before`. Splits + renames trigger on
	// effective_date; dividends trigger on pay_date.
	DueActions(ctx context.Context, before time.Time) ([]ActionRow, error)
	// DueDividendSnapshots returns Declared cash-dividend actions
	// whose record_date has passed and that haven't been
	// snapshotted yet.
	DueDividendSnapshots(ctx context.Context, before time.Time) ([]ActionRow, error)
	// List returns the ledger view filtered by symbol/status.
	List(ctx context.Context, symbol string, status corpactionv1.ActionStatus, limit int32) ([]*corpactionv1.CorporateActionRecord, error)
	// Get returns one record by action_id.
	Get(ctx context.Context, actionID string) (*corpactionv1.CorporateActionRecord, error)
}

// ActionRow is the in-Go shape the reactor consumes (the reactor
// doesn't want a proto). Mirrors the projection columns the reactor
// actually needs to dispatch.
type ActionRow struct {
	ActionID         string
	Symbol           string
	Type             corpactionv1.ActionType
	SplitNumerator   int32
	SplitDenominator int32
	DividendPerShare int64
	NewSymbol        string
	EffectiveDate    time.Time
	RecordDate       time.Time
	PayDate          time.Time
}

// PgProjection writes the corporate-action ledger from the four
// lifecycle events. Lives in its own consumer ("corp-actions") so a
// rebuild here doesn't block other portfolio/orderbook projections.
type PgProjection struct {
	pool *pgxpool.Pool
}

func NewPgProjection(pool *pgxpool.Pool) *PgProjection {
	return &PgProjection{pool: pool}
}

func (p *PgProjection) Reset(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx,
		`TRUNCATE projection_corporate_actions, projection_dividend_record_holders`,
	); err != nil {
		return fmt.Errorf("truncate corp-action tables: %w", err)
	}
	return nil
}

func (p *PgProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *corpactionv1.CorporateActionDeclared:
			batch.Queue(
				`INSERT INTO projection_corporate_actions
				 (action_id, symbol, type, status, split_numerator, split_denominator,
				  dividend_per_share, new_symbol, effective_date, record_date, pay_date,
				  declared_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				 ON CONFLICT (action_id) DO NOTHING`,
				data.ActionId, data.Symbol, int32(data.Type),
				int32(corpactionv1.ActionStatus_ACTION_STATUS_DECLARED),
				nullableInt32(data.SplitNumerator), nullableInt32(data.SplitDenominator),
				nullableInt64(data.DividendPerShare), nullableString(data.NewSymbol),
				asTime(data.EffectiveDate), asTime(data.RecordDate), asTime(data.PayDate),
				data.DeclaredAt.AsTime(),
			)
		case *corpactionv1.CorporateActionApplied:
			batch.Queue(
				`UPDATE projection_corporate_actions
				 SET status = $1, applied_at = $2,
				     holders_count = $3, orders_count = $4, sagas_count = $5
				 WHERE action_id = $6`,
				int32(corpactionv1.ActionStatus_ACTION_STATUS_APPLIED),
				data.AppliedAt.AsTime(),
				data.HoldersCount, data.OrdersCount, data.SagasCount,
				data.ActionId,
			)
		case *corpactionv1.CorporateActionFailed:
			batch.Queue(
				`UPDATE projection_corporate_actions
				 SET status = $1, failed_reason = $2
				 WHERE action_id = $3`,
				int32(corpactionv1.ActionStatus_ACTION_STATUS_FAILED),
				data.Reason, data.ActionId,
			)
		case *corpactionv1.DividendRecordSnapshotted:
			batch.Queue(
				`UPDATE projection_corporate_actions
				 SET dividend_snapshotted = TRUE
				 WHERE action_id = $1`,
				data.ActionId,
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
			return fmt.Errorf("corp-action projection: %w", err)
		}
	}
	return nil
}

func (p *PgProjection) DueActions(ctx context.Context, before time.Time) ([]ActionRow, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT action_id, symbol, type, split_numerator, split_denominator,
		        dividend_per_share, new_symbol, effective_date, record_date, pay_date
		 FROM projection_corporate_actions
		 WHERE status = $1
		   AND (
		     (type IN ($2, $3) AND effective_date <= $4) OR
		     (type = $5 AND pay_date <= $4)
		   )
		 ORDER BY COALESCE(effective_date, pay_date) ASC`,
		int32(corpactionv1.ActionStatus_ACTION_STATUS_DECLARED),
		int32(corpactionv1.ActionType_ACTION_TYPE_SPLIT),
		int32(corpactionv1.ActionType_ACTION_TYPE_SYMBOL_CHANGE),
		before,
		int32(corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND),
	)
	if err != nil {
		return nil, fmt.Errorf("query due actions: %w", err)
	}
	defer rows.Close()
	return scanActionRows(rows)
}

func (p *PgProjection) DueDividendSnapshots(ctx context.Context, before time.Time) ([]ActionRow, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT action_id, symbol, type, split_numerator, split_denominator,
		        dividend_per_share, new_symbol, effective_date, record_date, pay_date
		 FROM projection_corporate_actions
		 WHERE status = $1 AND type = $2 AND dividend_snapshotted = FALSE
		   AND record_date <= $3
		 ORDER BY record_date ASC`,
		int32(corpactionv1.ActionStatus_ACTION_STATUS_DECLARED),
		int32(corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND),
		before,
	)
	if err != nil {
		return nil, fmt.Errorf("query due dividend snapshots: %w", err)
	}
	defer rows.Close()
	return scanActionRows(rows)
}

func (p *PgProjection) List(ctx context.Context, symbol string, status corpactionv1.ActionStatus, limit int32) ([]*corpactionv1.CorporateActionRecord, error) {
	q := `SELECT action_id, symbol, type, status, split_numerator, split_denominator,
	             dividend_per_share, new_symbol, effective_date, record_date, pay_date,
	             declared_at, applied_at, failed_reason, holders_count, orders_count, sagas_count
	      FROM projection_corporate_actions
	      WHERE TRUE`
	args := []any{}
	if symbol != "" {
		args = append(args, symbol)
		q += fmt.Sprintf(" AND symbol = $%d", len(args))
	}
	if status != corpactionv1.ActionStatus_ACTION_STATUS_UNSPECIFIED {
		args = append(args, int32(status))
		q += fmt.Sprintf(" AND status = $%d", len(args))
	}
	q += " ORDER BY declared_at DESC"
	if limit > 0 {
		args = append(args, limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list corp-actions: %w", err)
	}
	defer rows.Close()
	return scanRecordRows(rows)
}

func (p *PgProjection) Get(ctx context.Context, actionID string) (*corpactionv1.CorporateActionRecord, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT action_id, symbol, type, status, split_numerator, split_denominator,
		        dividend_per_share, new_symbol, effective_date, record_date, pay_date,
		        declared_at, applied_at, failed_reason, holders_count, orders_count, sagas_count
		 FROM projection_corporate_actions WHERE action_id = $1`,
		actionID,
	)
	if err != nil {
		return nil, fmt.Errorf("get corp-action: %w", err)
	}
	defer rows.Close()
	out, err := scanRecordRows(rows)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out[0], nil
}

func scanActionRows(rows pgx.Rows) ([]ActionRow, error) {
	var out []ActionRow
	for rows.Next() {
		var (
			row              ActionRow
			typeInt          int32
			numer, denom     *int32
			divPerShare      *int64
			newSymbol        *string
			eff, rec, pay    *time.Time
		)
		if err := rows.Scan(
			&row.ActionID, &row.Symbol, &typeInt,
			&numer, &denom, &divPerShare, &newSymbol,
			&eff, &rec, &pay,
		); err != nil {
			return nil, fmt.Errorf("scan due row: %w", err)
		}
		row.Type = corpactionv1.ActionType(typeInt)
		if numer != nil {
			row.SplitNumerator = *numer
		}
		if denom != nil {
			row.SplitDenominator = *denom
		}
		if divPerShare != nil {
			row.DividendPerShare = *divPerShare
		}
		if newSymbol != nil {
			row.NewSymbol = *newSymbol
		}
		if eff != nil {
			row.EffectiveDate = *eff
		}
		if rec != nil {
			row.RecordDate = *rec
		}
		if pay != nil {
			row.PayDate = *pay
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func scanRecordRows(rows pgx.Rows) ([]*corpactionv1.CorporateActionRecord, error) {
	var out []*corpactionv1.CorporateActionRecord
	for rows.Next() {
		var (
			rec                              corpactionv1.CorporateActionRecord
			typeInt, statusInt               int32
			numer, denom                     *int32
			divPerShare                      *int64
			newSymbol                        *string
			eff, recd, pay                   *time.Time
			declaredAt                       time.Time
			appliedAt                        *time.Time
			failedReason                     *string
			holdersCount, ordersCount, sagas *int32
		)
		if err := rows.Scan(
			&rec.ActionId, &rec.Symbol, &typeInt, &statusInt,
			&numer, &denom, &divPerShare, &newSymbol,
			&eff, &recd, &pay,
			&declaredAt, &appliedAt, &failedReason,
			&holdersCount, &ordersCount, &sagas,
		); err != nil {
			return nil, fmt.Errorf("scan corp-action row: %w", err)
		}
		rec.Type = corpactionv1.ActionType(typeInt)
		rec.Status = corpactionv1.ActionStatus(statusInt)
		if numer != nil {
			rec.SplitNumerator = *numer
		}
		if denom != nil {
			rec.SplitDenominator = *denom
		}
		if divPerShare != nil {
			rec.DividendPerShare = *divPerShare
		}
		if newSymbol != nil {
			rec.NewSymbol = *newSymbol
		}
		if eff != nil {
			rec.EffectiveDate = timestamppb.New(*eff)
		}
		if recd != nil {
			rec.RecordDate = timestamppb.New(*recd)
		}
		if pay != nil {
			rec.PayDate = timestamppb.New(*pay)
		}
		rec.DeclaredAt = timestamppb.New(declaredAt)
		if appliedAt != nil {
			rec.AppliedAt = timestamppb.New(*appliedAt)
		}
		if failedReason != nil {
			rec.FailedReason = *failedReason
		}
		if holdersCount != nil {
			rec.HoldersCount = *holdersCount
		}
		if ordersCount != nil {
			rec.OrdersCount = *ordersCount
		}
		if sagas != nil {
			rec.SagasCount = *sagas
		}
		out = append(out, &rec)
	}
	return out, rows.Err()
}

func nullableInt32(v int32) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullableInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func asTime(ts *timestamppb.Timestamp) any {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	if t.IsZero() {
		return nil
	}
	return t
}
