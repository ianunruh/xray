package sagasvc

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// PgProjection maintains one row per saga across all kinds, keyed by saga_id.
// Powers SagaService.Get / List queries and Cancel's kind-dispatch lookup.
type PgProjection struct {
	pool *pgxpool.Pool
}

func NewPgProjection(pool *pgxpool.Pool) *PgProjection {
	return &PgProjection{pool: pool}
}

func (p *PgProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}

	for _, evt := range events {
		switch data := evt.Data.(type) {
		// Single-order saga lifecycle.
		case *portfoliov1.OrderSagaStarted:
			batch.Queue(
				`INSERT INTO projection_sagas (saga_id, kind, status, account_id, symbol, started_at)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (saga_id) DO NOTHING`,
				data.SagaId,
				int32(sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER),
				int32(sagav1.SagaStatus_SAGA_STATUS_ACTIVE),
				data.AccountId, data.Symbol,
				data.StartedAt.AsTime(),
			)
		case *portfoliov1.OrderSagaCompleted:
			batch.Queue(
				`UPDATE projection_sagas SET status = $1, ended_at = $2 WHERE saga_id = $3`,
				int32(sagav1.SagaStatus_SAGA_STATUS_COMPLETED),
				data.CompletedAt.AsTime(), data.SagaId,
			)
		case *portfoliov1.OrderSagaFailed:
			batch.Queue(
				`UPDATE projection_sagas SET status = $1, fail_reason = $2, ended_at = $3 WHERE saga_id = $4`,
				int32(sagav1.SagaStatus_SAGA_STATUS_FAILED),
				data.Reason, data.FailedAt.AsTime(), data.SagaId,
			)

		// Bracket saga lifecycle. SagaStarted lives in the orderbook
		// proto package since it predates the saga split.
		case *orderbookv1.SagaStarted:
			batch.Queue(
				`INSERT INTO projection_sagas (saga_id, kind, status, account_id, symbol, started_at)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (saga_id) DO NOTHING`,
				data.SagaId,
				int32(sagav1.SagaKind_SAGA_KIND_BRACKET),
				int32(sagav1.SagaStatus_SAGA_STATUS_ACTIVE),
				data.AccountId, data.Symbol,
				data.StartedAt.AsTime(),
			)
		case *orderbookv1.SagaCompleted:
			batch.Queue(
				`UPDATE projection_sagas SET status = $1, ended_at = $2 WHERE saga_id = $3`,
				int32(sagav1.SagaStatus_SAGA_STATUS_COMPLETED),
				data.CompletedAt.AsTime(), data.SagaId,
			)
		case *orderbookv1.SagaFailed:
			batch.Queue(
				`UPDATE projection_sagas SET status = $1, fail_reason = $2, ended_at = $3 WHERE saga_id = $4`,
				int32(sagav1.SagaStatus_SAGA_STATUS_FAILED),
				data.Reason, data.FailedAt.AsTime(), data.SagaId,
			)

		// OCO saga lifecycle.
		case *orderbookv1.OCOSagaStarted:
			batch.Queue(
				`INSERT INTO projection_sagas (saga_id, kind, status, account_id, symbol, started_at)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (saga_id) DO NOTHING`,
				data.SagaId,
				int32(sagav1.SagaKind_SAGA_KIND_OCO),
				int32(sagav1.SagaStatus_SAGA_STATUS_ACTIVE),
				data.AccountId, data.Symbol,
				data.StartedAt.AsTime(),
			)
		case *orderbookv1.OCOSagaCompleted:
			batch.Queue(
				`UPDATE projection_sagas SET status = $1, ended_at = $2 WHERE saga_id = $3`,
				int32(sagav1.SagaStatus_SAGA_STATUS_COMPLETED),
				data.CompletedAt.AsTime(), data.SagaId,
			)
		case *orderbookv1.OCOSagaFailed:
			batch.Queue(
				`UPDATE projection_sagas SET status = $1, fail_reason = $2, ended_at = $3 WHERE saga_id = $4`,
				int32(sagav1.SagaStatus_SAGA_STATUS_FAILED),
				data.Reason, data.FailedAt.AsTime(), data.SagaId,
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
			return fmt.Errorf("saga projection: %w", err)
		}
	}
	return nil
}

// SagaRow is the lookup result for kind-dispatch and listing.
type SagaRow struct {
	SagaID     string
	Kind       sagav1.SagaKind
	Status     sagav1.SagaStatus
	AccountID  string
	Symbol     string
	StartedAt  time.Time
	EndedAt    *time.Time
	FailReason string
}

func (p *PgProjection) Get(ctx context.Context, sagaID string) (*SagaRow, error) {
	var (
		row    SagaRow
		kind   int32
		status int32
	)
	err := p.pool.QueryRow(ctx,
		`SELECT saga_id, kind, status, account_id, symbol, started_at, ended_at, fail_reason
		FROM projection_sagas WHERE saga_id = $1`,
		sagaID,
	).Scan(&row.SagaID, &kind, &status, &row.AccountID, &row.Symbol, &row.StartedAt, &row.EndedAt, &row.FailReason)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get saga: %w", err)
	}
	row.Kind = sagav1.SagaKind(kind)
	row.Status = sagav1.SagaStatus(status)
	return &row, nil
}

// List returns sagas matching the given filters. Zero-valued filters are
// treated as "match any."
// childSagaPrefixes hides sagas whose IDs were synthesized by a parent
// saga (entry ordersagas owned by a bracket, exit OCO sagas owned by a
// bracket). These are implementation details — the parent is what
// callers see in List responses.
var childSagaPrefixes = []string{"bracket-entry:", "bracket-oco:"}

func (p *PgProjection) List(ctx context.Context, accountID, symbol string, kind sagav1.SagaKind, status sagav1.SagaStatus) ([]*SagaRow, error) {
	q := `SELECT saga_id, kind, status, account_id, symbol, started_at, ended_at, fail_reason
		FROM projection_sagas WHERE TRUE`
	args := []any{}
	for _, prefix := range childSagaPrefixes {
		args = append(args, prefix+"%")
		q += fmt.Sprintf(" AND saga_id NOT LIKE $%d", len(args))
	}
	if accountID != "" {
		args = append(args, accountID)
		q += fmt.Sprintf(" AND account_id = $%d", len(args))
	}
	if symbol != "" {
		args = append(args, symbol)
		q += fmt.Sprintf(" AND symbol = $%d", len(args))
	}
	if kind != sagav1.SagaKind_SAGA_KIND_UNSPECIFIED {
		args = append(args, int32(kind))
		q += fmt.Sprintf(" AND kind = $%d", len(args))
	}
	if status != sagav1.SagaStatus_SAGA_STATUS_UNSPECIFIED {
		args = append(args, int32(status))
		q += fmt.Sprintf(" AND status = $%d", len(args))
	}
	q += " ORDER BY started_at DESC"

	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list sagas: %w", err)
	}
	defer rows.Close()

	var out []*SagaRow
	for rows.Next() {
		var (
			r      SagaRow
			kind   int32
			status int32
		)
		if err := rows.Scan(&r.SagaID, &kind, &status, &r.AccountID, &r.Symbol, &r.StartedAt, &r.EndedAt, &r.FailReason); err != nil {
			return nil, fmt.Errorf("scan saga: %w", err)
		}
		r.Kind = sagav1.SagaKind(kind)
		r.Status = sagav1.SagaStatus(status)
		out = append(out, &r)
	}
	return out, rows.Err()
}
