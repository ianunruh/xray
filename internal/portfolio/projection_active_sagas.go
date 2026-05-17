package portfolio

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// ActiveSaga is the slice of saga state the margincall reactor needs
// to enumerate active user sagas for cancellation. Kept here so the
// margincall package doesn't import portfolio just for the type.
type ActiveSaga struct {
	SagaID    string
	AccountID string
}

// ActiveUserSagasTracker is the interface satisfied by both
// PgActiveUserSagasProjection (prod) and an in-memory test fake.
type ActiveUserSagasTracker interface {
	ActiveSingleOrderSagas(ctx context.Context, accountID string) ([]ActiveSaga, error)
}

// PgActiveUserSagasProjection mirrors the set of currently-active
// single-order sagas (including bracket-entry children) into PG so
// the margincall reactor — which lives in the same consumer as this
// projection — can list them without crossing consumer boundaries.
//
// Race avoided: sagasvc.PgProjection's identical query is updated by
// a *different* consumer, so a saga placed at the moment a margin
// call triggers could slip through that view.
type PgActiveUserSagasProjection struct {
	pool *pgxpool.Pool
}

func NewPgActiveUserSagasProjection(pool *pgxpool.Pool) *PgActiveUserSagasProjection {
	return &PgActiveUserSagasProjection{pool: pool}
}

func (p *PgActiveUserSagasProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.OrderSagaStarted:
			batch.Queue(
				`INSERT INTO projection_active_user_sagas (saga_id, account_id, started_at)
				VALUES ($1, $2, $3)
				ON CONFLICT (saga_id) DO NOTHING`,
				data.SagaId, data.AccountId, data.StartedAt.AsTime(),
			)
		case *portfoliov1.OrderSagaCompleted:
			batch.Queue(
				`DELETE FROM projection_active_user_sagas WHERE saga_id = $1`,
				data.SagaId,
			)
		case *portfoliov1.OrderSagaFailed:
			batch.Queue(
				`DELETE FROM projection_active_user_sagas WHERE saga_id = $1`,
				data.SagaId,
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
			return fmt.Errorf("active user sagas projection: %w", err)
		}
	}
	return nil
}

func (p *PgActiveUserSagasProjection) ActiveSingleOrderSagas(ctx context.Context, accountID string) ([]ActiveSaga, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT saga_id, account_id FROM projection_active_user_sagas
		WHERE account_id = $1`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("query active user sagas: %w", err)
	}
	defer rows.Close()
	var out []ActiveSaga
	for rows.Next() {
		var s ActiveSaga
		if err := rows.Scan(&s.SagaID, &s.AccountID); err != nil {
			return nil, fmt.Errorf("scan saga: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
