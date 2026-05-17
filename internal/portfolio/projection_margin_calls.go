package portfolio

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// MarginCallsReader is the read interface for the audit projection,
// satisfied by PgMarginCallsProjection.
type MarginCallsReader interface {
	ListMarginCalls(ctx context.Context, accountID string, limit int32) ([]*portfoliov1.MarginCallRecord, error)
}

// PgMarginCallsProjection writes one row per margin call (issued and
// later covered) plus the liquidation sagas it spawned. Used by the
// audit view in the UI; lives in the margin-call consumer alongside
// the reactor so it sees every relevant event without cross-consumer
// lag.
type PgMarginCallsProjection struct {
	pool *pgxpool.Pool
}

func NewPgMarginCallsProjection(pool *pgxpool.Pool) *PgMarginCallsProjection {
	return &PgMarginCallsProjection{pool: pool}
}

func (p *PgMarginCallsProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.MarginCallIssued:
			var graceExpires any
			if data.GraceExpiresAt != nil {
				graceExpires = data.GraceExpiresAt.AsTime()
			}
			batch.Queue(
				`INSERT INTO projection_margin_calls
				(call_id, account_id, trigger_trade_id, trigger_symbol,
				 mark_price, equity_at_issue, maintenance_requirement_at_issue,
				 issued_at, grace_expires_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				ON CONFLICT (call_id) DO NOTHING`,
				data.CallId, data.AccountId, data.TriggerTradeId, data.TriggerSymbol,
				data.MarkPrice, data.EquityAtIssue, data.MaintenanceRequirementAtIssue,
				data.IssuedAt.AsTime(), graceExpires,
			)
		case *portfoliov1.MarginCallCovered:
			batch.Queue(
				`UPDATE projection_margin_calls
				SET covered_at = $1,
				    equity_at_cover = $2,
				    maintenance_requirement_at_cover = $3
				WHERE call_id = $4`,
				data.CoveredAt.AsTime(), data.EquityAtCover,
				data.MaintenanceRequirementAtCover, data.CallId,
			)
		case *portfoliov1.OrderSagaStarted:
			// Link the liquidation saga back to its parent call.
			// cause_event_id is the call_id when the reactor spawned it.
			if data.Initiator != sagav1.Initiator_INITIATOR_MARGIN_CALL {
				continue
			}
			if data.CauseEventId == "" {
				continue
			}
			batch.Queue(
				`UPDATE projection_margin_calls
				SET liquidation_saga_ids = array_append(liquidation_saga_ids, $1)
				WHERE call_id = $2`,
				data.SagaId, data.CauseEventId,
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
			return fmt.Errorf("margin calls projection: %w", err)
		}
	}
	return nil
}

// ListMarginCalls returns the account's margin calls newest-first,
// capped at limit (0 means no cap).
func (p *PgMarginCallsProjection) ListMarginCalls(ctx context.Context, accountID string, limit int32) ([]*portfoliov1.MarginCallRecord, error) {
	q := `SELECT call_id, account_id, trigger_trade_id, trigger_symbol,
		mark_price, equity_at_issue, maintenance_requirement_at_issue,
		issued_at, covered_at, equity_at_cover, maintenance_requirement_at_cover,
		liquidation_saga_ids, grace_expires_at
		FROM projection_margin_calls
		WHERE account_id = $1
		ORDER BY issued_at DESC`
	args := []any{accountID}
	if limit > 0 {
		q += " LIMIT $2"
		args = append(args, limit)
	}
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query margin calls: %w", err)
	}
	defer rows.Close()

	var out []*portfoliov1.MarginCallRecord
	for rows.Next() {
		var (
			rec            portfoliov1.MarginCallRecord
			issuedAt       time.Time
			coveredAt      *time.Time
			equityAtCover  *int64
			maintAtCover   *int64
			liquidationIDs []string
			graceExpiresAt *time.Time
		)
		if err := rows.Scan(&rec.CallId, &rec.AccountId, &rec.TriggerTradeId,
			&rec.TriggerSymbol, &rec.MarkPrice, &rec.EquityAtIssue,
			&rec.MaintenanceRequirementAtIssue, &issuedAt,
			&coveredAt, &equityAtCover, &maintAtCover, &liquidationIDs,
			&graceExpiresAt); err != nil {
			return nil, fmt.Errorf("scan margin call: %w", err)
		}
		rec.IssuedAt = timestamppb.New(issuedAt)
		if coveredAt != nil {
			rec.CoveredAt = timestamppb.New(*coveredAt)
		}
		if equityAtCover != nil {
			rec.EquityAtCover = *equityAtCover
		}
		if maintAtCover != nil {
			rec.MaintenanceRequirementAtCover = *maintAtCover
		}
		if graceExpiresAt != nil {
			rec.GraceExpiresAt = timestamppb.New(*graceExpiresAt)
		}
		rec.LiquidationSagaIds = liquidationIDs
		out = append(out, &rec)
	}
	return out, rows.Err()
}
