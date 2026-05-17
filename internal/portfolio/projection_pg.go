package portfolio

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

type PgPortfolioProjection struct {
	pool *pgxpool.Pool
}

func NewPgPortfolioProjection(pool *pgxpool.Pool) *PgPortfolioProjection {
	return &PgPortfolioProjection{pool: pool}
}

func (p *PgPortfolioProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.CashDeposited:
			batch.Queue(
				`INSERT INTO projection_portfolios (account_id, cash_balance)
				VALUES ($1, $2)
				ON CONFLICT (account_id) DO UPDATE SET cash_balance = projection_portfolios.cash_balance + $2`,
				data.AccountId, data.Amount,
			)
		case *portfoliov1.CashWithdrawn:
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance - $1 WHERE account_id = $2`,
				data.Amount, data.AccountId,
			)
		case *portfoliov1.CashHeld:
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance - $1, cash_held = cash_held + $1 WHERE account_id = $2`,
				data.Amount, data.AccountId,
			)
		case *portfoliov1.CashReleased:
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance + $1, cash_held = cash_held - $1 WHERE account_id = $2`,
				data.Amount, data.AccountId,
			)
		case *portfoliov1.CashSettled:
			batch.Queue(
				`UPDATE projection_portfolios SET cash_held = cash_held - $1 WHERE account_id = $2`,
				data.Amount, data.AccountId,
			)
			batch.Queue(
				`INSERT INTO projection_holdings (account_id, symbol, quantity, total_cost)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (account_id, symbol) DO UPDATE SET
					quantity = projection_holdings.quantity + $3,
					total_cost = projection_holdings.total_cost + $4`,
				data.AccountId, data.Symbol, data.Quantity, data.CostPerShare*data.Quantity,
			)
		case *portfoliov1.SharesCredited:
			batch.Queue(
				`INSERT INTO projection_holdings (account_id, symbol, quantity, total_cost)
				VALUES ($1, $2, $3, $4)
				ON CONFLICT (account_id, symbol) DO UPDATE SET
					quantity = projection_holdings.quantity + $3,
					total_cost = projection_holdings.total_cost + $4`,
				data.AccountId, data.Symbol, data.Quantity, data.CostPerShare*data.Quantity,
			)
		case *portfoliov1.SharesDebited:
			batch.Queue(
				`UPDATE projection_holdings
				SET total_cost = CASE WHEN quantity > 0 THEN total_cost * (quantity - $1) / quantity ELSE 0 END,
					quantity = quantity - $1
				WHERE account_id = $2 AND symbol = $3`,
				data.Quantity, data.AccountId, data.Symbol,
			)
		case *portfoliov1.SharesHeld:
			batch.Queue(
				`UPDATE projection_holdings SET shares_held = shares_held + $1 WHERE account_id = $2 AND symbol = $3`,
				data.Quantity, data.AccountId, data.Symbol,
			)
		case *portfoliov1.SharesReleased:
			batch.Queue(
				`UPDATE projection_holdings SET shares_held = shares_held - $1 WHERE account_id = $2 AND symbol = $3`,
				data.Quantity, data.AccountId, data.Symbol,
			)
		case *portfoliov1.SharesSettled:
			batch.Queue(
				`UPDATE projection_holdings
				SET total_cost = CASE WHEN quantity > 0 THEN total_cost * (quantity - $1) / quantity ELSE 0 END,
					quantity = quantity - $1,
					shares_held = shares_held - $1
				WHERE account_id = $2 AND symbol = $3`,
				data.Quantity, data.AccountId, data.Symbol,
			)
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance + $1 WHERE account_id = $2`,
				data.Proceeds, data.AccountId,
			)
		case *portfoliov1.OrderSagaStarted:
			batch.Queue(
				`INSERT INTO projection_pending_orders (saga_id, account_id, symbol, side, price, quantity, order_type, time_in_force, status, started_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT DO NOTHING`,
				data.SagaId, data.AccountId, data.Symbol,
				int32(data.Side), data.Price, data.Quantity,
				int32(data.OrderType), int32(data.TimeInForce),
				int32(portfoliov1.OrderStatus_ORDER_STATUS_STARTED),
				data.StartedAt.AsTime(),
			)
		case *portfoliov1.OrderSagaCashHeld:
			batch.Queue(
				`UPDATE projection_pending_orders SET status = $1 WHERE saga_id = $2`,
				int32(portfoliov1.OrderStatus_ORDER_STATUS_CASH_HELD), data.SagaId,
			)
		case *portfoliov1.OrderSagaOrderPlaced:
			batch.Queue(
				`UPDATE projection_pending_orders SET status = $1 WHERE saga_id = $2`,
				int32(portfoliov1.OrderStatus_ORDER_STATUS_ORDER_PLACED), data.SagaId,
			)
		case *portfoliov1.OrderSagaFillRecorded:
			batch.Queue(
				`UPDATE projection_pending_orders SET filled_qty = filled_qty + $1 WHERE saga_id = $2`,
				data.FillQuantity, data.SagaId,
			)
		case *portfoliov1.OrderSagaCompleted:
			batch.Queue(
				`UPDATE projection_pending_orders SET status = $1, ended_at = $2 WHERE saga_id = $3`,
				int32(portfoliov1.OrderStatus_ORDER_STATUS_COMPLETED), data.CompletedAt.AsTime(), data.SagaId,
			)
		case *portfoliov1.OrderSagaFailed:
			batch.Queue(
				`UPDATE projection_pending_orders SET status = $1, reason = $2, ended_at = $3 WHERE saga_id = $4`,
				int32(portfoliov1.OrderStatus_ORDER_STATUS_FAILED), data.Reason, data.FailedAt.AsTime(), data.SagaId,
			)

		// Short-selling cash mutations. The aggregate is authoritative;
		// we mirror its cash_balance arithmetic so the streamed view
		// matches GetMarginSnapshot's cash_balance / buying_power.
		case *portfoliov1.CollateralHeld:
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance - $1 WHERE account_id = $2`,
				data.Amount, data.AccountId,
			)
		case *portfoliov1.CollateralReleased:
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance + $1 WHERE account_id = $2`,
				data.Amount, data.AccountId,
			)
		case *portfoliov1.ShortOpened:
			// Aggregate consumes the pre-fill collateral hold into the
			// short's locked pool — cash-neutral when the hold matched
			// the execution. Any overflow (executed collateral above
			// the pre-held estimate) is taken straight from cash.
			// Without the prior CollateralHeld amount in scope, treat
			// the common case (overflow = 0) as cash-neutral. The rare
			// overflow path will show up later as a margin-snapshot
			// vs cash_balance discrepancy.
		case *portfoliov1.ShortCovered:
			// Net cash impact: pools (proceeds_released +
			// collateral_released) return to cash; cost is paid.
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance + $1 WHERE account_id = $2`,
				data.ProceedsReleased+data.CollateralReleased-data.Cost, data.AccountId,
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
			return fmt.Errorf("portfolio projection: %w", err)
		}
	}

	return nil
}

func (p *PgPortfolioProjection) ListPortfolios(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT account_id FROM projection_portfolios ORDER BY account_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("query portfolios: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan portfolio: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (p *PgPortfolioProjection) GetPortfolio(ctx context.Context, accountID string) (*portfoliov1.GetPortfolioResponse, error) {
	resp := &portfoliov1.GetPortfolioResponse{
		AccountId: accountID,
	}

	err := p.pool.QueryRow(ctx,
		`SELECT cash_balance, cash_held FROM projection_portfolios WHERE account_id = $1`,
		accountID,
	).Scan(&resp.CashBalance, &resp.CashHeld)
	if err == pgx.ErrNoRows {
		return resp, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query portfolio: %w", err)
	}

	rows, err := p.pool.Query(ctx,
		`SELECT h.symbol, h.quantity, h.total_cost, h.shares_held, COALESCE(p.realized_pnl, 0)
		FROM projection_holdings h
		LEFT JOIN projection_pnl_positions p ON h.account_id = p.account_id AND h.symbol = p.symbol
		WHERE h.account_id = $1 AND h.quantity > 0`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("query holdings: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		h := &portfoliov1.Holding{}
		if err := rows.Scan(&h.Symbol, &h.Quantity, &h.TotalCost, &h.SharesHeld, &h.RealizedPnl); err != nil {
			return nil, fmt.Errorf("scan holding: %w", err)
		}
		if h.Quantity > 0 {
			h.AverageCost = h.TotalCost / h.Quantity
		}
		resp.Holdings = append(resp.Holdings, h)
	}

	terminalCutoff := time.Now().Add(-5 * time.Minute)
	orderRows, err := p.pool.Query(ctx,
		`SELECT saga_id, symbol, side, price, quantity, order_type, time_in_force, filled_qty, status, started_at, reason, ended_at
		FROM projection_pending_orders
		WHERE account_id = $1 AND (status < $2 OR ended_at > $3)
		ORDER BY started_at DESC`,
		accountID, int32(portfoliov1.OrderStatus_ORDER_STATUS_COMPLETED), terminalCutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("query pending orders: %w", err)
	}
	defer orderRows.Close()

	for orderRows.Next() {
		var (
			o           portfoliov1.PendingOrder
			side        int32
			orderType   int32
			timeInForce int32
			status      int32
			startedAt   time.Time
			reason      string
			endedAt     *time.Time
		)
		if err := orderRows.Scan(
			&o.SagaId, &o.Symbol, &side, &o.Price, &o.Quantity,
			&orderType, &timeInForce, &o.FilledQuantity, &status, &startedAt, &reason, &endedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending order: %w", err)
		}
		o.Side = orderbookv1.Side(side)
		o.OrderType = orderbookv1.OrderType(orderType)
		o.TimeInForce = orderbookv1.TimeInForce(timeInForce)
		o.Status = portfoliov1.OrderStatus(status)
		o.StartedAt = timestamppb.New(startedAt)
		o.FailReason = reason
		if endedAt != nil {
			o.EndedAt = timestamppb.New(*endedAt)
		}
		resp.PendingOrders = append(resp.PendingOrders, &o)
	}

	return resp, nil
}
