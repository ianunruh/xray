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

func (p *PgPortfolioProjection) Reset(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx,
		`TRUNCATE projection_portfolios, projection_holdings, projection_pending_orders`,
	); err != nil {
		return fmt.Errorf("truncate portfolio tables: %w", err)
	}
	return nil
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
				`INSERT INTO projection_pending_orders (saga_id, account_id, symbol, side, price, quantity, display_quantity, order_type, time_in_force, status, started_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
				ON CONFLICT DO NOTHING`,
				data.SagaId, data.AccountId, data.Symbol,
				int32(data.Side), data.Price, data.Quantity, data.DisplayQuantity,
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
				`UPDATE projection_pending_orders SET filled_qty = filled_qty + $1, cash_settled = cash_settled + $2, last_fill_price = $3 WHERE saga_id = $4`,
				data.FillQuantity, data.CashSettled, data.FillPrice, data.SagaId,
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
			// Cash-neutral by construction: the pre-fill collateral
			// was already debited from cash_balance by CollateralHeld;
			// the aggregate just moves it from the per-saga bucket
			// into the short's locked pool (which the streamed view
			// doesn't track). ShortCovered later returns pool money
			// to cash. If policy ever changes to take additional
			// cash at fill time, ShortOpened should grow an explicit
			// overflow field so the projection stays in sync.
		case *portfoliov1.ShortCovered:
			// Net cash impact: pools (proceeds_released +
			// collateral_released) return to cash; cost is paid.
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance + $1 WHERE account_id = $2`,
				data.ProceedsReleased+data.CollateralReleased-data.Cost, data.AccountId,
			)
		case *portfoliov1.TransactionFeeCharged:
			batch.Queue(
				`UPDATE projection_portfolios SET cash_balance = cash_balance - $1 WHERE account_id = $2`,
				data.Amount, data.AccountId,
			)
			batch.Queue(
				`UPDATE projection_pending_orders SET fees_paid = fees_paid + $1 WHERE saga_id = $2`,
				data.Amount, data.OrderSagaId,
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

	// Constrain the JOIN to LONG rows in projection_pnl_positions —
	// holdings is long-inventory only, so picking a SHORT row by
	// accident would surface short-side realized P&L against the
	// wrong holding (or against none at all).
	rows, err := p.pool.Query(ctx,
		`SELECT h.symbol, h.quantity, h.total_cost, h.shares_held, COALESCE(p.realized_pnl, 0)
		FROM projection_holdings h
		LEFT JOIN projection_pnl_positions p ON h.account_id = p.account_id
			AND h.symbol = p.symbol
			AND p.position_side = $2
		WHERE h.account_id = $1 AND h.quantity > 0`,
		accountID, posSideLong,
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

	// Total realized P&L across long AND short positions. Holdings only
	// surfaces long-side per-symbol P&L; this top-level field gives
	// the UI a single number that captures both.
	if err := p.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(realized_pnl), 0)
		FROM projection_pnl_positions WHERE account_id = $1`,
		accountID,
	).Scan(&resp.TotalRealizedPnl); err != nil {
		return nil, fmt.Errorf("query total realized pnl: %w", err)
	}

	terminalCutoff := time.Now().Add(-5 * time.Minute)
	orderRows, err := p.pool.Query(ctx,
		`SELECT saga_id, symbol, side, price, quantity, display_quantity, order_type, time_in_force, filled_qty, status, started_at, reason, ended_at, last_fill_price, fees_paid,
			CASE WHEN filled_qty > 0 THEN cash_settled / filled_qty ELSE 0 END
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
			&o.SagaId, &o.Symbol, &side, &o.Price, &o.Quantity, &o.DisplayQuantity,
			&orderType, &timeInForce, &o.FilledQuantity, &status, &startedAt, &reason, &endedAt,
			&o.LastFillPrice, &o.FeesPaid, &o.VwapFillPrice,
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
