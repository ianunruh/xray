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

type PnLReader interface {
	GetPnL(ctx context.Context, accountID string) (*portfoliov1.GetPnLResponse, error)
}

type PgPnLProjection struct {
	pool *pgxpool.Pool
}

func NewPgPnLProjection(pool *pgxpool.Pool) *PgPnLProjection {
	return &PgPnLProjection{pool: pool}
}

func (p *PgPnLProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.CashSettled:
			p.handleBuy(batch, data.AccountId, data.Symbol, data.Quantity, data.CostPerShare, data.SettledAt.AsTime())
		case *portfoliov1.SharesCredited:
			p.handleBuy(batch, data.AccountId, data.Symbol, data.Quantity, data.CostPerShare, data.CreditedAt.AsTime())
		case *portfoliov1.SharesSettled:
			p.handleSell(batch, data.AccountId, data.Symbol, data.Quantity, data.PricePerShare, data.Proceeds, data.SettledAt.AsTime())
		}
	}

	if batch.Len() == 0 {
		return nil
	}

	br := p.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range batch.Len() {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("pnl projection: %w", err)
		}
	}

	return nil
}

func (p *PgPnLProjection) handleBuy(batch *pgx.Batch, accountID, symbol string, quantity, costPerShare int64, settledAt time.Time) {
	batch.Queue(
		`INSERT INTO projection_pnl_positions (account_id, symbol, quantity, total_cost)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (account_id, symbol) DO UPDATE SET
			quantity = projection_pnl_positions.quantity + $3,
			total_cost = projection_pnl_positions.total_cost + $4`,
		accountID, symbol, quantity, costPerShare*quantity,
	)
	batch.Queue(
		`INSERT INTO projection_pnl (account_id, symbol, side, quantity, price, realized_pnl, settled_at)
		VALUES ($1, $2, $3, $4, $5, 0, $6)`,
		accountID, symbol, int32(orderbookv1.Side_SIDE_BUY), quantity, costPerShare, settledAt,
	)
}

func (p *PgPnLProjection) handleSell(batch *pgx.Batch, accountID, symbol string, quantity, pricePerShare, proceeds int64, settledAt time.Time) {
	// Insert P&L entry with realized_pnl computed from current position's avg cost.
	// proceeds - (total_cost / quantity * sold_quantity) using integer math:
	// proceeds - (total_cost * sold_quantity / quantity)
	batch.Queue(
		`INSERT INTO projection_pnl (account_id, symbol, side, quantity, price, realized_pnl, settled_at)
		SELECT $1, $2, $3::INT, $4::BIGINT, $5::BIGINT,
			$6::BIGINT - CASE WHEN pp.quantity > 0 THEN pp.total_cost * $4::BIGINT / pp.quantity ELSE 0 END,
			$7
		FROM projection_pnl_positions pp
		WHERE pp.account_id = $1 AND pp.symbol = $2`,
		accountID, symbol, int32(orderbookv1.Side_SIDE_SELL), quantity, pricePerShare, proceeds, settledAt,
	)

	// Update position: reduce quantity and total_cost proportionally, accumulate realized_pnl.
	batch.Queue(
		`UPDATE projection_pnl_positions
		SET realized_pnl = realized_pnl + ($4::BIGINT - CASE WHEN quantity > 0 THEN total_cost * $3::BIGINT / quantity ELSE 0 END),
			total_cost = CASE WHEN quantity > 0 THEN total_cost * (quantity - $3::BIGINT) / quantity ELSE 0 END,
			quantity = quantity - $3::BIGINT
		WHERE account_id = $1 AND symbol = $2`,
		accountID, symbol, quantity, proceeds,
	)
}

func (p *PgPnLProjection) GetPnL(ctx context.Context, accountID string) (*portfoliov1.GetPnLResponse, error) {
	resp := &portfoliov1.GetPnLResponse{
		AccountId: accountID,
	}

	rows, err := p.pool.Query(ctx,
		`SELECT symbol, quantity, total_cost, realized_pnl FROM projection_pnl_positions WHERE account_id = $1`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("query pnl positions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		pos := &portfoliov1.PositionPnL{}
		if err := rows.Scan(&pos.Symbol, &pos.Quantity, &pos.TotalCost, &pos.RealizedPnl); err != nil {
			return nil, fmt.Errorf("scan pnl position: %w", err)
		}
		if pos.Quantity > 0 {
			pos.AvgCost = pos.TotalCost / pos.Quantity
		}
		resp.TotalRealizedPnl += pos.RealizedPnl
		resp.Positions = append(resp.Positions, pos)
	}

	historyRows, err := p.pool.Query(ctx,
		`SELECT symbol, side, quantity, price, realized_pnl, settled_at
		FROM projection_pnl WHERE account_id = $1 ORDER BY settled_at`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("query pnl history: %w", err)
	}
	defer historyRows.Close()

	for historyRows.Next() {
		var (
			entry     portfoliov1.PnLEntry
			side      int32
			settledAt time.Time
		)
		if err := historyRows.Scan(&entry.Symbol, &side, &entry.Quantity, &entry.Price, &entry.RealizedPnl, &settledAt); err != nil {
			return nil, fmt.Errorf("scan pnl entry: %w", err)
		}
		entry.Side = orderbookv1.Side(side)
		entry.SettledAt = timestamppb.New(settledAt)
		resp.History = append(resp.History, &entry)
	}

	return resp, nil
}
