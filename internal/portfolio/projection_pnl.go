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

func (p *PgPnLProjection) Reset(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx,
		`TRUNCATE projection_pnl, projection_pnl_positions`,
	); err != nil {
		return fmt.Errorf("truncate pnl tables: %w", err)
	}
	return nil
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
		case *portfoliov1.ShortOpened:
			p.handleShortOpen(batch, data)
		case *portfoliov1.ShortCovered:
			p.handleShortCover(batch, data)
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

const (
	posSideLong  = int32(orderbookv1.PositionSide_POSITION_SIDE_LONG)
	posSideShort = int32(orderbookv1.PositionSide_POSITION_SIDE_SHORT)
)

func (p *PgPnLProjection) handleBuy(batch *pgx.Batch, accountID, symbol string, quantity, costPerShare int64, settledAt time.Time) {
	batch.Queue(
		`INSERT INTO projection_pnl_positions (account_id, symbol, position_side, quantity, total_cost)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (account_id, symbol, position_side) DO UPDATE SET
			quantity = projection_pnl_positions.quantity + $4,
			total_cost = projection_pnl_positions.total_cost + $5`,
		accountID, symbol, posSideLong, quantity, costPerShare*quantity,
	)
	batch.Queue(
		`INSERT INTO projection_pnl (account_id, symbol, side, position_side, quantity, price, realized_pnl, settled_at)
		VALUES ($1, $2, $3, $4, $5, $6, 0, $7)`,
		accountID, symbol, int32(orderbookv1.Side_SIDE_BUY), posSideLong, quantity, costPerShare, settledAt,
	)
}

func (p *PgPnLProjection) handleSell(batch *pgx.Batch, accountID, symbol string, quantity, pricePerShare, proceeds int64, settledAt time.Time) {
	batch.Queue(
		`INSERT INTO projection_pnl (account_id, symbol, side, position_side, quantity, price, realized_pnl, settled_at)
		SELECT $1, $2, $3::INT, $4::INT, $5::BIGINT, $6::BIGINT,
			$7::BIGINT - CASE WHEN pp.quantity > 0 THEN pp.total_cost * $5::BIGINT / pp.quantity ELSE 0 END,
			$8
		FROM projection_pnl_positions pp
		WHERE pp.account_id = $1 AND pp.symbol = $2 AND pp.position_side = $4::INT`,
		accountID, symbol, int32(orderbookv1.Side_SIDE_SELL), posSideLong, quantity, pricePerShare, proceeds, settledAt,
	)

	batch.Queue(
		`UPDATE projection_pnl_positions
		SET realized_pnl = realized_pnl + ($4::BIGINT - CASE WHEN quantity > 0 THEN total_cost * $3::BIGINT / quantity ELSE 0 END),
			total_cost = CASE WHEN quantity > 0 THEN total_cost * (quantity - $3::BIGINT) / quantity ELSE 0 END,
			quantity = quantity - $3::BIGINT
		WHERE account_id = $1 AND symbol = $2 AND position_side = $5::INT`,
		accountID, symbol, quantity, proceeds, posSideLong,
	)
}

// handleShortOpen tracks a sell-to-open. quantity goes into the SHORT
// row's quantity field; total_cost holds the cumulative sale proceeds
// (used for avg-open-price computation on read). Opening doesn't
// realize PnL — realized_pnl row is zero until the cover lands.
func (p *PgPnLProjection) handleShortOpen(batch *pgx.Batch, data *portfoliov1.ShortOpened) {
	at := data.OpenedAt.AsTime()
	batch.Queue(
		`INSERT INTO projection_pnl_positions (account_id, symbol, position_side, quantity, total_cost)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (account_id, symbol, position_side) DO UPDATE SET
			quantity = projection_pnl_positions.quantity + $4,
			total_cost = projection_pnl_positions.total_cost + $5`,
		data.AccountId, data.Symbol, posSideShort,
		data.Quantity, data.PricePerShare*data.Quantity,
	)
	batch.Queue(
		`INSERT INTO projection_pnl (account_id, symbol, side, position_side, quantity, price, realized_pnl, settled_at)
		VALUES ($1, $2, $3, $4, $5, $6, 0, $7)`,
		data.AccountId, data.Symbol, int32(orderbookv1.Side_SIDE_SELL),
		posSideShort, data.Quantity, data.PricePerShare, at,
	)
}

// handleShortCover settles a buy-to-cover. The event carries the
// realized PnL pre-computed by the portfolio aggregate, so we just
// record it.
func (p *PgPnLProjection) handleShortCover(batch *pgx.Batch, data *portfoliov1.ShortCovered) {
	at := data.CoveredAt.AsTime()
	batch.Queue(
		`INSERT INTO projection_pnl (account_id, symbol, side, position_side, quantity, price, realized_pnl, settled_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		data.AccountId, data.Symbol, int32(orderbookv1.Side_SIDE_BUY),
		posSideShort, data.Quantity, data.CostPerShare, data.RealizedPnl, at,
	)
	// Reduce the short position proportionally. total_cost (cumulative
	// open proceeds) is reduced by the same ratio so the implied
	// avg-open-price stays stable.
	batch.Queue(
		`UPDATE projection_pnl_positions
		SET realized_pnl = realized_pnl + $3::BIGINT,
			total_cost = CASE WHEN quantity > 0 THEN total_cost * (quantity - $4::BIGINT) / quantity ELSE 0 END,
			quantity = quantity - $4::BIGINT
		WHERE account_id = $1 AND symbol = $2 AND position_side = $5::INT`,
		data.AccountId, data.Symbol, data.RealizedPnl, data.Quantity, posSideShort,
	)
}

func (p *PgPnLProjection) GetPnL(ctx context.Context, accountID string) (*portfoliov1.GetPnLResponse, error) {
	resp := &portfoliov1.GetPnLResponse{
		AccountId: accountID,
	}

	rows, err := p.pool.Query(ctx,
		`SELECT symbol, position_side, quantity, total_cost, realized_pnl
		FROM projection_pnl_positions WHERE account_id = $1
		ORDER BY symbol, position_side`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("query pnl positions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		pos := &portfoliov1.PositionPnL{}
		var side int32
		if err := rows.Scan(&pos.Symbol, &side, &pos.Quantity, &pos.TotalCost, &pos.RealizedPnl); err != nil {
			return nil, fmt.Errorf("scan pnl position: %w", err)
		}
		pos.PositionSide = orderbookv1.PositionSide(side)
		if pos.Quantity > 0 {
			pos.AvgCost = pos.TotalCost / pos.Quantity
		}
		resp.TotalRealizedPnl += pos.RealizedPnl
		resp.Positions = append(resp.Positions, pos)
	}

	historyRows, err := p.pool.Query(ctx,
		`SELECT symbol, side, position_side, quantity, price, realized_pnl, settled_at
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
			posSide   int32
			settledAt time.Time
		)
		if err := historyRows.Scan(&entry.Symbol, &side, &posSide, &entry.Quantity, &entry.Price, &entry.RealizedPnl, &settledAt); err != nil {
			return nil, fmt.Errorf("scan pnl entry: %w", err)
		}
		entry.Side = orderbookv1.Side(side)
		entry.PositionSide = orderbookv1.PositionSide(posSide)
		entry.SettledAt = timestamppb.New(settledAt)
		resp.History = append(resp.History, &entry)
	}

	return resp, nil
}
