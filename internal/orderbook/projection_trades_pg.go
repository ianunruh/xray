package orderbook

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// PgTradeProjection maintains trade history in Postgres, updated incrementally
// from TradeExecuted events.
type PgTradeProjection struct {
	pool *pgxpool.Pool
}

// NewPgTradeProjection creates a Postgres-backed trade projection.
func NewPgTradeProjection(pool *pgxpool.Pool) *PgTradeProjection {
	return &PgTradeProjection{pool: pool}
}

func (p *PgTradeProjection) Reset(ctx context.Context) error {
	if _, err := p.pool.Exec(ctx, `TRUNCATE projection_trades`); err != nil {
		return fmt.Errorf("truncate projection_trades: %w", err)
	}
	return nil
}

func (p *PgTradeProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}

	for _, evt := range events {
		data, ok := evt.Data.(*orderbookv1.TradeExecuted)
		if !ok {
			continue
		}

		batch.Queue(
			`INSERT INTO projection_trades (trade_id, symbol, buy_order_id, sell_order_id, price, quantity, executed_at, cross_type)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT DO NOTHING`,
			data.TradeId, data.Symbol, data.BuyOrderId, data.SellOrderId,
			data.Price, data.Quantity, data.ExecutedAt.AsTime(), int32(data.CrossType),
		)
	}

	if batch.Len() == 0 {
		return nil
	}

	br := p.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range batch.Len() {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("trade projection: %w", err)
		}
	}

	return nil
}

// TradesByOrderID returns all trades that matched against the given
// order ID (as either buy or sell side), ordered by execution time.
// Used by the reconciler to detect trades whose settle commands never
// landed.
func (p *PgTradeProjection) TradesByOrderID(ctx context.Context, orderID string) ([]*orderbookv1.TradeExecuted, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT trade_id, symbol, buy_order_id, sell_order_id, price, quantity, executed_at, cross_type
		FROM projection_trades WHERE buy_order_id = $1 OR sell_order_id = $1
		ORDER BY executed_at`,
		orderID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*orderbookv1.TradeExecuted
	for rows.Next() {
		var (
			t          orderbookv1.TradeExecuted
			executedAt time.Time
			crossType  int32
		)
		if err := rows.Scan(
			&t.TradeId, &t.Symbol, &t.BuyOrderId, &t.SellOrderId,
			&t.Price, &t.Quantity, &executedAt, &crossType,
		); err != nil {
			return nil, err
		}
		t.ExecutedAt = timestamppb.New(executedAt)
		t.CrossType = orderbookv1.CrossType(crossType)
		out = append(out, &t)
	}
	return out, rows.Err()
}

func (p *PgTradeProjection) ListTrades(symbol string) []*orderbookv1.Trade {
	rows, err := p.pool.Query(context.Background(),
		`SELECT trade_id, symbol, buy_order_id, sell_order_id, price, quantity, executed_at, cross_type
		FROM projection_trades WHERE symbol = $1 ORDER BY executed_at`,
		symbol,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var out []*orderbookv1.Trade
	for rows.Next() {
		var (
			t          orderbookv1.Trade
			executedAt time.Time
			crossType  int32
		)

		if err := rows.Scan(
			&t.TradeId, &t.Symbol, &t.BuyOrderId, &t.SellOrderId,
			&t.Price, &t.Quantity, &executedAt, &crossType,
		); err != nil {
			return nil
		}

		t.ExecutedAt = timestamppb.New(executedAt)
		t.CrossType = orderbookv1.CrossType(crossType)
		out = append(out, &t)
	}
	return out
}
