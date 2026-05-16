package orderbook

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// PgOrderProjection maintains order status in Postgres, updated incrementally
// from order lifecycle events.
type PgOrderProjection struct {
	pool *pgxpool.Pool
}

// NewPgOrderProjection creates a Postgres-backed order projection.
func NewPgOrderProjection(pool *pgxpool.Pool) *PgOrderProjection {
	return &PgOrderProjection{pool: pool}
}

func (p *PgOrderProjection) HandleEvents(ctx context.Context, events []es.Event) error {
	batch := &pgx.Batch{}

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.OrderPlaced:
			batch.Queue(
				`INSERT INTO projection_orders (symbol, order_id, side, price, stop_price, quantity, remaining_quantity, status, placed_at, order_type, time_in_force)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
				ON CONFLICT DO NOTHING`,
				data.Symbol, data.OrderId, int32(data.Side), data.Price, data.StopPrice, data.Quantity, data.Quantity,
				int32(orderbookv1.OrderStatus_ORDER_STATUS_OPEN), data.PlacedAt.AsTime(),
				int32(data.OrderType), int32(data.TimeInForce),
			)
		case *orderbookv1.TradeExecuted:
			for _, orderID := range []string{data.BuyOrderId, data.SellOrderId} {
				batch.Queue(
					`UPDATE projection_orders
					SET remaining_quantity = remaining_quantity - $1,
						status = CASE WHEN remaining_quantity - $1 <= 0 THEN $2::INT ELSE $3::INT END
					WHERE symbol = $4 AND order_id = $5 AND status != $6::INT`,
					data.Quantity,
					int32(orderbookv1.OrderStatus_ORDER_STATUS_FILLED),
					int32(orderbookv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED),
					data.Symbol, orderID,
					int32(orderbookv1.OrderStatus_ORDER_STATUS_FILLED),
				)
			}
		case *orderbookv1.OrderCancelled:
			batch.Queue(
				`UPDATE projection_orders SET status = $1::INT WHERE symbol = $2 AND order_id = $3`,
				int32(orderbookv1.OrderStatus_ORDER_STATUS_CANCELLED), data.Symbol, data.OrderId,
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
			return fmt.Errorf("order projection: %w", err)
		}
	}

	return nil
}

func (p *PgOrderProjection) GetOrder(symbol, orderID string) (*orderbookv1.OrderSummary, bool) {
	row := p.pool.QueryRow(context.Background(),
		`SELECT order_id, symbol, side, price, stop_price, quantity, remaining_quantity, status, placed_at, order_type, time_in_force
		FROM projection_orders WHERE symbol = $1 AND order_id = $2`,
		symbol, orderID,
	)

	summary, err := scanOrderSummary(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	return summary, true
}

func (p *PgOrderProjection) ListOrders(symbol string) []*orderbookv1.OrderSummary {
	rows, err := p.pool.Query(context.Background(),
		`SELECT order_id, symbol, side, price, stop_price, quantity, remaining_quantity, status, placed_at, order_type, time_in_force
		FROM projection_orders WHERE symbol = $1`,
		symbol,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var out []*orderbookv1.OrderSummary
	for rows.Next() {
		summary, err := scanOrderSummary(rows)
		if err != nil {
			return nil
		}
		out = append(out, summary)
	}
	return out
}

func (p *PgOrderProjection) ListSymbols() []string {
	rows, err := p.pool.Query(context.Background(),
		`SELECT DISTINCT symbol FROM projection_orders ORDER BY symbol`,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil
		}
		out = append(out, s)
	}
	return out
}

type orderScannable interface {
	Scan(dest ...any) error
}

func scanOrderSummary(s orderScannable) (*orderbookv1.OrderSummary, error) {
	var (
		o           orderbookv1.OrderSummary
		side        int32
		status      int32
		placedAt    time.Time
		orderType   int32
		timeInForce int32
	)

	if err := s.Scan(
		&o.OrderId, &o.Symbol, &side, &o.Price, &o.StopPrice, &o.Quantity, &o.RemainingQuantity,
		&status, &placedAt, &orderType, &timeInForce,
	); err != nil {
		return nil, err
	}

	o.Side = orderbookv1.Side(side)
	o.Status = orderbookv1.OrderStatus(status)
	o.PlacedAt = timestamppb.New(placedAt)
	o.OrderType = orderbookv1.OrderType(orderType)
	o.TimeInForce = orderbookv1.TimeInForce(timeInForce)

	return &o, nil
}
