package orderbook

import (
	"context"
	"sync"

	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// OrderProjection builds an in-memory order status view from order lifecycle events.
type OrderProjection struct {
	mu     sync.RWMutex
	orders map[string]map[string]*orderbookv1.OrderSummary // symbol -> orderID -> summary
}

// NewOrderProjection creates a new OrderProjection.
func NewOrderProjection() *OrderProjection {
	return &OrderProjection{
		orders: make(map[string]map[string]*orderbookv1.OrderSummary),
	}
}

// HandleEvents processes events to track order lifecycle.
func (p *OrderProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.OrderPlaced:
			p.applyOrderPlaced(data)
		case *orderbookv1.TradeExecuted:
			p.applyTradeExecuted(data)
		case *orderbookv1.OrderCancelled:
			p.applyOrderCancelled(data)
		case *orderbookv1.IcebergSliceReplenished:
			p.applyIcebergSliceReplenished(data)
		case *orderbookv1.TrailingStopAdjusted:
			p.applyTrailingStopAdjusted(data)
		}
	}

	return nil
}

func (p *OrderProjection) applyOrderPlaced(data *orderbookv1.OrderPlaced) {
	byID := p.orders[data.Symbol]
	if byID == nil {
		byID = make(map[string]*orderbookv1.OrderSummary)
		p.orders[data.Symbol] = byID
	}

	displayed := int64(0)
	if data.DisplayQuantity > 0 {
		displayed = data.DisplayQuantity
		if displayed > data.Quantity {
			displayed = data.Quantity
		}
	}
	byID[data.OrderId] = &orderbookv1.OrderSummary{
		OrderId:            data.OrderId,
		Symbol:             data.Symbol,
		Side:               data.Side,
		Price:              data.Price,
		StopPrice:          data.StopPrice,
		Quantity:           data.Quantity,
		RemainingQuantity:  data.Quantity,
		DisplayQuantity:    data.DisplayQuantity,
		DisplayedRemaining: displayed,
		TrailAmount:        data.TrailAmount,
		TrailOffsetBps:     data.TrailOffsetBps,
		LimitOffset:        data.LimitOffset,
		Status:             orderbookv1.OrderStatus_ORDER_STATUS_OPEN,
		PlacedAt:           timestamppb.New(data.PlacedAt.AsTime()),
		OrderType:          data.OrderType,
		TimeInForce:        data.TimeInForce,
	}
}

func (p *OrderProjection) applyTrailingStopAdjusted(data *orderbookv1.TrailingStopAdjusted) {
	byID := p.orders[data.Symbol]
	if byID == nil {
		return
	}
	if summary := byID[data.OrderId]; summary != nil {
		summary.StopPrice = data.NewStopPrice
	}
}

func (p *OrderProjection) applyIcebergSliceReplenished(data *orderbookv1.IcebergSliceReplenished) {
	byID := p.orders[data.Symbol]
	if byID == nil {
		return
	}
	summary := byID[data.OrderId]
	if summary == nil {
		return
	}
	summary.DisplayedRemaining = data.NewDisplayedQty
	summary.PlacedAt = timestamppb.New(data.ReplenishedAt.AsTime())
}

func (p *OrderProjection) applyTradeExecuted(data *orderbookv1.TradeExecuted) {
	byID := p.orders[data.Symbol]
	if byID == nil {
		return
	}

	for _, orderID := range []string{data.BuyOrderId, data.SellOrderId} {
		summary := byID[orderID]
		if summary == nil {
			continue
		}
		summary.RemainingQuantity -= data.Quantity
		if summary.RemainingQuantity <= 0 {
			summary.Status = orderbookv1.OrderStatus_ORDER_STATUS_FILLED
		} else {
			summary.Status = orderbookv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED
		}
	}
}

func (p *OrderProjection) applyOrderCancelled(data *orderbookv1.OrderCancelled) {
	byID := p.orders[data.Symbol]
	if byID == nil {
		return
	}

	summary := byID[data.OrderId]
	if summary == nil {
		return
	}
	summary.Status = orderbookv1.OrderStatus_ORDER_STATUS_CANCELLED
}

// GetOrder returns a single order by symbol and order ID.
func (p *OrderProjection) GetOrder(symbol, orderID string) (*orderbookv1.OrderSummary, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	summary, ok := p.orders[symbol][orderID]
	return summary, ok
}

// ListSymbols returns all known symbols.
func (p *OrderProjection) ListSymbols() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	out := make([]string, 0, len(p.orders))
	for symbol := range p.orders {
		out = append(out, symbol)
	}
	return out
}

// ListOrders returns all orders for the given symbol.
func (p *OrderProjection) ListOrders(symbol string) []*orderbookv1.OrderSummary {
	p.mu.RLock()
	defer p.mu.RUnlock()

	byID := p.orders[symbol]
	out := make([]*orderbookv1.OrderSummary, 0, len(byID))
	for _, summary := range byID {
		out = append(out, summary)
	}
	return out
}
