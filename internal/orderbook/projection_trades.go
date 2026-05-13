package orderbook

import (
	"context"
	"sync"

	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// TradeProjection builds an in-memory trade history from TradeExecuted events.
type TradeProjection struct {
	mu     sync.RWMutex
	trades map[string][]*orderbookv1.Trade // keyed by symbol
}

// NewTradeProjection creates a new TradeProjection.
func NewTradeProjection() *TradeProjection {
	return &TradeProjection{
		trades: make(map[string][]*orderbookv1.Trade),
	}
}

// HandleEvents processes events, extracting TradeExecuted to build the trade history.
func (p *TradeProjection) HandleEvents(_ context.Context, events []es.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, evt := range events {
		data, ok := evt.Data.(*orderbookv1.TradeExecuted)
		if !ok {
			continue
		}

		trade := &orderbookv1.Trade{
			TradeId:     data.TradeId,
			Symbol:      data.Symbol,
			BuyOrderId:  data.BuyOrderId,
			SellOrderId: data.SellOrderId,
			Price:       data.Price,
			Quantity:    data.Quantity,
			ExecutedAt:  timestamppb.New(data.ExecutedAt.AsTime()),
		}

		p.trades[data.Symbol] = append(p.trades[data.Symbol], trade)
	}

	return nil
}

// ListTrades returns all trades for the given symbol.
func (p *TradeProjection) ListTrades(symbol string) []*orderbookv1.Trade {
	p.mu.RLock()
	defer p.mu.RUnlock()

	trades := p.trades[symbol]
	out := make([]*orderbookv1.Trade, len(trades))
	copy(out, trades)
	return out
}
