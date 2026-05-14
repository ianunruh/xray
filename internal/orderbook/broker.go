package orderbook

import (
	"context"
	"sync"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

type Broker struct {
	mu    sync.Mutex
	subs  map[uint64]*brokerSub
	next  uint64
	ready bool
}

type brokerSub struct {
	symbol string
	ch     chan []es.Event
}

func NewBroker() *Broker {
	return &Broker{
		subs: make(map[uint64]*brokerSub),
	}
}

func (b *Broker) SetReady() {
	b.mu.Lock()
	b.ready = true
	b.mu.Unlock()
}

func (b *Broker) HandleEvents(_ context.Context, events []es.Event) error {
	b.mu.Lock()
	if !b.ready {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	symbols := make(map[string]bool)
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.OrderPlaced:
			symbols[data.Symbol] = true
		case *orderbookv1.TradeExecuted:
			symbols[data.Symbol] = true
		case *orderbookv1.OrderCancelled:
			symbols[data.Symbol] = true
		case *orderbookv1.StopTriggered:
			symbols[data.Symbol] = true
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sub := range b.subs {
		if symbols[sub.symbol] {
			select {
			case sub.ch <- events:
			default:
			}
		}
	}
	return nil
}

func (b *Broker) Subscribe(symbol string) (uint64, <-chan []es.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.next
	b.next++

	ch := make(chan []es.Event, 16)
	b.subs[id] = &brokerSub{symbol: symbol, ch: ch}
	return id, ch
}

func (b *Broker) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if sub, ok := b.subs[id]; ok {
		close(sub.ch)
		delete(b.subs, id)
	}
}
