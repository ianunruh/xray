package portfolio

import (
	"context"
	"sync"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

type PortfolioBroker struct {
	mu          sync.Mutex
	subs        map[uint64]*portfolioBrokerSub
	next        uint64
	ready       bool
	sagaAccount map[string]string
}

type portfolioBrokerSub struct {
	accountID string
	ch        chan []es.Event
}

func NewPortfolioBroker() *PortfolioBroker {
	return &PortfolioBroker{
		subs:        make(map[uint64]*portfolioBrokerSub),
		sagaAccount: make(map[string]string),
	}
}

func (b *PortfolioBroker) SetReady() {
	b.mu.Lock()
	b.ready = true
	b.mu.Unlock()
}

func (b *PortfolioBroker) HandleEvents(_ context.Context, events []es.Event) error {
	b.mu.Lock()
	if !b.ready {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	accountIDs := make(map[string]bool)
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *portfoliov1.CashDeposited:
			accountIDs[data.AccountId] = true
		case *portfoliov1.CashWithdrawn:
			accountIDs[data.AccountId] = true
		case *portfoliov1.CashHeld:
			accountIDs[data.AccountId] = true
		case *portfoliov1.CashReleased:
			accountIDs[data.AccountId] = true
		case *portfoliov1.CashSettled:
			accountIDs[data.AccountId] = true
		case *portfoliov1.SharesCredited:
			accountIDs[data.AccountId] = true
		case *portfoliov1.SharesDebited:
			accountIDs[data.AccountId] = true
		case *portfoliov1.SharesHeld:
			accountIDs[data.AccountId] = true
		case *portfoliov1.SharesReleased:
			accountIDs[data.AccountId] = true
		case *portfoliov1.SharesSettled:
			accountIDs[data.AccountId] = true
		case *portfoliov1.OrderSagaStarted:
			b.sagaAccount[data.SagaId] = data.AccountId
			accountIDs[data.AccountId] = true
		case *portfoliov1.OrderSagaCashHeld:
			if aid, ok := b.sagaAccount[data.SagaId]; ok {
				accountIDs[aid] = true
			}
		case *portfoliov1.OrderSagaOrderPlaced:
			if aid, ok := b.sagaAccount[data.SagaId]; ok {
				accountIDs[aid] = true
			}
		case *portfoliov1.OrderSagaFillRecorded:
			if aid, ok := b.sagaAccount[data.SagaId]; ok {
				accountIDs[aid] = true
			}
		case *portfoliov1.OrderSagaCompleted:
			if aid, ok := b.sagaAccount[data.SagaId]; ok {
				accountIDs[aid] = true
				delete(b.sagaAccount, data.SagaId)
			}
		case *portfoliov1.OrderSagaFailed:
			if aid, ok := b.sagaAccount[data.SagaId]; ok {
				accountIDs[aid] = true
				delete(b.sagaAccount, data.SagaId)
			}
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sub := range b.subs {
		if accountIDs[sub.accountID] {
			select {
			case sub.ch <- events:
			default:
			}
		}
	}
	return nil
}

func (b *PortfolioBroker) Subscribe(accountID string) (uint64, <-chan []es.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	id := b.next
	b.next++

	ch := make(chan []es.Event, 16)
	b.subs[id] = &portfolioBrokerSub{accountID: accountID, ch: ch}
	return id, ch
}

func (b *PortfolioBroker) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if sub, ok := b.subs[id]; ok {
		close(sub.ch)
		delete(b.subs, id)
	}
}
