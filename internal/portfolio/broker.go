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

// BootstrapSagas pre-populates the saga→account routing map from a
// persisted snapshot so that saga lifecycle events (OrderSagaCashHeld,
// OrderSagaFillRecorded, etc.) for sagas in flight across a restart
// still route to the right subscriber. Without this, the persistent
// consumer resumes from checkpoint with an empty map — the only saga
// events that route would be ones for sagas STARTED after the
// restart, leaving in-flight users with a stale UI until the next
// event with an explicit AccountId field.
func (b *PortfolioBroker) BootstrapSagas(sagas []ActiveSaga) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range sagas {
		b.sagaAccount[s.SagaID] = s.AccountID
	}
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
		case *portfoliov1.CollateralHeld:
			accountIDs[data.AccountId] = true
		case *portfoliov1.CollateralReleased:
			accountIDs[data.AccountId] = true
		case *portfoliov1.ShortOpened:
			accountIDs[data.AccountId] = true
		case *portfoliov1.ShortCoverHeld:
			accountIDs[data.AccountId] = true
		case *portfoliov1.ShortCoverReleased:
			accountIDs[data.AccountId] = true
		case *portfoliov1.ShortCovered:
			accountIDs[data.AccountId] = true
		case *portfoliov1.MarginCallIssued:
			accountIDs[data.AccountId] = true
		case *portfoliov1.MarginCallCovered:
			accountIDs[data.AccountId] = true
		case *portfoliov1.OrderSagaStarted:
			b.sagaAccount[data.SagaId] = data.AccountId
			accountIDs[data.AccountId] = true
		case *portfoliov1.OrderSagaCashHeld:
			if aid, ok := b.sagaAccount[data.SagaId]; ok {
				accountIDs[aid] = true
			}
		case *portfoliov1.OrderSagaCollateralHeld:
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
