package portfolio

import (
	"fmt"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "portfolio"

func AggregateID(accountID string) string {
	return AggregateType + ":" + accountID
}

type Holding struct {
	Quantity  int64
	TotalCost int64
}

type ShareHold struct {
	Symbol   string
	Quantity int64
}

type Portfolio struct {
	es.AggregateBase

	AccountID        string
	CashBalance      int64
	CashHeld         int64
	Holdings         map[string]*Holding
	HoldsBySaga      map[string]int64
	SharesHeld       map[string]int64
	ShareHoldsBySaga map[string]*ShareHold
}

func NewPortfolio(id string) *Portfolio {
	p := &Portfolio{
		Holdings:         make(map[string]*Holding),
		HoldsBySaga:      make(map[string]int64),
		SharesHeld:       make(map[string]int64),
		ShareHoldsBySaga: make(map[string]*ShareHold),
	}
	p.SetID(id)
	return p
}

func (p *Portfolio) Apply(evt es.Event) error {
	switch data := evt.Data.(type) {
	case *portfoliov1.CashDeposited:
		p.applyCashDeposited(data)
	case *portfoliov1.CashWithdrawn:
		p.applyCashWithdrawn(data)
	case *portfoliov1.CashHeld:
		p.applyCashHeld(data)
	case *portfoliov1.CashReleased:
		p.applyCashReleased(data)
	case *portfoliov1.CashSettled:
		p.applyCashSettled(data)
	case *portfoliov1.SharesCredited:
		p.applySharesCredited(data)
	case *portfoliov1.SharesDebited:
		p.applySharesDebited(data)
	case *portfoliov1.SharesHeld:
		p.applySharesHeld(data)
	case *portfoliov1.SharesReleased:
		p.applySharesReleased(data)
	case *portfoliov1.SharesSettled:
		p.applySharesSettled(data)
	default:
		return fmt.Errorf("unknown event type: %T", evt.Data)
	}
	p.IncrementVersion()
	return nil
}

func (p *Portfolio) applyCashDeposited(data *portfoliov1.CashDeposited) {
	p.AccountID = data.AccountId
	p.CashBalance += data.Amount
}

func (p *Portfolio) applyCashWithdrawn(data *portfoliov1.CashWithdrawn) {
	p.CashBalance -= data.Amount
}

func (p *Portfolio) applyCashHeld(data *portfoliov1.CashHeld) {
	p.CashBalance -= data.Amount
	p.CashHeld += data.Amount
	p.HoldsBySaga[data.OrderSagaId] = data.Amount
}

func (p *Portfolio) applyCashReleased(data *portfoliov1.CashReleased) {
	p.CashHeld -= data.Amount
	p.CashBalance += data.Amount
	delete(p.HoldsBySaga, data.OrderSagaId)
}

func (p *Portfolio) applyCashSettled(data *portfoliov1.CashSettled) {
	p.CashHeld -= data.Amount
	p.HoldsBySaga[data.OrderSagaId] -= data.Amount
	if p.HoldsBySaga[data.OrderSagaId] <= 0 {
		delete(p.HoldsBySaga, data.OrderSagaId)
	}

	h, ok := p.Holdings[data.Symbol]
	if !ok {
		h = &Holding{}
		p.Holdings[data.Symbol] = h
	}
	h.Quantity += data.Quantity
	h.TotalCost += data.CostPerShare * data.Quantity
}

func (p *Portfolio) applySharesCredited(data *portfoliov1.SharesCredited) {
	p.AccountID = data.AccountId
	h, ok := p.Holdings[data.Symbol]
	if !ok {
		h = &Holding{}
		p.Holdings[data.Symbol] = h
	}
	h.Quantity += data.Quantity
	h.TotalCost += data.CostPerShare * data.Quantity
}

func (p *Portfolio) applySharesDebited(data *portfoliov1.SharesDebited) {
	h, ok := p.Holdings[data.Symbol]
	if !ok {
		return
	}
	if h.Quantity > 0 {
		h.TotalCost = h.TotalCost * (h.Quantity - data.Quantity) / h.Quantity
	}
	h.Quantity -= data.Quantity
	if h.Quantity <= 0 {
		delete(p.Holdings, data.Symbol)
	}
}

func (p *Portfolio) applySharesHeld(data *portfoliov1.SharesHeld) {
	p.SharesHeld[data.Symbol] += data.Quantity
	p.ShareHoldsBySaga[data.OrderSagaId] = &ShareHold{
		Symbol:   data.Symbol,
		Quantity: data.Quantity,
	}
}

func (p *Portfolio) applySharesReleased(data *portfoliov1.SharesReleased) {
	p.SharesHeld[data.Symbol] -= data.Quantity
	if p.SharesHeld[data.Symbol] <= 0 {
		delete(p.SharesHeld, data.Symbol)
	}
	delete(p.ShareHoldsBySaga, data.OrderSagaId)
}

func (p *Portfolio) applySharesSettled(data *portfoliov1.SharesSettled) {
	p.SharesHeld[data.Symbol] -= data.Quantity
	if p.SharesHeld[data.Symbol] <= 0 {
		delete(p.SharesHeld, data.Symbol)
	}

	hold := p.ShareHoldsBySaga[data.OrderSagaId]
	if hold != nil {
		hold.Quantity -= data.Quantity
		if hold.Quantity <= 0 {
			delete(p.ShareHoldsBySaga, data.OrderSagaId)
		}
	}

	h, ok := p.Holdings[data.Symbol]
	if ok {
		if h.Quantity > 0 {
			h.TotalCost = h.TotalCost * (h.Quantity - data.Quantity) / h.Quantity
		}
		h.Quantity -= data.Quantity
		if h.Quantity <= 0 {
			delete(p.Holdings, data.Symbol)
		}
	}

	p.CashBalance += data.Proceeds
}
