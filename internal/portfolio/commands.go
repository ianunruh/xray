package portfolio

import (
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

var (
	ErrInvalidAmount     = errors.New("amount must be positive")
	ErrInsufficientFunds = errors.New("insufficient funds")
)

type DepositCash struct {
	AccountID string
	Amount    int64
}

func (c DepositCash) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteDepositCash(p *Portfolio, cmd DepositCash) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        "CashDeposited",
		Timestamp:   now,
		Data: &portfoliov1.CashDeposited{
			AccountId:   cmd.AccountID,
			Amount:      cmd.Amount,
			DepositedAt: timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type WithdrawCash struct {
	AccountID string
	Amount    int64
}

func (c WithdrawCash) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteWithdrawCash(p *Portfolio, cmd WithdrawCash) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if p.CashBalance < cmd.Amount {
		return nil, ErrInsufficientFunds
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        "CashWithdrawn",
		Timestamp:   now,
		Data: &portfoliov1.CashWithdrawn{
			AccountId:   cmd.AccountID,
			Amount:      cmd.Amount,
			WithdrawnAt: timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type HoldCash struct {
	AccountID   string
	OrderSagaID string
	Amount      int64
}

func (c HoldCash) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteHoldCash(p *Portfolio, cmd HoldCash) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if p.CashBalance < cmd.Amount {
		return nil, ErrInsufficientFunds
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        "CashHeld",
		Timestamp:   now,
		Data: &portfoliov1.CashHeld{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Amount:      cmd.Amount,
			HeldAt:      timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type ReleaseCash struct {
	AccountID   string
	OrderSagaID string
	Amount      int64
}

func (c ReleaseCash) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteReleaseCash(p *Portfolio, cmd ReleaseCash) ([]es.Event, error) {
	if cmd.Amount <= 0 {
		return nil, ErrInvalidAmount
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        "CashReleased",
		Timestamp:   now,
		Data: &portfoliov1.CashReleased{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Amount:      cmd.Amount,
			ReleasedAt:  timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type SettleTrade struct {
	AccountID    string
	OrderSagaID  string
	Amount       int64
	Symbol       string
	Quantity     int64
	CostPerShare int64
}

func (c SettleTrade) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteSettleTrade(p *Portfolio, cmd SettleTrade) ([]es.Event, error) {
	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        "CashSettled",
		Timestamp:   now,
		Data: &portfoliov1.CashSettled{
			AccountId:    cmd.AccountID,
			OrderSagaId:  cmd.OrderSagaID,
			Amount:       cmd.Amount,
			Symbol:       cmd.Symbol,
			Quantity:     cmd.Quantity,
			CostPerShare: cmd.CostPerShare,
			SettledAt:    timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}
