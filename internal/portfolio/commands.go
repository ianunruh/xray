package portfolio

import (
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/pkg/es"
)

var (
	ErrInvalidAmount      = errors.New("amount must be positive")
	ErrInsufficientFunds  = errors.New("insufficient funds")
	ErrInsufficientShares = errors.New("insufficient shares")
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
		Type:        EventCashDeposited,
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
		Type:        EventCashWithdrawn,
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
		Type:        EventCashHeld,
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
	if p.HoldsBySaga[cmd.OrderSagaID] == 0 {
		return nil, nil
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventCashReleased,
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
		Type:        EventCashSettled,
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

type CreditShares struct {
	AccountID    string
	Symbol       string
	Quantity     int64
	CostPerShare int64
}

func (c CreditShares) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteCreditShares(p *Portfolio, cmd CreditShares) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesCredited,
		Timestamp:   now,
		Data: &portfoliov1.SharesCredited{
			AccountId:    cmd.AccountID,
			Symbol:       cmd.Symbol,
			Quantity:     cmd.Quantity,
			CostPerShare: cmd.CostPerShare,
			CreditedAt:   timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type HoldShares struct {
	AccountID   string
	OrderSagaID string
	Symbol      string
	Quantity    int64
}

func (c HoldShares) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteHoldShares(p *Portfolio, cmd HoldShares) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	h := p.Holdings[cmd.Symbol]
	available := int64(0)
	if h != nil {
		available = h.Quantity - p.SharesHeld[cmd.Symbol]
	}
	if available < cmd.Quantity {
		return nil, ErrInsufficientShares
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesHeld,
		Timestamp:   now,
		Data: &portfoliov1.SharesHeld{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Symbol:      cmd.Symbol,
			Quantity:    cmd.Quantity,
			HeldAt:      timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type ReleaseShares struct {
	AccountID   string
	OrderSagaID string
	Symbol      string
	Quantity    int64
}

func (c ReleaseShares) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteReleaseShares(p *Portfolio, cmd ReleaseShares) ([]es.Event, error) {
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if p.ShareHoldsBySaga[cmd.OrderSagaID] == nil {
		return nil, nil
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesReleased,
		Timestamp:   now,
		Data: &portfoliov1.SharesReleased{
			AccountId:   cmd.AccountID,
			OrderSagaId: cmd.OrderSagaID,
			Symbol:      cmd.Symbol,
			Quantity:    cmd.Quantity,
			ReleasedAt:  timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type SettleSale struct {
	AccountID     string
	OrderSagaID   string
	Symbol        string
	Quantity      int64
	PricePerShare int64
	Proceeds      int64
}

func (c SettleSale) AggregateID() string {
	return AggregateID(c.AccountID)
}

func ExecuteSettleSale(p *Portfolio, cmd SettleSale) ([]es.Event, error) {
	now := time.Now()
	evt := es.Event{
		AggregateID: p.AggregateID(),
		Type:        EventSharesSettled,
		Timestamp:   now,
		Data: &portfoliov1.SharesSettled{
			AccountId:     cmd.AccountID,
			OrderSagaId:   cmd.OrderSagaID,
			Symbol:        cmd.Symbol,
			Quantity:      cmd.Quantity,
			PricePerShare: cmd.PricePerShare,
			Proceeds:      cmd.Proceeds,
			SettledAt:     timestamppb.New(now),
		},
	}

	if err := p.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}
