package portfolio_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTestRegistry() *es.Registry {
	r := es.NewRegistry()
	portfolio.RegisterEvents(r)
	return r
}

func newTestHandler(store es.EventStore, registry *es.Registry) *es.Handler[*portfolio.Portfolio] {
	return es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, slog.Default())
}

func TestDepositCash(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	cmd := portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}
	err := handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, cmd)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(10000000), p.CashBalance)
	assert.Equal(t, "acct-1", p.AccountID)
}

func TestDepositCash_InvalidAmount(t *testing.T) {
	p := portfolio.NewPortfolio(portfolio.AggregateID("acct-1"))
	_, err := portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 0})
	assert.ErrorIs(t, err, portfolio.ErrInvalidAmount)

	_, err = portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: -100})
	assert.ErrorIs(t, err, portfolio.ErrInvalidAmount)
}

func TestWithdrawCash(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	withdraw := portfolio.WithdrawCash{AccountID: "acct-1", Amount: 3000000}
	err = handler.Handle(ctx, withdraw, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteWithdrawCash(p, withdraw)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(7000000), p.CashBalance)
}

func TestWithdrawCash_InsufficientFunds(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 5000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	withdraw := portfolio.WithdrawCash{AccountID: "acct-1", Amount: 6000000}
	err = handler.Handle(ctx, withdraw, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteWithdrawCash(p, withdraw)
	})
	assert.ErrorIs(t, err, portfolio.ErrInsufficientFunds)
}

func TestHoldAndReleaseCash(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 4000000}
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(6000000), p.CashBalance)
	assert.Equal(t, int64(4000000), p.CashHeld)
	assert.Equal(t, int64(4000000), p.HoldsBySaga["saga-1"])

	release := portfolio.ReleaseCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 4000000}
	err = handler.Handle(ctx, release, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseCash(p, release)
	})
	require.NoError(t, err)

	p, err = handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(10000000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Empty(t, p.HoldsBySaga)
}

func TestHoldCash_InsufficientFunds(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 5000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 6000000}
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	assert.ErrorIs(t, err, portfolio.ErrInsufficientFunds)
}

func TestSettleTrade(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 15000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 15000000}
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	require.NoError(t, err)

	// Settle a fill: 100 shares at $150.00 = $15,000.00
	settle := portfolio.SettleTrade{
		AccountID:    "acct-1",
		OrderSagaID:  "saga-1",
		Amount:       15000000,
		Symbol:       "AAPL",
		Quantity:     100,
		CostPerShare: 1500000,
	}
	err = handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Empty(t, p.HoldsBySaga)

	h := p.Holdings["AAPL"]
	require.NotNil(t, h)
	assert.Equal(t, int64(100), h.Quantity)
	assert.Equal(t, int64(150000000), h.TotalCost) // 100 * 1500000
}

func TestSettleTrade_CostBasisAccumulation(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 30000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 15000000}
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	require.NoError(t, err)

	hold2 := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-2", Amount: 10000000}
	err = handler.Handle(ctx, hold2, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold2)
	})
	require.NoError(t, err)

	// First fill: 100 shares at $150.00
	settle1 := portfolio.SettleTrade{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Amount: 15000000, Symbol: "AAPL", Quantity: 100, CostPerShare: 1500000,
	}
	err = handler.Handle(ctx, settle1, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle1)
	})
	require.NoError(t, err)

	// Second fill: 50 shares at $100.00
	settle2 := portfolio.SettleTrade{
		AccountID: "acct-1", OrderSagaID: "saga-2",
		Amount: 5000000, Symbol: "AAPL", Quantity: 50, CostPerShare: 1000000,
	}
	err = handler.Handle(ctx, settle2, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle2)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)

	h := p.Holdings["AAPL"]
	require.NotNil(t, h)
	assert.Equal(t, int64(150), h.Quantity)
	// Total cost = (100 * $150) + (50 * $100) = $15,000 + $5,000 = $20,000
	assert.Equal(t, int64(200000000), h.TotalCost)
}

func TestSettleTrade_PartialFill(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 15000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 15000000}
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	require.NoError(t, err)

	// Partial fill: 60 of 100 shares at $150.00
	settle := portfolio.SettleTrade{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Amount: 9000000, Symbol: "AAPL", Quantity: 60, CostPerShare: 1500000,
	}
	err = handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(6000000), p.CashHeld) // 15M - 9M = 6M remaining
	assert.Equal(t, int64(6000000), p.HoldsBySaga["saga-1"])
	assert.Equal(t, int64(60), p.Holdings["AAPL"].Quantity)
}
