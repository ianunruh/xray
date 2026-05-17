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

func TestSettleTrade_OverflowDebitsCashBalance(t *testing.T) {
	// When a fill costs more than was held (market BUY that swept higher
	// than the hold estimated), CashHeld is capped at zero and the overrun
	// is taken from CashBalance — never silently dropped.
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 15000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 10000000}
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	require.NoError(t, err)

	// Fill exceeds the hold by 500,000.
	settle := portfolio.SettleTrade{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Amount: 10500000, Symbol: "AAPL", Quantity: 70, CostPerShare: 150000,
	}
	err = handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	})
	require.NoError(t, err)

	p := mustLoad(t, handler, ctx, "acct-1")
	// Deposit 15M, held 10M (balance → 5M), settle 10.5M (10M from hold + 0.5M from balance).
	assert.Equal(t, int64(4500000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Empty(t, p.HoldsBySaga)
	assert.Equal(t, int64(70), p.Holdings["AAPL"].Quantity)
}

func TestSettleTrade_PartialFillThenOverflow(t *testing.T) {
	// Two-fill case: first fill is within the hold, second fill exceeds the
	// remaining hold and the overrun hits CashBalance.
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 20000000}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 10000000}
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	require.NoError(t, err)

	// First fill: 7M, well within the 10M hold.
	fill1 := portfolio.SettleTrade{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Amount: 7000000, Symbol: "AAPL", Quantity: 50, CostPerShare: 140000,
	}
	err = handler.Handle(ctx, fill1, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, fill1)
	})
	require.NoError(t, err)

	// Second fill: 4M, but only 3M of hold remains. 1M should come from balance.
	fill2 := portfolio.SettleTrade{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Amount: 4000000, Symbol: "AAPL", Quantity: 25, CostPerShare: 160000,
	}
	err = handler.Handle(ctx, fill2, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, fill2)
	})
	require.NoError(t, err)

	p := mustLoad(t, handler, ctx, "acct-1")
	// Deposit 20M, hold moves 10M → CashHeld. After both fills hold should be 0.
	// Cash balance: started at 10M after hold, second fill overflowed by 1M → 9M.
	assert.Equal(t, int64(9000000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Empty(t, p.HoldsBySaga)
	assert.Equal(t, int64(75), p.Holdings["AAPL"].Quantity)
}

func setupPortfolioWithHolding(t *testing.T, handler *es.Handler[*portfolio.Portfolio], ctx context.Context, accountID, symbol string, qty, costPerShare int64) {
	t.Helper()

	totalCost := costPerShare * qty
	deposit := portfolio.DepositCash{AccountID: accountID, Amount: totalCost}
	err := handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	})
	require.NoError(t, err)

	hold := portfolio.HoldCash{AccountID: accountID, OrderSagaID: "setup-saga", Amount: totalCost}
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	require.NoError(t, err)

	settle := portfolio.SettleTrade{
		AccountID: accountID, OrderSagaID: "setup-saga",
		Amount: totalCost, Symbol: symbol, Quantity: qty, CostPerShare: costPerShare,
	}
	err = handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	})
	require.NoError(t, err)
}

func TestHoldAndReleaseShares(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)

	hold := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 60}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(60), p.SharesHeld["AAPL"])
	assert.Equal(t, int64(60), p.ShareHoldsBySaga["saga-1"].Quantity)

	release := portfolio.ReleaseShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 60}
	err = handler.Handle(ctx, release, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseShares(p, release)
	})
	require.NoError(t, err)

	p, err = handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Empty(t, p.SharesHeld)
	assert.Empty(t, p.ShareHoldsBySaga)
}

func TestHoldCash_Idempotent(t *testing.T) {
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

	// Second hold for the same saga must be a no-op: no event, no double-deduction.
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	require.NoError(t, err)

	p := mustLoad(t, handler, ctx, "acct-1")
	assert.Equal(t, int64(6000000), p.CashBalance)
	assert.Equal(t, int64(4000000), p.CashHeld)
	assert.Equal(t, int64(4000000), p.HoldsBySaga["saga-1"])
	assert.Equal(t, p.Version(), 2) // Deposit + Hold, no second Hold event.
}

func TestHoldShares_Idempotent(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)
	versionBeforeHold := mustLoad(t, handler, ctx, "acct-1").Version()

	hold := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 60}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	})
	require.NoError(t, err)

	// Second hold for the same saga must be a no-op: no event, no double-reservation.
	err = handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	})
	require.NoError(t, err)

	p := mustLoad(t, handler, ctx, "acct-1")
	assert.Equal(t, int64(60), p.SharesHeld["AAPL"])
	assert.Equal(t, int64(60), p.ShareHoldsBySaga["saga-1"].Quantity)
	assert.Equal(t, p.Version(), versionBeforeHold+1) // single Hold event, second was a no-op.
}

func TestReleaseCash_Idempotent(t *testing.T) {
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

	release := portfolio.ReleaseCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 4000000}
	err = handler.Handle(ctx, release, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseCash(p, release)
	})
	require.NoError(t, err)

	// Second release for the same saga must be a no-op: no event, no balance change.
	err = handler.Handle(ctx, release, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseCash(p, release)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(10000000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Equal(t, p.Version(), 3) // Deposit + Hold + Release, no second Release event.
}

func TestReleaseShares_Idempotent(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)
	versionBeforeHold := mustLoad(t, handler, ctx, "acct-1").Version()

	hold := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 60}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	})
	require.NoError(t, err)

	release := portfolio.ReleaseShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 60}
	err = handler.Handle(ctx, release, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseShares(p, release)
	})
	require.NoError(t, err)

	// Second release for the same saga must be a no-op: no event, no state change.
	err = handler.Handle(ctx, release, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseShares(p, release)
	})
	require.NoError(t, err)

	p := mustLoad(t, handler, ctx, "acct-1")
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Empty(t, p.SharesHeld)
	assert.Empty(t, p.ShareHoldsBySaga)
	assert.Equal(t, p.Version(), versionBeforeHold+2) // Hold + Release, no second Release event.
}

func mustLoad(t *testing.T, handler *es.Handler[*portfolio.Portfolio], ctx context.Context, accountID string) *portfolio.Portfolio {
	t.Helper()
	p, err := handler.Load(ctx, portfolio.AggregateID(accountID))
	require.NoError(t, err)
	return p
}

func TestHoldShares_InsufficientShares(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)

	hold := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 101}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	})
	assert.ErrorIs(t, err, portfolio.ErrInsufficientShares)
}

func TestHoldShares_InsufficientShares_AlreadyHeld(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)

	hold1 := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 80}
	err := handler.Handle(ctx, hold1, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold1)
	})
	require.NoError(t, err)

	hold2 := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-2", Symbol: "AAPL", Quantity: 30}
	err = handler.Handle(ctx, hold2, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold2)
	})
	assert.ErrorIs(t, err, portfolio.ErrInsufficientShares)
}

func TestSettleSale(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)

	hold := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 100}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	})
	require.NoError(t, err)

	settle := portfolio.SettleSale{
		AccountID:     "acct-1",
		OrderSagaID:   "saga-1",
		Symbol:        "AAPL",
		Quantity:      100,
		PricePerShare: 1550000,
		Proceeds:      155000000,
	}
	err = handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleSale(p, settle)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(155000000), p.CashBalance)
	assert.Empty(t, p.SharesHeld)
	assert.Empty(t, p.ShareHoldsBySaga)
	assert.Nil(t, p.Holdings["AAPL"])
}

func TestSettleSale_PartialFill(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)

	hold := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 100}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	})
	require.NoError(t, err)

	settle := portfolio.SettleSale{
		AccountID:     "acct-1",
		OrderSagaID:   "saga-1",
		Symbol:        "AAPL",
		Quantity:      60,
		PricePerShare: 1550000,
		Proceeds:      93000000,
	}
	err = handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleSale(p, settle)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(93000000), p.CashBalance)
	assert.Equal(t, int64(40), p.SharesHeld["AAPL"])
	assert.Equal(t, int64(40), p.ShareHoldsBySaga["saga-1"].Quantity)
	assert.Equal(t, int64(40), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(60000000), p.Holdings["AAPL"].TotalCost) // 40/100 * 150M = 60M
}

func TestCreditShares(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	// Credit 100 shares at $150.00 per share
	cmd := portfolio.CreditShares{AccountID: "acct-1", Symbol: "AAPL", Quantity: 100, CostPerShare: 1500000}
	err := handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCreditShares(p, cmd)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, "acct-1", p.AccountID)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(150000000), p.Holdings["AAPL"].TotalCost) // 100 * 1500000
}

func TestCreditShares_InvalidQuantity(t *testing.T) {
	p := portfolio.NewPortfolio(portfolio.AggregateID("acct-1"))
	_, err := portfolio.ExecuteCreditShares(p, portfolio.CreditShares{AccountID: "acct-1", Symbol: "AAPL", Quantity: 0})
	assert.ErrorIs(t, err, portfolio.ErrInvalidQuantity)

	_, err = portfolio.ExecuteCreditShares(p, portfolio.CreditShares{AccountID: "acct-1", Symbol: "AAPL", Quantity: -10})
	assert.ErrorIs(t, err, portfolio.ErrInvalidQuantity)
}

func TestSettleTrade_IdempotentPerTradeID(t *testing.T) {
	// A redelivered TradeExecuted must not double-settle. Tests the
	// dedup path that protects against reactor batch retries after a
	// mid-batch crash.
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}
	require.NoError(t, handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	}))

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 9000000}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	}))

	settle := portfolio.SettleTrade{
		AccountID:    "acct-1",
		OrderSagaID:  "saga-1",
		TradeID:      "trade-A",
		Amount:       4500000,
		Symbol:       "AAPL",
		Quantity:     30,
		CostPerShare: 150000,
	}
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	}))

	// Replay the same trade — must be a no-op.
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	}))

	p := mustLoad(t, handler, ctx, "acct-1")
	assert.Equal(t, int64(4500000), p.CashHeld, "hold only decremented once (9M - 4.5M)")
	assert.Equal(t, int64(1000000), p.CashBalance, "balance unchanged after the dup")
	assert.Equal(t, int64(30), p.Holdings["AAPL"].Quantity, "shares credited once")
	// Deposit + Hold + Settle = 3 events; second Settle was suppressed.
	assert.Equal(t, 3, p.Version())
}

func TestSettleSale_IdempotentPerTradeID(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)

	hold := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 100}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	}))

	settle := portfolio.SettleSale{
		AccountID:     "acct-1",
		OrderSagaID:   "saga-1",
		TradeID:       "trade-X",
		Symbol:        "AAPL",
		Quantity:      40,
		PricePerShare: 1550000,
		Proceeds:      62000000,
	}
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleSale(p, settle)
	}))

	// Replay — no-op.
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleSale(p, settle)
	}))

	p := mustLoad(t, handler, ctx, "acct-1")
	assert.Equal(t, int64(60), p.Holdings["AAPL"].Quantity, "only 40 of 100 shares sold")
	assert.Equal(t, int64(60), p.SharesHeld["AAPL"], "remaining hold unchanged after dup")
	assert.Equal(t, int64(62000000), p.CashBalance, "credited only once")
}

func TestSettleSale_DifferentTradeIDsBothApply(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 100, 1500000)

	hold := portfolio.HoldShares{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 100}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	}))

	for i, tradeID := range []string{"trade-1", "trade-2"} {
		settle := portfolio.SettleSale{
			AccountID:     "acct-1",
			OrderSagaID:   "saga-1",
			TradeID:       tradeID,
			Symbol:        "AAPL",
			Quantity:      50,
			PricePerShare: 1550000,
			Proceeds:      77500000,
		}
		require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteSettleSale(p, settle)
		}), "fill %d", i)
	}

	p := mustLoad(t, handler, ctx, "acct-1")
	assert.Empty(t, p.Holdings["AAPL"], "all 100 shares sold across two fills")
	assert.Empty(t, p.SharesHeld)
	assert.Equal(t, int64(155000000), p.CashBalance)
}

func TestCreditShares_Accumulation(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	// First credit: 100 shares at $150.00
	cmd1 := portfolio.CreditShares{AccountID: "acct-1", Symbol: "AAPL", Quantity: 100, CostPerShare: 1500000}
	err := handler.Handle(ctx, cmd1, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCreditShares(p, cmd1)
	})
	require.NoError(t, err)

	// Second credit: 50 shares at $100.00
	cmd2 := portfolio.CreditShares{AccountID: "acct-1", Symbol: "AAPL", Quantity: 50, CostPerShare: 1000000}
	err = handler.Handle(ctx, cmd2, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCreditShares(p, cmd2)
	})
	require.NoError(t, err)

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(150), p.Holdings["AAPL"].Quantity)
	// Total cost = (100 * $150) + (50 * $100) = $15,000 + $5,000 = $20,000
	assert.Equal(t, int64(200000000), p.Holdings["AAPL"].TotalCost)
}

// --- Short-selling tests ---

// totalEncumberedCash sums every place cash can live on a portfolio.
// Invariant: deposits - withdrawals - realized losses on shorts =
// totalEncumberedCash (and inversely for realized gains).
func totalEncumberedCash(p *portfolio.Portfolio) int64 {
	total := p.CashBalance + p.CashHeld + p.CollateralPool + p.ProceedsPool
	for _, h := range p.CollateralHeldBySaga {
		total += h.Amount
	}
	return total
}

func TestHoldCollateral_HappyPath(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}
	require.NoError(t, handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	}))

	hold := portfolio.HoldCollateral{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Symbol: "AAPL", Quantity: 100, Amount: 7500000,
	}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, hold)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(2500000), p.CashBalance)
	assert.Equal(t, int64(7500000), p.CollateralHeldBySaga["saga-1"].Amount)
	assert.Equal(t, int64(10000000), totalEncumberedCash(p))
}

func TestHoldCollateral_InsufficientFunds(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 5000000}
	require.NoError(t, handler.Handle(ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	}))

	hold := portfolio.HoldCollateral{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Symbol: "AAPL", Quantity: 100, Amount: 7500000,
	}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, hold)
	})
	assert.ErrorIs(t, err, portfolio.ErrInsufficientFunds)
}

func TestHoldCollateral_RefusesWhenLong(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	setupPortfolioWithHolding(t, handler, ctx, "acct-1", "AAPL", 50, 1500000)
	// Top up cash so the only failure point is the long-conflict check.
	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))

	hold := portfolio.HoldCollateral{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Symbol: "AAPL", Quantity: 100, Amount: 7500000,
	}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, hold)
	})
	assert.ErrorIs(t, err, portfolio.ErrShortHoldsLong)
}

func TestReleaseCollateral_HappyPath(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))

	hold := portfolio.HoldCollateral{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Symbol: "AAPL", Quantity: 100, Amount: 7500000,
	}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, hold)
	}))

	release := portfolio.ReleaseCollateral{AccountID: "acct-1", OrderSagaID: "saga-1"}
	require.NoError(t, handler.Handle(ctx, release, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseCollateral(p, release)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(10000000), p.CashBalance)
	assert.Empty(t, p.CollateralHeldBySaga)
}

func TestOpenShort_HappyPath(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	// Deposit $100, hold $75 collateral, open short of 100 shares @ $150.
	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))

	hold := portfolio.HoldCollateral{
		AccountID: "acct-1", OrderSagaID: "saga-1",
		Symbol: "AAPL", Quantity: 100, Amount: 7500000,
	}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, hold)
	}))

	open := portfolio.OpenShort{
		AccountID: "acct-1", OrderSagaID: "saga-1", TradeID: "trade-1",
		Symbol: "AAPL", Quantity: 100, PricePerShare: 1500000,
	}
	require.NoError(t, handler.Handle(ctx, open, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteOpenShort(p, open)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)

	short := p.ShortPositions["AAPL"]
	require.NotNil(t, short)
	assert.Equal(t, int64(100), short.Quantity)
	assert.Equal(t, int64(150000000), short.ProceedsHeld) // 100 * 1.5M
	assert.Equal(t, int64(7500000), short.CollateralHeld)
	assert.Equal(t, int64(1500000), short.AvgOpenPrice)
	assert.Equal(t, int64(150000000), p.ProceedsPool)
	assert.Equal(t, int64(7500000), p.CollateralPool)
	assert.Empty(t, p.CollateralHeldBySaga)
	assert.Equal(t, int64(2500000), p.CashBalance)
	assert.Equal(t, int64(160000000), totalEncumberedCash(p))
}

func TestOpenShort_WeightedAvgAcrossOpens(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 100000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 100000000})
	}))

	// Open 100 @ $150, then 50 @ $120 — weighted avg = (100*150 + 50*120) / 150 = 140.
	for i, in := range []struct {
		saga, trade  string
		qty, price   int64
		collateral   int64
	}{
		{"saga-1", "trade-1", 100, 1500000, 7500000},
		{"saga-2", "trade-2", 50, 1200000, 3000000},
	} {
		_ = i
		hold := portfolio.HoldCollateral{
			AccountID: "acct-1", OrderSagaID: in.saga,
			Symbol: "AAPL", Quantity: in.qty, Amount: in.collateral,
		}
		require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteHoldCollateral(p, hold)
		}))
		open := portfolio.OpenShort{
			AccountID: "acct-1", OrderSagaID: in.saga, TradeID: in.trade,
			Symbol: "AAPL", Quantity: in.qty, PricePerShare: in.price,
		}
		require.NoError(t, handler.Handle(ctx, open, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteOpenShort(p, open)
		}))
	}

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(150), p.ShortPositions["AAPL"].Quantity)
	assert.Equal(t, int64(1400000), p.ShortPositions["AAPL"].AvgOpenPrice)
}

func TestOpenShort_Idempotent(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))
	require.NoError(t, handler.Handle(ctx, portfolio.HoldCollateral{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 100, Amount: 7500000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, portfolio.HoldCollateral{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Quantity: 100, Amount: 7500000})
	}))
	open := portfolio.OpenShort{
		AccountID: "acct-1", OrderSagaID: "saga-1", TradeID: "trade-1",
		Symbol: "AAPL", Quantity: 100, PricePerShare: 1500000,
	}
	require.NoError(t, handler.Handle(ctx, open, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteOpenShort(p, open)
	}))
	// Redeliver — must be a no-op.
	require.NoError(t, handler.Handle(ctx, open, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteOpenShort(p, open)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(100), p.ShortPositions["AAPL"].Quantity)
}

func TestHoldShortCover_InsufficientShortQty(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))
	openShort(t, handler, ctx, "acct-1", "AAPL", 100, 1500000, 7500000)

	hold := portfolio.HoldShortCover{
		AccountID: "acct-1", OrderSagaID: "cover-1",
		Symbol: "AAPL", Quantity: 101,
	}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShortCover(p, hold)
	})
	assert.ErrorIs(t, err, portfolio.ErrInsufficientShortQty)
}

func TestCoverShort_FullCloseProfit(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	// Deposit $100, open 100 @ $150 with $75 collateral, then cover at $120.
	// Expected realized PnL = (150 - 120) * 100 = +3000.
	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))
	openShort(t, handler, ctx, "acct-1", "AAPL", 100, 1500000, 7500000)

	cover := portfolio.CoverShort{
		AccountID: "acct-1", OrderSagaID: "cover-1", TradeID: "ctrade-1",
		Symbol: "AAPL", Quantity: 100, CostPerShare: 1200000,
	}
	require.NoError(t, handler.Handle(ctx, cover, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCoverShort(p, cover)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Empty(t, p.ShortPositions)
	assert.Equal(t, int64(0), p.ProceedsPool)
	assert.Equal(t, int64(0), p.CollateralPool)
	// Realized PnL = (1500000 - 1200000) * 100 = 30M.
	// Final cash = deposit 10M + realized 30M = 40M.
	assert.Equal(t, int64(40000000), p.CashBalance)
	assert.Equal(t, int64(40000000), totalEncumberedCash(p))
}

func TestCoverShort_FullCloseLoss(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	// Open 100 @ $150 with $75 collateral, cover at $180 — loss of $30/sh = $3000.
	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))
	openShort(t, handler, ctx, "acct-1", "AAPL", 100, 1500000, 7500000)

	cover := portfolio.CoverShort{
		AccountID: "acct-1", OrderSagaID: "cover-1", TradeID: "ctrade-1",
		Symbol: "AAPL", Quantity: 100, CostPerShare: 1800000,
	}
	require.NoError(t, handler.Handle(ctx, cover, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCoverShort(p, cover)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	// Realized PnL = (1500000 - 1800000) * 100 = -30M. The 7.5M
	// collateral isn't nearly enough to absorb the 30M loss, so cash
	// goes negative: 10M deposit - 30M loss = -20M. (In a real system
	// margin call would have fired long before; the test verifies the
	// math, not the policy.)
	assert.Equal(t, int64(-20000000), p.CashBalance)
	assert.Empty(t, p.ShortPositions)
	assert.Equal(t, int64(-20000000), totalEncumberedCash(p))
}

func TestCoverShort_PartialThenFull(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))
	openShort(t, handler, ctx, "acct-1", "AAPL", 100, 1500000, 7500000)

	// Cover 40 of 100 at $120.
	cover1 := portfolio.CoverShort{
		AccountID: "acct-1", OrderSagaID: "cover-1", TradeID: "ctrade-1",
		Symbol: "AAPL", Quantity: 40, CostPerShare: 1200000,
	}
	require.NoError(t, handler.Handle(ctx, cover1, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCoverShort(p, cover1)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	short := p.ShortPositions["AAPL"]
	require.NotNil(t, short)
	assert.Equal(t, int64(60), short.Quantity)
	// 40% of pool released: proceeds 60M of 150M -> 90M remaining,
	// collateral 3M of 7.5M -> 4.5M remaining.
	assert.Equal(t, int64(90000000), short.ProceedsHeld)
	assert.Equal(t, int64(4500000), short.CollateralHeld)

	// Now cover the remaining 60 at $130 (still profit).
	cover2 := portfolio.CoverShort{
		AccountID: "acct-1", OrderSagaID: "cover-2", TradeID: "ctrade-2",
		Symbol: "AAPL", Quantity: 60, CostPerShare: 1300000,
	}
	require.NoError(t, handler.Handle(ctx, cover2, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCoverShort(p, cover2)
	}))

	p, err = handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Empty(t, p.ShortPositions)
	assert.Equal(t, int64(0), p.ProceedsPool)
	assert.Equal(t, int64(0), p.CollateralPool)
	// Realized PnL = (1500000-1200000)*40 + (1500000-1300000)*60
	//              = 12M + 12M = 24M. Final cash = 10M + 24M = 34M.
	assert.Equal(t, int64(34000000), p.CashBalance)
	assert.Equal(t, int64(34000000), totalEncumberedCash(p))
}

func TestCoverShort_CannotGoThroughZero(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))
	openShort(t, handler, ctx, "acct-1", "AAPL", 100, 1500000, 7500000)

	cover := portfolio.CoverShort{
		AccountID: "acct-1", OrderSagaID: "cover-1", TradeID: "ctrade-1",
		Symbol: "AAPL", Quantity: 101, CostPerShare: 1200000,
	}
	err := handler.Handle(ctx, cover, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCoverShort(p, cover)
	})
	assert.ErrorIs(t, err, portfolio.ErrInsufficientShortQty)
}

func TestCoverShort_Idempotent(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 10000000})
	}))
	openShort(t, handler, ctx, "acct-1", "AAPL", 100, 1500000, 7500000)

	cover := portfolio.CoverShort{
		AccountID: "acct-1", OrderSagaID: "cover-1", TradeID: "ctrade-1",
		Symbol: "AAPL", Quantity: 100, CostPerShare: 1200000,
	}
	require.NoError(t, handler.Handle(ctx, cover, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCoverShort(p, cover)
	}))
	// Redeliver — must be a no-op.
	require.NoError(t, handler.Handle(ctx, cover, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCoverShort(p, cover)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(40000000), p.CashBalance)
}

func TestHoldCash_RefusesWhenShort(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()

	require.NoError(t, handler.Handle(ctx, portfolio.DepositCash{AccountID: "acct-1", Amount: 20000000}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "acct-1", Amount: 20000000})
	}))
	openShort(t, handler, ctx, "acct-1", "AAPL", 100, 1500000, 7500000)

	// Try to long-buy AAPL while AAPL short is open.
	hold := portfolio.HoldCash{
		AccountID: "acct-1", OrderSagaID: "buy-1",
		Symbol: "AAPL", Amount: 1500000,
	}
	err := handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	})
	assert.ErrorIs(t, err, portfolio.ErrLongHoldsShort)
}

// openShort is a helper that runs HoldCollateral + OpenShort for a fresh saga,
// using deterministic saga/trade IDs derived from the symbol. Assumes the
// account has enough cash for the collateral.
func openShort(t *testing.T, handler *es.Handler[*portfolio.Portfolio], ctx context.Context, accountID, symbol string, qty, price, collateral int64) {
	t.Helper()
	sagaID := "open-" + symbol
	tradeID := "otrade-" + symbol
	hold := portfolio.HoldCollateral{
		AccountID: accountID, OrderSagaID: sagaID,
		Symbol: symbol, Quantity: qty, Amount: collateral,
	}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, hold)
	}))
	open := portfolio.OpenShort{
		AccountID: accountID, OrderSagaID: sagaID, TradeID: tradeID,
		Symbol: symbol, Quantity: qty, PricePerShare: price,
	}
	require.NoError(t, handler.Handle(ctx, open, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteOpenShort(p, open)
	}))
}
