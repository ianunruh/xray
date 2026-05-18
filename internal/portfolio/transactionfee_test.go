package portfolio_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ianunruh/xray/internal/margin"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

// withTxnFeeBps pins TxnFeeBps for the duration of a test (overriding
// the package-wide zero set by TestMain). Production default is 10.
func withTxnFeeBps(t *testing.T, bps int64) {
	t.Helper()
	prev := margin.TxnFeeBps
	margin.TxnFeeBps = bps
	t.Cleanup(func() { margin.TxnFeeBps = prev })
}

func TestTransactionFee_BuyChargesFee(t *testing.T) {
	withTxnFeeBps(t, 10)

	ctx := context.Background()
	handler := newTestHandler(memstore.New(), newTestRegistry())

	require.NoError(t, handler.Handle(ctx,
		portfolio.DepositCash{AccountID: "a", Amount: 20_000_000},
		func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "a", Amount: 20_000_000})
		}))

	hold := portfolio.HoldCash{AccountID: "a", OrderSagaID: "s", Amount: 15_000_000}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	}))

	settle := portfolio.SettleTrade{
		AccountID: "a", OrderSagaID: "s", TradeID: "t",
		Amount: 15_000_000, Symbol: "AAPL", Quantity: 100, CostPerShare: 150_000,
	}
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("a"))
	require.NoError(t, err)
	// Started with 20M, held 15M (cash 5M), settled 15M (hold drained),
	// then fee = 15_000_000 * 10 / 10000 = 15_000 debited.
	assert.Equal(t, int64(20_000_000-15_000_000-margin.TxnFeeAmount(15_000_000)), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
}

func TestTransactionFee_SellChargesFee(t *testing.T) {
	withTxnFeeBps(t, 10)

	ctx := context.Background()
	handler := newTestHandler(memstore.New(), newTestRegistry())

	// Seed shares directly via CreditShares so we can isolate the sale.
	credit := portfolio.CreditShares{AccountID: "a", Symbol: "AAPL", Quantity: 100, CostPerShare: 100_000}
	require.NoError(t, handler.Handle(ctx, credit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCreditShares(p, credit)
	}))

	hold := portfolio.HoldShares{AccountID: "a", OrderSagaID: "s", Symbol: "AAPL", Quantity: 100}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, hold)
	}))

	settle := portfolio.SettleSale{
		AccountID: "a", OrderSagaID: "s", TradeID: "t",
		Symbol: "AAPL", Quantity: 100, PricePerShare: 150_000, Proceeds: 15_000_000,
	}
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleSale(p, settle)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("a"))
	require.NoError(t, err)
	// Proceeds 15M credited, fee 15_000 debited.
	assert.Equal(t, int64(15_000_000-margin.TxnFeeAmount(15_000_000)), p.CashBalance)
}

func TestTransactionFee_ZeroFeeOnTinyNotional(t *testing.T) {
	withTxnFeeBps(t, 10)

	ctx := context.Background()
	handler := newTestHandler(memstore.New(), newTestRegistry())

	require.NoError(t, handler.Handle(ctx,
		portfolio.DepositCash{AccountID: "a", Amount: 100_000},
		func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "a", Amount: 100_000})
		}))

	hold := portfolio.HoldCash{AccountID: "a", OrderSagaID: "s", Amount: 100}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	}))

	// Notional 100, fee = 100 * 10 / 10000 = 0 (rounds to zero).
	settle := portfolio.SettleTrade{
		AccountID: "a", OrderSagaID: "s", TradeID: "t",
		Amount: 100, Symbol: "X", Quantity: 1, CostPerShare: 100,
	}
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("a"))
	require.NoError(t, err)
	assert.Equal(t, int64(100_000-100), p.CashBalance)
}

func TestTransactionFee_IdempotentOnReplay(t *testing.T) {
	withTxnFeeBps(t, 10)

	ctx := context.Background()
	handler := newTestHandler(memstore.New(), newTestRegistry())

	require.NoError(t, handler.Handle(ctx,
		portfolio.DepositCash{AccountID: "a", Amount: 20_000_000},
		func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteDepositCash(p, portfolio.DepositCash{AccountID: "a", Amount: 20_000_000})
		}))
	hold := portfolio.HoldCash{AccountID: "a", OrderSagaID: "s", Amount: 15_000_000}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	}))
	settle := portfolio.SettleTrade{
		AccountID: "a", OrderSagaID: "s", TradeID: "t",
		Amount: 15_000_000, Symbol: "AAPL", Quantity: 100, CostPerShare: 150_000,
	}
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	}))

	// Replay: same saga, same trade — should no-op (no second fee).
	require.NoError(t, handler.Handle(ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("a"))
	require.NoError(t, err)
	assert.Equal(t, int64(20_000_000-15_000_000-margin.TxnFeeAmount(15_000_000)), p.CashBalance)
}
