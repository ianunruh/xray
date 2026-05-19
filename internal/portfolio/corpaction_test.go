package portfolio_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newSplitEnv(t *testing.T) (*es.Handler[*portfolio.Portfolio], context.Context) {
	t.Helper()
	registry := newTestRegistry()
	store := memstore.New()
	return newTestHandler(store, registry), context.Background()
}

func creditShares(t *testing.T, h *es.Handler[*portfolio.Portfolio], accountID, symbol string, qty, costPerShare int64) {
	t.Helper()
	cmd := portfolio.CreditShares{AccountID: accountID, Symbol: symbol, Quantity: qty, CostPerShare: costPerShare}
	require.NoError(t, h.Handle(context.Background(), cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCreditShares(p, cmd)
	}))
}

func TestAdjustHolding_ForwardSplit(t *testing.T) {
	handler, ctx := newSplitEnv(t)
	// Use Deposit to ensure the account exists with a valid AccountID
	// — CreditShares sets it but Deposit is the simpler bootstrap.
	fund(t, handler, "acct-1", 100_000_000)
	creditShares(t, handler, "acct-1", "AAPL", 100, 500_000) // 100 shares @ $50

	cmd := portfolio.AdjustHolding{
		AccountID:   "acct-1",
		ActionID:    "split-1",
		Symbol:      "AAPL",
		Numerator:   2,
		Denominator: 1,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteAdjustHolding(p, cmd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	h := p.Holdings["AAPL"]
	require.NotNil(t, h)
	assert.Equal(t, int64(200), h.Quantity, "2-for-1 doubles quantity")
	assert.Equal(t, int64(100)*int64(500_000), h.TotalCost, "TotalCost preserved across splits")
	// avg_cost derived = TotalCost / Quantity = $25 — auto-scaled.
	assert.True(t, p.HasAppliedAction("split-1"), "dedup set marked")
}

func TestAdjustHolding_ReverseSplit_CleanDivision(t *testing.T) {
	handler, ctx := newSplitEnv(t)
	fund(t, handler, "acct-1", 100_000_000)
	creditShares(t, handler, "acct-1", "AAPL", 100, 50_000)

	cmd := portfolio.AdjustHolding{
		AccountID:   "acct-1",
		ActionID:    "rev-1",
		Symbol:      "AAPL",
		Numerator:   1,
		Denominator: 10,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteAdjustHolding(p, cmd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	h := p.Holdings["AAPL"]
	require.NotNil(t, h)
	assert.Equal(t, int64(10), h.Quantity, "1-for-10 of 100 shares yields 10")
	assert.Equal(t, int64(100)*int64(50_000), h.TotalCost, "cost basis preserved")
}

func TestAdjustHolding_ReverseSplit_Truncation(t *testing.T) {
	handler, ctx := newSplitEnv(t)
	fund(t, handler, "acct-1", 100_000_000)
	creditShares(t, handler, "acct-1", "AAPL", 105, 50_000)

	cmd := portfolio.AdjustHolding{
		AccountID:   "acct-1",
		ActionID:    "rev-1",
		Symbol:      "AAPL",
		Numerator:   1,
		Denominator: 10,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteAdjustHolding(p, cmd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	h := p.Holdings["AAPL"]
	require.NotNil(t, h)
	assert.Equal(t, int64(10), h.Quantity, "1-for-10 of 105 yields 10 + 5 truncated")
}

func TestAdjustHolding_Idempotent(t *testing.T) {
	handler, ctx := newSplitEnv(t)
	fund(t, handler, "acct-1", 100_000_000)
	creditShares(t, handler, "acct-1", "AAPL", 100, 500_000)

	cmd := portfolio.AdjustHolding{
		AccountID:   "acct-1",
		ActionID:    "split-1",
		Symbol:      "AAPL",
		Numerator:   2,
		Denominator: 1,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteAdjustHolding(p, cmd)
	}))
	// Re-apply: no-op (would otherwise multiply twice and yield 400).
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		evts, err := portfolio.ExecuteAdjustHolding(p, cmd)
		require.NoError(t, err)
		assert.Empty(t, evts, "repeat apply must emit no events")
		return evts, nil
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(200), p.Holdings["AAPL"].Quantity, "still 200, not 400")
}

func TestAdjustHolding_NoPosition(t *testing.T) {
	// Action applies but account doesn't hold the symbol — still
	// emits the event so AppliedActions records the apply (audit
	// trail completeness).
	handler, ctx := newSplitEnv(t)
	fund(t, handler, "acct-1", 100_000_000)

	cmd := portfolio.AdjustHolding{
		AccountID:   "acct-1",
		ActionID:    "split-1",
		Symbol:      "AAPL",
		Numerator:   2,
		Denominator: 1,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteAdjustHolding(p, cmd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Nil(t, p.Holdings["AAPL"], "no holding created")
	assert.True(t, p.HasAppliedAction("split-1"), "but marked applied")
}

func TestAdjustHolding_AcrossMultipleActions(t *testing.T) {
	// Two distinct splits on different action IDs should compound.
	handler, ctx := newSplitEnv(t)
	fund(t, handler, "acct-1", 100_000_000)
	creditShares(t, handler, "acct-1", "AAPL", 100, 500_000)

	require.NoError(t, handler.Handle(ctx, portfolio.AdjustHolding{
		AccountID:   "acct-1",
		ActionID:    "split-1",
		Symbol:      "AAPL",
		Numerator:   2,
		Denominator: 1,
	}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteAdjustHolding(p, portfolio.AdjustHolding{
			AccountID:   "acct-1",
			ActionID:    "split-1",
			Symbol:      "AAPL",
			Numerator:   2,
			Denominator: 1,
		})
	}))

	require.NoError(t, handler.Handle(ctx, portfolio.AdjustHolding{
		AccountID:   "acct-1",
		ActionID:    "split-2",
		Symbol:      "AAPL",
		Numerator:   3,
		Denominator: 1,
	}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteAdjustHolding(p, portfolio.AdjustHolding{
			AccountID:   "acct-1",
			ActionID:    "split-2",
			Symbol:      "AAPL",
			Numerator:   3,
			Denominator: 1,
		})
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(600), p.Holdings["AAPL"].Quantity, "100 → 200 (2x) → 600 (3x)")
	assert.True(t, p.HasAppliedAction("split-1"))
	assert.True(t, p.HasAppliedAction("split-2"))
}
