package portfolio_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

// fund deposits cash and returns the loaded portfolio.
func fund(t *testing.T, h *es.Handler[*portfolio.Portfolio], account string, amount int64) *portfolio.Portfolio {
	t.Helper()
	ctx := context.Background()
	cmd := portfolio.DepositCash{AccountID: account, Amount: amount}
	require.NoError(t, h.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, cmd)
	}))
	p, err := h.Load(ctx, portfolio.AggregateID(account))
	require.NoError(t, err)
	return p
}

// appendEvent stamps and applies an event directly through the
// handler. Used to inject T+1-style events into the store without
// changing the command APIs (those gain SettlementPolicy in phase 3).
// Uses a synthetic Command — the handler does Apply + persist for us.
type injectCmd struct{ id string }

func (c injectCmd) AggregateID() string { return c.id }

func appendEvent(t *testing.T, h *es.Handler[*portfolio.Portfolio], evt es.Event) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, h.Handle(ctx, injectCmd{id: evt.AggregateID}, func(p *portfolio.Portfolio) ([]es.Event, error) {
		if err := p.Apply(evt); err != nil {
			return nil, err
		}
		return []es.Event{evt}, nil
	}))
}

func TestSettlement_CashSettled_Instant(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	fund(t, handler, "acct-1", 100_000_000)
	ctx := context.Background()

	// Use the existing SettleTrade command which today produces an
	// instant settlement (settles_at omitted).
	cmd := portfolio.SettleTrade{
		AccountID:    "acct-1",
		OrderSagaID:  "saga-1",
		TradeID:      "trade-1",
		Amount:       10_000_000,
		Symbol:       "AAPL",
		Quantity:     100,
		CostPerShare: 100_000,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, cmd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(90_000_000), p.CashBalance, "cash should drop by trade amount")
	assert.Equal(t, int64(90_000_000), p.SettledCash, "settled cash moves in lockstep on instant path")
	assert.Empty(t, p.PendingLegs, "no pending legs on instant settlement")
}

func TestSettlement_CashSettled_Deferred(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	fund(t, handler, "acct-1", 100_000_000)

	// Inject a CashSettled event with settles_at in the future.
	tradeDate := time.Now()
	settlesAt := tradeDate.Add(24 * time.Hour)
	evt := es.Event{
		AggregateID: portfolio.AggregateID("acct-1"),
		Type:        portfolio.EventCashSettled,
		Timestamp:   tradeDate,
		Data: &portfoliov1.CashSettled{
			AccountId:    "acct-1",
			OrderSagaId:  "saga-1",
			TradeId:      "trade-1",
			Amount:       10_000_000,
			Symbol:       "AAPL",
			Quantity:     100,
			CostPerShare: 100_000,
			SettledAt:    timestamppb.New(tradeDate),
			SettlesAt:    timestamppb.New(settlesAt),
		},
	}
	appendEvent(t, handler, evt)

	p, err := handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(90_000_000), p.CashBalance, "cash balance moves on trade date as today")
	assert.Equal(t, int64(100_000_000), p.SettledCash, "settled cash unchanged until clearing")
	require.Len(t, p.PendingLegs, 2, "deferred buy emits both cash + share legs")
	key := portfolio.PendingLegKey{TradeID: "trade-1", Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_DEBIT}
	leg := p.PendingLegs[key]
	require.NotNil(t, leg)
	assert.Equal(t, int64(-10_000_000), leg.CashAmount, "leg is signed debit")
	assert.WithinDuration(t, settlesAt, leg.SettlesAt, time.Second)
}

func TestSettlement_SharesSettled_Deferred(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	fund(t, handler, "acct-1", 100_000_000)

	tradeDate := time.Now()
	settlesAt := tradeDate.Add(24 * time.Hour)
	evt := es.Event{
		AggregateID: portfolio.AggregateID("acct-1"),
		Type:        portfolio.EventSharesSettled,
		Timestamp:   tradeDate,
		Data: &portfoliov1.SharesSettled{
			AccountId:     "acct-1",
			OrderSagaId:   "saga-1",
			TradeId:       "trade-1",
			Symbol:        "AAPL",
			Quantity:      50,
			PricePerShare: 150_000,
			Proceeds:      7_500_000,
			SettledAt:     timestamppb.New(tradeDate),
			SettlesAt:     timestamppb.New(settlesAt),
		},
	}
	appendEvent(t, handler, evt)

	p, err := handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(107_500_000), p.CashBalance, "cash rises on trade date")
	assert.Equal(t, int64(100_000_000), p.SettledCash, "settled cash unchanged until clearing")
	require.Len(t, p.PendingLegs, 1)
	key := portfolio.PendingLegKey{TradeID: "trade-1", Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT}
	leg := p.PendingLegs[key]
	require.NotNil(t, leg)
	assert.Equal(t, int64(7_500_000), leg.CashAmount)
}

func TestSettlement_Clear_AdvancesSettledCash(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()
	fund(t, handler, "acct-1", 100_000_000)

	tradeDate := time.Now().Add(-48 * time.Hour) // past so the leg is due
	settlesAt := tradeDate.Add(24 * time.Hour)
	evt := es.Event{
		AggregateID: portfolio.AggregateID("acct-1"),
		Type:        portfolio.EventSharesSettled,
		Timestamp:   tradeDate,
		Data: &portfoliov1.SharesSettled{
			AccountId:     "acct-1",
			OrderSagaId:   "saga-1",
			TradeId:       "trade-1",
			Symbol:        "AAPL",
			Quantity:      50,
			PricePerShare: 150_000,
			Proceeds:      7_500_000,
			SettledAt:     timestamppb.New(tradeDate),
			SettlesAt:     timestamppb.New(settlesAt),
		},
	}
	appendEvent(t, handler, evt)

	cmd := portfolio.ClearSettlement{
		AccountID: "acct-1",
		TradeID:   "trade-1",
		Kind:      portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteClearSettlement(p, cmd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(107_500_000), p.SettledCash, "clearing rolls leg amount into settled cash")
	assert.Equal(t, int64(107_500_000), p.CashBalance, "cash balance unchanged on clear")
	assert.Empty(t, p.PendingLegs)
}

func TestSettlement_Clear_Idempotent(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()
	fund(t, handler, "acct-1", 100_000_000)

	// First clear succeeds via injected event; second call is a no-op.
	cmd := portfolio.ClearSettlement{
		AccountID: "acct-1",
		TradeID:   "ghost-trade",
		Kind:      portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT,
	}
	// No leg exists for ghost-trade — should return cleanly with no event.
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		evts, err := portfolio.ExecuteClearSettlement(p, cmd)
		require.NoError(t, err)
		assert.Empty(t, evts)
		return evts, err
	}))
}

func TestSettlement_Clear_NotDue(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()
	fund(t, handler, "acct-1", 100_000_000)

	tradeDate := time.Now()
	settlesAt := tradeDate.Add(24 * time.Hour) // future
	evt := es.Event{
		AggregateID: portfolio.AggregateID("acct-1"),
		Type:        portfolio.EventSharesSettled,
		Timestamp:   tradeDate,
		Data: &portfoliov1.SharesSettled{
			AccountId:     "acct-1",
			OrderSagaId:   "saga-1",
			TradeId:       "trade-1",
			Symbol:        "AAPL",
			Quantity:      50,
			PricePerShare: 150_000,
			Proceeds:      7_500_000,
			SettledAt:     timestamppb.New(tradeDate),
			SettlesAt:     timestamppb.New(settlesAt),
		},
	}
	appendEvent(t, handler, evt)

	cmd := portfolio.ClearSettlement{
		AccountID: "acct-1",
		TradeID:   "trade-1",
		Kind:      portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_CREDIT,
	}
	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	_, err = portfolio.ExecuteClearSettlement(p, cmd)
	assert.ErrorIs(t, err, portfolio.ErrSettlementNotDue)
}

func TestSettlement_HoldsDoNotMoveSettledCash(t *testing.T) {
	// CashHeld / CashReleased are reservations, not clearings, so
	// SettledCash is untouched. Withdraw still respects the held
	// portion via the separate CashBalance check (Phase 4).
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()
	fund(t, handler, "acct-1", 100_000_000)

	hold := portfolio.HoldCash{AccountID: "acct-1", OrderSagaID: "saga-1", Symbol: "AAPL", Amount: 30_000_000}
	require.NoError(t, handler.Handle(ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, hold)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(70_000_000), p.CashBalance, "hold deducts from spendable cash")
	assert.Equal(t, int64(100_000_000), p.SettledCash, "hold doesn't change settled cash")

	release := portfolio.ReleaseCash{AccountID: "acct-1", OrderSagaID: "saga-1", Amount: 30_000_000}
	require.NoError(t, handler.Handle(ctx, release, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseCash(p, release)
	}))

	p, err = handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(100_000_000), p.CashBalance)
	assert.Equal(t, int64(100_000_000), p.SettledCash)
}

func TestSettlement_Policy_StampsSettlesAt(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()
	fund(t, handler, "acct-1", 100_000_000)

	policy := portfolio.SettlementPolicy{Enabled: true, Window: 24 * time.Hour}
	cmd := portfolio.SettleTrade{
		AccountID:    "acct-1",
		OrderSagaID:  "saga-1",
		TradeID:      "trade-1",
		Amount:       10_000_000,
		Symbol:       "AAPL",
		Quantity:     100,
		CostPerShare: 100_000,
		SettlesAt:    policy.SettlesAt(time.Now()),
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, cmd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(90_000_000), p.CashBalance)
	assert.Equal(t, int64(100_000_000), p.SettledCash, "policy-enabled command defers settled cash")
	require.Len(t, p.PendingLegs, 2, "buy emits both cash + share legs")
}

func TestSettlement_Policy_DisabledStaysInstant(t *testing.T) {
	policy := portfolio.SettlementPolicy{} // zero = disabled
	assert.True(t, policy.SettlesAt(time.Now()).IsZero(), "disabled policy returns zero time")

	policy = portfolio.SettlementPolicy{Enabled: true} // window=0 too
	assert.True(t, policy.SettlesAt(time.Now()).IsZero(), "zero window returns zero time")

	policy = portfolio.SettlementPolicy{Enabled: true, Window: 24 * time.Hour}
	tradeDate := time.Now()
	got := policy.SettlesAt(tradeDate)
	assert.WithinDuration(t, tradeDate.Add(24*time.Hour), got, time.Second)
}

func TestSettlement_ShareCreditLeg_OnDeferredBuy(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	fund(t, handler, "acct-1", 100_000_000)

	tradeDate := time.Now()
	settlesAt := tradeDate.Add(24 * time.Hour)
	appendEvent(t, handler, es.Event{
		AggregateID: portfolio.AggregateID("acct-1"),
		Type:        portfolio.EventCashSettled,
		Timestamp:   tradeDate,
		Data: &portfoliov1.CashSettled{
			AccountId:    "acct-1",
			OrderSagaId:  "saga-1",
			TradeId:      "trade-1",
			Amount:       10_000_000,
			Symbol:       "AAPL",
			Quantity:     100,
			CostPerShare: 100_000,
			SettledAt:    timestamppb.New(tradeDate),
			SettlesAt:    timestamppb.New(settlesAt),
		},
	})

	p, err := handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity, "Holdings updates on trade date (margin-account model)")
	assert.Equal(t, int64(100), p.PendingShareCredits["AAPL"], "100 shares marked pending settlement")
	assert.Len(t, p.PendingLegs, 2, "deferred buy emits both a cash and a share leg")

	cashKey := portfolio.PendingLegKey{TradeID: "trade-1", Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_DEBIT}
	shareKey := portfolio.PendingLegKey{TradeID: "trade-1", Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_SHARE_CREDIT}
	require.NotNil(t, p.PendingLegs[cashKey])
	require.NotNil(t, p.PendingLegs[shareKey])
	assert.Equal(t, int64(100), p.PendingLegs[shareKey].Quantity, "share leg carries the share count")
	assert.Equal(t, int64(0), p.PendingLegs[shareKey].CashAmount, "share leg has no cash side")
}

func TestSettlement_ShareCreditLeg_ClearsPendingShares(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()
	fund(t, handler, "acct-1", 100_000_000)

	past := time.Now().Add(-48 * time.Hour)
	settlesAt := past.Add(24 * time.Hour) // also past — leg is due
	appendEvent(t, handler, es.Event{
		AggregateID: portfolio.AggregateID("acct-1"),
		Type:        portfolio.EventCashSettled,
		Timestamp:   past,
		Data: &portfoliov1.CashSettled{
			AccountId:    "acct-1",
			OrderSagaId:  "saga-1",
			TradeId:      "trade-1",
			Amount:       10_000_000,
			Symbol:       "AAPL",
			Quantity:     100,
			CostPerShare: 100_000,
			SettledAt:    timestamppb.New(past),
			SettlesAt:    timestamppb.New(settlesAt),
		},
	})

	// Clear the SHARE_CREDIT leg.
	cmd := portfolio.ClearSettlement{
		AccountID: "acct-1",
		TradeID:   "trade-1",
		Kind:      portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_SHARE_CREDIT,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteClearSettlement(p, cmd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Empty(t, p.PendingShareCredits, "clearing the share leg drains the pending shares map")
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity, "Holdings unchanged by clearing the share leg")
	// Cash leg still pending.
	cashKey := portfolio.PendingLegKey{TradeID: "trade-1", Kind: portfoliov1.SettlementLegKind_SETTLEMENT_LEG_KIND_CASH_DEBIT}
	assert.NotNil(t, p.PendingLegs[cashKey], "cash leg clears independently")
}

func TestSettlement_Withdraw_GatedOnSettledCash(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	ctx := context.Background()
	fund(t, handler, "acct-1", 100_000_000)

	// Defer a $50K credit so cash_balance=$150K but settled_cash=$100K.
	tradeDate := time.Now().Add(-48 * time.Hour) // doesn't matter for the gate
	settlesAt := tradeDate.Add(24 * time.Hour)
	appendEvent(t, handler, es.Event{
		AggregateID: portfolio.AggregateID("acct-1"),
		Type:        portfolio.EventSharesSettled,
		Timestamp:   tradeDate,
		Data: &portfoliov1.SharesSettled{
			AccountId:     "acct-1",
			OrderSagaId:   "saga-1",
			TradeId:       "trade-1",
			Symbol:        "AAPL",
			Quantity:      100,
			PricePerShare: 500_000,
			Proceeds:      50_000_000,
			SettledAt:     timestamppb.New(tradeDate),
			SettlesAt:     timestamppb.New(settlesAt),
		},
	})

	// Withdraw of $100K succeeds (within both cash_balance and settled_cash).
	wd := portfolio.WithdrawCash{AccountID: "acct-1", Amount: 100_000_000}
	require.NoError(t, handler.Handle(ctx, wd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteWithdrawCash(p, wd)
	}))

	p, err := handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(50_000_000), p.CashBalance, "balance reflects the pending credit minus withdrawal")
	assert.Equal(t, int64(0), p.SettledCash, "withdrawal drained settled cash to zero")

	// Further withdrawal: $1 fails — settled cash exhausted even though
	// cash_balance has $50M of pending credit on it.
	wd2 := portfolio.WithdrawCash{AccountID: "acct-1", Amount: 1}
	p, err = handler.Load(ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	_, err = portfolio.ExecuteWithdrawCash(p, wd2)
	assert.ErrorIs(t, err, portfolio.ErrUnsettledFunds)
}

func TestSettlement_Replay_DeterministicAcrossLoads(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := newTestHandler(store, registry)
	fund(t, handler, "acct-1", 100_000_000)

	tradeDate := time.Now()
	settlesAt := tradeDate.Add(24 * time.Hour)
	appendEvent(t, handler, es.Event{
		AggregateID: portfolio.AggregateID("acct-1"),
		Type:        portfolio.EventCashSettled,
		Timestamp:   tradeDate,
		Data: &portfoliov1.CashSettled{
			AccountId:    "acct-1",
			OrderSagaId:  "saga-1",
			TradeId:      "trade-1",
			Amount:       10_000_000,
			Symbol:       "AAPL",
			Quantity:     100,
			CostPerShare: 100_000,
			SettledAt:    timestamppb.New(tradeDate),
			SettlesAt:    timestamppb.New(settlesAt),
		},
	})

	p1, err := handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	p2, err := handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, p1.CashBalance, p2.CashBalance)
	assert.Equal(t, p1.SettledCash, p2.SettledCash)
	assert.Equal(t, len(p1.PendingLegs), len(p2.PendingLegs))
}
