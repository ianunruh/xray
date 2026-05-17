package feesaccruer_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ianunruh/xray/internal/feesaccruer"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

type fakeMarker struct{ prices map[string]int64 }

func (f *fakeMarker) GetMarkPrice(sym string) (int64, time.Time, bool) {
	p, ok := f.prices[sym]
	return p, time.Now(), ok
}

// trackerPublisher forwards published events to the
// AccruableAccountsTracker synchronously so the test doesn't have to
// run a real consumer.
type trackerPublisher struct {
	tracker *portfolio.InMemoryAccruableAccounts
}

func (p *trackerPublisher) Publish(ctx context.Context, events []es.Event) error {
	return p.tracker.HandleEvents(ctx, events)
}

type env struct {
	ctx     context.Context
	handler *es.Handler[*portfolio.Portfolio]
	tracker *portfolio.InMemoryAccruableAccounts
	marker  *fakeMarker
}

func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	store := memstore.New()
	registry := es.NewRegistry()
	portfolio.RegisterEvents(registry)
	tracker := portfolio.NewInMemoryAccruableAccounts()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, logger).WithPublisher(&trackerPublisher{tracker: tracker})

	return &env{ctx: ctx, handler: handler, tracker: tracker, marker: &fakeMarker{prices: map[string]int64{}}}
}

func (e *env) deposit(t *testing.T, accountID string, amount int64) {
	t.Helper()
	cmd := portfolio.DepositCash{AccountID: accountID, Amount: amount}
	require.NoError(t, e.handler.Handle(e.ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, cmd)
	}))
}

// openShort issues HoldCollateral + OpenShort. Amount returned in
// proceeds = qty * price; collateral required = price * qty * 50%.
func (e *env) openShort(t *testing.T, accountID, sagaID, symbol string, qty, price int64) {
	t.Helper()
	collateral := price * qty / 2
	hold := portfolio.HoldCollateral{
		AccountID: accountID, OrderSagaID: sagaID, Symbol: symbol,
		Quantity: qty, Amount: collateral,
	}
	require.NoError(t, e.handler.Handle(e.ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, hold)
	}))
	open := portfolio.OpenShort{
		AccountID: accountID, OrderSagaID: sagaID, Symbol: symbol,
		TradeID: "trade-" + sagaID, Quantity: qty, PricePerShare: price,
	}
	require.NoError(t, e.handler.Handle(e.ctx, open, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteOpenShort(p, open)
	}))
}

func (e *env) load(t *testing.T, accountID string) *portfolio.Portfolio {
	t.Helper()
	p, err := e.handler.Load(e.ctx, portfolio.AggregateID(accountID))
	require.NoError(t, err)
	return p
}

func (e *env) newAccruer(t *testing.T, cfg feesaccruer.Config) *feesaccruer.Accruer {
	t.Helper()
	return feesaccruer.NewAccruer(e.handler, e.tracker, e.marker, time.Now,
		cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestAccruer_NoLiabilities_NoOp(t *testing.T) {
	e := newEnv(t)
	a := e.newAccruer(t, feesaccruer.Config{Interval: time.Hour})
	require.NoError(t, a.Tick(e.ctx, time.Now()))
}

func TestAccruer_ChargesBorrowFeeOnOpenShort(t *testing.T) {
	e := newEnv(t)
	e.deposit(t, "acct-1", 100_000_000_000) // $10M
	e.openShort(t, "acct-1", "saga-1", "AAPL", 100, 1_500_000)
	e.marker.prices["AAPL"] = 1_500_000

	pBefore := e.load(t, "acct-1")
	cashBefore := pBefore.CashBalance
	require.False(t, pBefore.LastAccruedAt.IsZero(), "clock should be seeded by first event")

	a := e.newAccruer(t, feesaccruer.Config{Interval: time.Hour})
	tickAt := pBefore.LastAccruedAt.Add(time.Hour)
	require.NoError(t, a.Tick(e.ctx, tickAt))

	p := e.load(t, "acct-1")
	// 1h of 3% APR on 100*1.5M = 150M notional ≈ 150M * 0.03 / 8760 ≈ 514.
	// (Cash went down by that much.)
	loss := cashBefore - p.CashBalance
	assert.InDelta(t, 514, loss, 50, "1h of borrow fee on $15k notional")
	assert.Equal(t, tickAt.UTC(), p.LastAccruedAt.UTC(), "clock should advance to tick time")
}

func TestAccruer_DoubleTickAtSameTime_NoDoubleCharge(t *testing.T) {
	e := newEnv(t)
	e.deposit(t, "acct-1", 100_000_000_000)
	e.openShort(t, "acct-1", "saga-1", "AAPL", 100, 1_500_000)
	e.marker.prices["AAPL"] = 1_500_000

	tickAt := e.load(t, "acct-1").LastAccruedAt.Add(time.Hour)
	a := e.newAccruer(t, feesaccruer.Config{Interval: time.Hour})
	require.NoError(t, a.Tick(e.ctx, tickAt))
	cashAfter := e.load(t, "acct-1").CashBalance

	require.NoError(t, a.Tick(e.ctx, tickAt))
	assert.Equal(t, cashAfter, e.load(t, "acct-1").CashBalance,
		"second tick at same wall-time must not charge again")
}

func TestAccruer_SkipsBelowMinElapsed(t *testing.T) {
	e := newEnv(t)
	e.deposit(t, "acct-1", 100_000_000_000)
	e.openShort(t, "acct-1", "saga-1", "AAPL", 100, 1_500_000)
	e.marker.prices["AAPL"] = 1_500_000

	cashBefore := e.load(t, "acct-1").CashBalance
	tickAt := e.load(t, "acct-1").LastAccruedAt.Add(10 * time.Minute)

	a := e.newAccruer(t, feesaccruer.Config{Interval: time.Hour, MinElapsed: 30 * time.Minute})
	require.NoError(t, a.Tick(e.ctx, tickAt))

	p := e.load(t, "acct-1")
	assert.Equal(t, cashBefore, p.CashBalance, "no charge when elapsed < MinElapsed")
}

func TestAccruer_NoMarkForShort_SkipsThatSymbol(t *testing.T) {
	e := newEnv(t)
	e.deposit(t, "acct-1", 100_000_000_000)
	e.openShort(t, "acct-1", "saga-1", "AAPL", 100, 1_500_000)
	// Intentionally no mark for AAPL.

	cashBefore := e.load(t, "acct-1").CashBalance
	tickAt := e.load(t, "acct-1").LastAccruedAt.Add(time.Hour)

	a := e.newAccruer(t, feesaccruer.Config{Interval: time.Hour})
	require.NoError(t, a.Tick(e.ctx, tickAt))

	// Cash unchanged (no fee charged) but clock advanced via the
	// zero-amount MarginInterestAccrued event.
	p := e.load(t, "acct-1")
	assert.Equal(t, cashBefore, p.CashBalance, "no fee without a mark")
	assert.Equal(t, tickAt.UTC(), p.LastAccruedAt.UTC(), "clock advances even with zero amounts")
}
