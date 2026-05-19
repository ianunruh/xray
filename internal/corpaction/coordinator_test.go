package corpaction_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/internal/corpaction"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

type fakeHolders struct{ rows map[string][]string }

func (f *fakeHolders) HoldersOfSymbol(_ context.Context, sym string) ([]string, error) {
	return f.rows[sym], nil
}

type fakeHoldings struct{ rows map[string][]corpaction.HolderShares }

func (f *fakeHoldings) HoldingsForSymbol(_ context.Context, sym string) ([]corpaction.HolderShares, error) {
	return f.rows[sym], nil
}

type fakeDividendStore struct {
	snapshots map[string][]corpaction.HolderShares
}

func newFakeDividendStore() *fakeDividendStore {
	return &fakeDividendStore{snapshots: map[string][]corpaction.HolderShares{}}
}

func (f *fakeDividendStore) SaveSnapshot(_ context.Context, actionID string, holders []corpaction.HolderShares, _ time.Time) (int32, error) {
	if _, exists := f.snapshots[actionID]; exists {
		return 0, nil // idempotent re-snapshot
	}
	dup := make([]corpaction.HolderShares, len(holders))
	copy(dup, holders)
	f.snapshots[actionID] = dup
	return int32(len(dup)), nil
}

func (f *fakeDividendStore) LoadSnapshot(_ context.Context, actionID string) ([]corpaction.HolderShares, error) {
	return f.snapshots[actionID], nil
}

type fakeOrders struct{ rows map[string][]corpaction.OpenOrder }

func (f *fakeOrders) OpenOrdersBySymbol(sym string) []corpaction.OpenOrder { return f.rows[sym] }

type fakeSagas struct{ rows map[string][]corpaction.SagaInfo }

func (f *fakeSagas) ActiveSagasBySymbol(_ context.Context, sym string) ([]corpaction.SagaInfo, error) {
	return f.rows[sym], nil
}

type fakeOrderCanceler struct {
	mu        sync.Mutex
	cancelled []struct {
		symbol, orderID, reason string
	}
}

func (f *fakeOrderCanceler) CancelOrder(_ context.Context, symbol, orderID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, struct {
		symbol, orderID, reason string
	}{symbol, orderID, reason})
	return nil
}

type fakeSagaCanceler struct {
	mu        sync.Mutex
	cancelled []string
}

func (f *fakeSagaCanceler) CancelByID(_ context.Context, sagaID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, sagaID)
	return nil
}

type coordEnv struct {
	handler   *es.Handler[*portfolio.Portfolio]
	holders   *fakeHolders
	holdings  *fakeHoldings
	orders    *fakeOrders
	sagas     *fakeSagas
	orderC    *fakeOrderCanceler
	sagaC     *fakeSagaCanceler
	divStore  *fakeDividendStore
	coord     *corpaction.Coordinator
}

func newCoordinatorEnv(t *testing.T) *coordEnv {
	t.Helper()
	registry := es.NewRegistry()
	portfolio.RegisterEvents(registry)
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, slog.Default())

	e := &coordEnv{
		handler:  handler,
		holders:  &fakeHolders{rows: map[string][]string{}},
		holdings: &fakeHoldings{rows: map[string][]corpaction.HolderShares{}},
		orders:   &fakeOrders{rows: map[string][]corpaction.OpenOrder{}},
		sagas:    &fakeSagas{rows: map[string][]corpaction.SagaInfo{}},
		orderC:   &fakeOrderCanceler{},
		sagaC:    &fakeSagaCanceler{},
		divStore: newFakeDividendStore(),
	}
	e.coord = corpaction.NewCoordinator(handler, e.holders, e.holdings, e.orders, e.sagas, e.orderC, e.sagaC, e.divStore)
	return e
}

func depositAndCredit(t *testing.T, h *es.Handler[*portfolio.Portfolio], account, symbol string, qty, cost int64) {
	t.Helper()
	dep := portfolio.DepositCash{AccountID: account, Amount: 1_000_000_000}
	require.NoError(t, h.Handle(context.Background(), dep, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, dep)
	}))
	cr := portfolio.CreditShares{AccountID: account, Symbol: symbol, Quantity: qty, CostPerShare: cost}
	require.NoError(t, h.Handle(context.Background(), cr, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCreditShares(p, cr)
	}))
}

func TestCoordinator_Split_FansOutAcrossAggregates(t *testing.T) {
	e := newCoordinatorEnv(t)

	depositAndCredit(t, e.handler, "acct-1", "AAPL", 100, 500_000)
	depositAndCredit(t, e.handler, "acct-2", "AAPL", 50, 500_000)

	e.holders.rows["AAPL"] = []string{"acct-1", "acct-2"}
	e.orders.rows["AAPL"] = []corpaction.OpenOrder{
		{OrderID: "ord-1", Symbol: "AAPL"},
		{OrderID: "ord-2", Symbol: "AAPL"},
		{OrderID: "ord-3", Symbol: "AAPL"},
	}
	e.sagas.rows["AAPL"] = []corpaction.SagaInfo{
		{SagaID: "saga-1"},
		{SagaID: "saga-2"},
	}

	counts, err := e.coord.ApplyAction(context.Background(), corpaction.ActionRow{
		ActionID:         "split-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
	})
	require.NoError(t, err)

	assert.Equal(t, int32(2), counts.Holders, "fan-out touched 2 portfolios")
	assert.Equal(t, int32(3), counts.Orders, "cancelled 3 orders")
	assert.Equal(t, int32(2), counts.Sagas, "cancelled 2 sagas")

	// Both portfolios got AdjustHolding events with the expected ratio.
	for _, acct := range []string{"acct-1", "acct-2"} {
		p, err := e.handler.Load(context.Background(), portfolio.AggregateID(acct))
		require.NoError(t, err)
		assert.True(t, p.HasAppliedAction("split-1"), "%s marked", acct)
	}
	p1, _ := e.handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	assert.Equal(t, int64(200), p1.Holdings["AAPL"].Quantity)
	p2, _ := e.handler.Load(context.Background(), portfolio.AggregateID("acct-2"))
	assert.Equal(t, int64(100), p2.Holdings["AAPL"].Quantity)

	// Cancel reasons carry the action_id so audit trails can link
	// orders back to the action that killed them.
	require.Len(t, e.orderC.cancelled, 3)
	assert.Contains(t, e.orderC.cancelled[0].reason, "split-1")
	require.Len(t, e.sagaC.cancelled, 2)
}

func TestCoordinator_Split_NoHoldersNoOrders(t *testing.T) {
	e := newCoordinatorEnv(t)

	counts, err := e.coord.ApplyAction(context.Background(), corpaction.ActionRow{
		ActionID:         "split-empty",
		Symbol:           "ZZZZ",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
	})
	require.NoError(t, err)
	assert.Zero(t, counts.Holders)
	assert.Zero(t, counts.Orders)
	assert.Zero(t, counts.Sagas)
	assert.Empty(t, e.orderC.cancelled)
	assert.Empty(t, e.sagaC.cancelled)
}

func TestCoordinator_Split_IdempotentOnRetry(t *testing.T) {
	// Coordinator is called twice for the same action — second pass
	// must not double-adjust holdings. (Order/saga cancels are
	// idempotent at the underlying layer; here we focus on the
	// portfolio adjustment path which uses AppliedActions.)
	e := newCoordinatorEnv(t)
	depositAndCredit(t, e.handler, "acct-1", "AAPL", 100, 500_000)
	e.holders.rows["AAPL"] = []string{"acct-1"}

	row := corpaction.ActionRow{
		ActionID:         "split-retry",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
	}
	_, err := e.coord.ApplyAction(context.Background(), row)
	require.NoError(t, err)
	_, err = e.coord.ApplyAction(context.Background(), row)
	require.NoError(t, err)

	p, err := e.handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(200), p.Holdings["AAPL"].Quantity, "still 200, not 400")
}

func TestCoordinator_UnsupportedType_ReturnsErr(t *testing.T) {
	e := newCoordinatorEnv(t)
	_, err := e.coord.ApplyAction(context.Background(), corpaction.ActionRow{
		ActionID:  "rename-1",
		Symbol:    "AAPL",
		Type:      corpactionv1.ActionType_ACTION_TYPE_SYMBOL_CHANGE,
		NewSymbol: "MSFT",
	})
	assert.ErrorIs(t, err, corpaction.ErrNotImplemented)
}

func TestCoordinator_Dividend_SnapshotAndApply(t *testing.T) {
	e := newCoordinatorEnv(t)

	depositAndCredit(t, e.handler, "acct-1", "AAPL", 100, 500_000)
	depositAndCredit(t, e.handler, "acct-2", "AAPL", 50, 500_000)

	e.holdings.rows["AAPL"] = []corpaction.HolderShares{
		{AccountID: "acct-1", Shares: 100},
		{AccountID: "acct-2", Shares: 50},
	}

	action := corpaction.ActionRow{
		ActionID:         "div-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 2400, // $0.24
	}

	// 1. Record-date snapshot.
	count, err := e.coord.SnapshotDividendHolders(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, int32(2), count)
	assert.Len(t, e.divStore.snapshots["div-1"], 2)

	// 2. Pay-date apply.
	counts, err := e.coord.ApplyAction(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, int32(2), counts.Holders)

	// acct-1 got 100 * 2400 = 240000, acct-2 got 50 * 2400 = 120000
	p1, _ := e.handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	assert.Equal(t, int64(1_000_000_000+240_000), p1.CashBalance, "100 shares * $0.24")
	assert.Equal(t, p1.CashBalance, p1.SettledCash, "dividends settle instantly")
	p2, _ := e.handler.Load(context.Background(), portfolio.AggregateID("acct-2"))
	assert.Equal(t, int64(1_000_000_000+120_000), p2.CashBalance, "50 shares * $0.24")
}

func TestCoordinator_Dividend_SoldAfterRecordDate(t *testing.T) {
	// Account holds 100 shares at record-date (in the snapshot), then
	// sells some after. Pay-date credit is based on the snapshot, not
	// live holdings — that's the whole point of record_date.
	e := newCoordinatorEnv(t)

	depositAndCredit(t, e.handler, "acct-1", "AAPL", 100, 500_000)
	e.holdings.rows["AAPL"] = []corpaction.HolderShares{
		{AccountID: "acct-1", Shares: 100},
	}

	action := corpaction.ActionRow{
		ActionID:         "div-2",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 1000,
	}
	_, err := e.coord.SnapshotDividendHolders(context.Background(), action)
	require.NoError(t, err)

	// Simulate post-record-date sell by zeroing live holdings.
	e.holdings.rows["AAPL"] = nil

	counts, err := e.coord.ApplyAction(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, int32(1), counts.Holders, "still credits the record-date holder")
	p, _ := e.handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	assert.Equal(t, int64(1_000_000_000+100_000), p.CashBalance, "got 100 * $0.10 from snapshot")
}

func TestCoordinator_Dividend_Idempotent(t *testing.T) {
	e := newCoordinatorEnv(t)
	depositAndCredit(t, e.handler, "acct-1", "AAPL", 100, 500_000)
	e.holdings.rows["AAPL"] = []corpaction.HolderShares{
		{AccountID: "acct-1", Shares: 100},
	}

	action := corpaction.ActionRow{
		ActionID:         "div-3",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 1000,
	}
	_, _ = e.coord.SnapshotDividendHolders(context.Background(), action)
	_, _ = e.coord.ApplyAction(context.Background(), action)
	_, _ = e.coord.ApplyAction(context.Background(), action) // retry

	p, _ := e.handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	assert.Equal(t, int64(1_000_000_000+100_000), p.CashBalance, "only credited once")
}
