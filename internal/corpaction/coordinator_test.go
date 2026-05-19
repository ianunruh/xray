package corpaction_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

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

func newCoordinatorEnv(t *testing.T) (*es.Handler[*portfolio.Portfolio], *fakeHolders, *fakeOrders, *fakeSagas, *fakeOrderCanceler, *fakeSagaCanceler, *corpaction.Coordinator) {
	t.Helper()
	registry := es.NewRegistry()
	portfolio.RegisterEvents(registry)
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, slog.Default())

	holders := &fakeHolders{rows: map[string][]string{}}
	orders := &fakeOrders{rows: map[string][]corpaction.OpenOrder{}}
	sagas := &fakeSagas{rows: map[string][]corpaction.SagaInfo{}}
	orderC := &fakeOrderCanceler{}
	sagaC := &fakeSagaCanceler{}
	coord := corpaction.NewCoordinator(handler, holders, orders, sagas, orderC, sagaC)
	return handler, holders, orders, sagas, orderC, sagaC, coord
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
	handler, holders, orders, sagas, orderC, sagaC, coord := newCoordinatorEnv(t)

	depositAndCredit(t, handler, "acct-1", "AAPL", 100, 500_000)
	depositAndCredit(t, handler, "acct-2", "AAPL", 50, 500_000)

	holders.rows["AAPL"] = []string{"acct-1", "acct-2"}
	orders.rows["AAPL"] = []corpaction.OpenOrder{
		{OrderID: "ord-1", Symbol: "AAPL"},
		{OrderID: "ord-2", Symbol: "AAPL"},
		{OrderID: "ord-3", Symbol: "AAPL"},
	}
	sagas.rows["AAPL"] = []corpaction.SagaInfo{
		{SagaID: "saga-1"},
		{SagaID: "saga-2"},
	}

	counts, err := coord.ApplyAction(context.Background(), corpaction.ActionRow{
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
		p, err := handler.Load(context.Background(), portfolio.AggregateID(acct))
		require.NoError(t, err)
		assert.True(t, p.HasAppliedAction("split-1"), "%s marked", acct)
	}
	p1, _ := handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	assert.Equal(t, int64(200), p1.Holdings["AAPL"].Quantity)
	p2, _ := handler.Load(context.Background(), portfolio.AggregateID("acct-2"))
	assert.Equal(t, int64(100), p2.Holdings["AAPL"].Quantity)

	// Cancel reasons carry the action_id so audit trails can link
	// orders back to the action that killed them.
	require.Len(t, orderC.cancelled, 3)
	assert.Contains(t, orderC.cancelled[0].reason, "split-1")
	require.Len(t, sagaC.cancelled, 2)
}

func TestCoordinator_Split_NoHoldersNoOrders(t *testing.T) {
	_, _, _, _, orderC, sagaC, coord := newCoordinatorEnv(t)

	counts, err := coord.ApplyAction(context.Background(), corpaction.ActionRow{
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
	assert.Empty(t, orderC.cancelled)
	assert.Empty(t, sagaC.cancelled)
}

func TestCoordinator_Split_IdempotentOnRetry(t *testing.T) {
	// Coordinator is called twice for the same action — second pass
	// must not double-adjust holdings. (Order/saga cancels are
	// idempotent at the underlying layer; here we focus on the
	// portfolio adjustment path which uses AppliedActions.)
	handler, holders, _, _, _, _, coord := newCoordinatorEnv(t)
	depositAndCredit(t, handler, "acct-1", "AAPL", 100, 500_000)
	holders.rows["AAPL"] = []string{"acct-1"}

	row := corpaction.ActionRow{
		ActionID:         "split-retry",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
	}
	_, err := coord.ApplyAction(context.Background(), row)
	require.NoError(t, err)
	_, err = coord.ApplyAction(context.Background(), row)
	require.NoError(t, err)

	p, err := handler.Load(context.Background(), portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(200), p.Holdings["AAPL"].Quantity, "still 200, not 400")
}

func TestCoordinator_UnsupportedType_ReturnsErr(t *testing.T) {
	_, _, _, _, _, _, coord := newCoordinatorEnv(t)
	_, err := coord.ApplyAction(context.Background(), corpaction.ActionRow{
		ActionID: "div-1",
		Symbol:   "AAPL",
		Type:     corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
	})
	assert.ErrorIs(t, err, corpaction.ErrNotImplemented)
}
