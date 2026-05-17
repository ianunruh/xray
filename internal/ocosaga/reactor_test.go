package ocosaga_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/ocosaga"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

type collectingPublisher struct {
	events []es.Event
}

func (p *collectingPublisher) Publish(_ context.Context, events []es.Event) error {
	p.events = append(p.events, events...)
	return nil
}

type env struct {
	ctx              context.Context
	pub              *collectingPublisher
	obHandler        *es.Handler[*orderbook.OrderBook]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	ocoSagaHandler   *es.Handler[*ocosaga.OCOSaga]
	ocoSagaReactor   *ocosaga.Reactor
}

func (e *env) flush() {
	for len(e.pub.events) > 0 {
		batch := e.pub.events
		e.pub.events = nil
		_ = e.ocoSagaReactor.HandleEvents(e.ctx, batch)
	}
}

func setupEnv(t *testing.T) *env {
	t.Helper()
	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)
	portfolio.RegisterEvents(registry)
	ocosaga.RegisterEvents(registry)

	store := memstore.New()
	ctx := context.Background()
	pub := &collectingPublisher{}
	log := slog.Default()

	obHandler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, log).WithPublisher(pub)
	portfolioHandler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, log).WithPublisher(pub)
	ocoSagaHandler := es.NewHandler(store, registry, func(id string) *ocosaga.OCOSaga {
		return ocosaga.NewOCOSaga(id)
	}, log).WithPublisher(pub)

	ocoSagaReactor := ocosaga.NewReactor(ocoSagaHandler, portfolioHandler, obHandler, log)
	return &env{
		ctx: ctx, pub: pub,
		obHandler: obHandler, portfolioHandler: portfolioHandler,
		ocoSagaHandler: ocoSagaHandler, ocoSagaReactor: ocoSagaReactor,
	}
}

func TestOCOSaga_TakeProfitFills(t *testing.T) {
	// Account holds 100 shares; OCO sells them with TP $155 / SL $145.
	// An aggressive buyer at $155 fills the TP; SL is atomically
	// cancelled at the orderbook layer.
	e := setupEnv(t)

	// Seed the account with 100 shares owned at cost basis 1500000.
	depositCmd := portfolio.DepositCash{AccountID: "acct-1", Amount: 1_500_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, depositCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, depositCmd)
	}))
	creditCmd := portfolio.CreditShares{
		AccountID: "acct-1", Symbol: "AAPL",
		Quantity: 100, CostPerShare: 1_500_000,
	}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, creditCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCreditShares(p, creditCmd)
	}))
	e.pub.events = nil

	start := ocosaga.StartOCOSaga{
		SagaID: "oco-1", AccountID: "acct-1", Symbol: "AAPL",
		ExitSide:        orderbookv1.Side_SIDE_SELL,
		Quantity:        100,
		TakeProfitPrice: 1_550_000,
		StopLossPrice:   1_450_000,
	}
	require.NoError(t, e.ocoSagaHandler.Handle(e.ctx, start, func(s *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteStartOCOSaga(s, start)
	}))
	e.flush()

	saga, err := e.ocoSagaHandler.Load(e.ctx, ocosaga.AggregateID("oco-1"))
	require.NoError(t, err)
	require.Equal(t, ocosaga.ExitPlaced, saga.Status, "exits placed and reactor advanced to ExitPlaced")

	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	require.Equal(t, int64(100), p.SharesHeld["AAPL"], "100 shares held for the OCO")

	// Aggressor buy at TP price triggers the take-profit; orderbook OCO
	// cancels the SL atomically (no race window).
	buyCmd := orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Buy, Price: 1_550_000, Quantity: 100, OrderType: orderbook.Limit,
	}
	require.NoError(t, e.obHandler.Handle(e.ctx, buyCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, buyCmd)
	}))
	e.flush()

	saga, err = e.ocoSagaHandler.Load(e.ctx, ocosaga.AggregateID("oco-1"))
	require.NoError(t, err)
	assert.Equal(t, ocosaga.Completed, saga.Status)
	assert.Equal(t, int64(100), saga.SettledQty)

	p, err = e.portfolioHandler.Load(e.ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Empty(t, p.SharesHeld, "shares fully settled, hold drained")
	assert.Nil(t, p.Holdings["AAPL"], "100 shares fully sold")
	// Starting cash 1.5B + 155M from the TP fill.
	assert.Equal(t, int64(1_655_000_000), p.CashBalance)
}

func TestOCOSaga_Cancelled_ReleasesShares(t *testing.T) {
	// User cancels the OCO mid-flight; reactor must release the share hold.
	e := setupEnv(t)

	depositCmd := portfolio.DepositCash{AccountID: "acct-1", Amount: 1_500_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, depositCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, depositCmd)
	}))
	creditCmd := portfolio.CreditShares{
		AccountID: "acct-1", Symbol: "AAPL", Quantity: 100, CostPerShare: 1_500_000,
	}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, creditCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteCreditShares(p, creditCmd)
	}))
	e.pub.events = nil

	start := ocosaga.StartOCOSaga{
		SagaID: "oco-1", AccountID: "acct-1", Symbol: "AAPL",
		ExitSide: orderbookv1.Side_SIDE_SELL, Quantity: 100,
		TakeProfitPrice: 1_550_000, StopLossPrice: 1_450_000,
	}
	require.NoError(t, e.ocoSagaHandler.Handle(e.ctx, start, func(s *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteStartOCOSaga(s, start)
	}))
	e.flush()

	require.Equal(t, ocosaga.ExitPlaced,
		mustLoadSaga(t, e, "oco-1").Status)

	// User-initiated failure (what sagasvc.cancelOCO would emit).
	failCmd := ocosaga.RecordFailed{SagaID: "oco-1", Reason: "user cancelled"}
	require.NoError(t, e.ocoSagaHandler.Handle(e.ctx, failCmd, func(s *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteRecordFailed(s, failCmd)
	}))
	e.flush()

	assert.Equal(t, ocosaga.Failed, mustLoadSaga(t, e, "oco-1").Status)

	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Empty(t, p.SharesHeld, "share hold released after failure")
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity, "shares still owned")
}

func mustLoadSaga(t *testing.T, e *env, sagaID string) *ocosaga.OCOSaga {
	t.Helper()
	s, err := e.ocoSagaHandler.Load(e.ctx, ocosaga.AggregateID(sagaID))
	require.NoError(t, err)
	return s
}

// openShort is a helper that seeds an open short position for the OCO
// short-cover tests. Uses the portfolio aggregate directly rather than
// running a full ordersaga.
func openShort(t *testing.T, e *env, accountID, symbol string, qty, price, collateral int64) {
	t.Helper()
	holdCmd := portfolio.HoldCollateral{
		AccountID: accountID, OrderSagaID: "seed-" + symbol,
		Symbol: symbol, Quantity: qty, Amount: collateral,
	}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, holdCmd)
	}))
	openCmd := portfolio.OpenShort{
		AccountID: accountID, OrderSagaID: "seed-" + symbol, TradeID: "seed-trade-" + symbol,
		Symbol: symbol, Quantity: qty, PricePerShare: price,
	}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, openCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteOpenShort(p, openCmd)
	}))
}

func TestOCOSaga_ShortCover_TakeProfitFills(t *testing.T) {
	// Account has an open short of 100 AAPL @ $150. OCO covers with
	// TP $120 (profit, buy at low) / SL $180 (stop, buy at high).
	// An aggressive seller hits the TP buy at $120.
	e := setupEnv(t)

	depositCmd := portfolio.DepositCash{AccountID: "acct-1", Amount: 1_000_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, depositCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, depositCmd)
	}))
	openShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.pub.events = nil

	start := ocosaga.StartOCOSaga{
		SagaID: "oco-1", AccountID: "acct-1", Symbol: "AAPL",
		ExitSide:        orderbookv1.Side_SIDE_BUY,
		Quantity:        100,
		TakeProfitPrice: 1_200_000,
		StopLossPrice:   1_800_000,
		PositionSide:    orderbookv1.PositionSide_POSITION_SIDE_SHORT,
	}
	require.NoError(t, e.ocoSagaHandler.Handle(e.ctx, start, func(s *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteStartOCOSaga(s, start)
	}))
	e.flush()

	saga := mustLoadSaga(t, e, "oco-1")
	require.Equal(t, ocosaga.ExitPlaced, saga.Status)

	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(100), p.ShortCoversHeld["AAPL"], "100 short-cover capacity held")

	// Aggressor sell at TP price (cover buy fills) triggers TP; SL cancelled.
	sellCmd := orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1_200_000, Quantity: 100, OrderType: orderbook.Limit,
	}
	require.NoError(t, e.obHandler.Handle(e.ctx, sellCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, sellCmd)
	}))
	e.flush()

	saga = mustLoadSaga(t, e, "oco-1")
	assert.Equal(t, ocosaga.Completed, saga.Status)

	p, err = e.portfolioHandler.Load(e.ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Empty(t, p.ShortPositions, "short fully covered")
	assert.Empty(t, p.ShortCoversHeld, "cover capacity drained")
	// Realized PnL = (1500000 - 1200000) * 100 = 30M.
	// Cash = $100k deposit + $3000 profit = $103,000.
	assert.Equal(t, int64(1_030_000_000), p.CashBalance)
}

func TestOCOSaga_ShortCover_Cancelled_ReleasesCapacity(t *testing.T) {
	e := setupEnv(t)

	depositCmd := portfolio.DepositCash{AccountID: "acct-1", Amount: 1_000_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, depositCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, depositCmd)
	}))
	openShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.pub.events = nil

	start := ocosaga.StartOCOSaga{
		SagaID: "oco-1", AccountID: "acct-1", Symbol: "AAPL",
		ExitSide: orderbookv1.Side_SIDE_BUY, Quantity: 100,
		TakeProfitPrice: 1_200_000, StopLossPrice: 1_800_000,
		PositionSide: orderbookv1.PositionSide_POSITION_SIDE_SHORT,
	}
	require.NoError(t, e.ocoSagaHandler.Handle(e.ctx, start, func(s *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteStartOCOSaga(s, start)
	}))
	e.flush()
	require.Equal(t, ocosaga.ExitPlaced, mustLoadSaga(t, e, "oco-1").Status)

	failCmd := ocosaga.RecordFailed{SagaID: "oco-1", Reason: "user cancelled"}
	require.NoError(t, e.ocoSagaHandler.Handle(e.ctx, failCmd, func(s *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteRecordFailed(s, failCmd)
	}))
	e.flush()

	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Empty(t, p.ShortCoversHeld, "short-cover capacity released")
	assert.Equal(t, int64(100), p.ShortPositions["AAPL"].Quantity, "short still open")
}
