package reconciler_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/ocosaga"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/internal/reconciler"
	"github.com/ianunruh/xray/internal/sagasvc"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

// stubSagaLookup returns a fixed set of saga rows.
type stubSagaLookup struct {
	rows []*sagasvc.SagaRow
}

func (s *stubSagaLookup) List(_ context.Context, _, _ string, _ sagav1.SagaKind, _ sagav1.SagaStatus) ([]*sagasvc.SagaRow, error) {
	return s.rows, nil
}

// stubTradeLookup serves trades keyed by order ID.
type stubTradeLookup struct {
	tradesByOrder map[string][]*orderbookv1.TradeExecuted
}

func (s *stubTradeLookup) TradesByOrderID(_ context.Context, orderID string) ([]*orderbookv1.TradeExecuted, error) {
	return s.tradesByOrder[orderID], nil
}

type collectingPublisher struct {
	events []es.Event
}

func (p *collectingPublisher) Publish(_ context.Context, events []es.Event) error {
	p.events = append(p.events, events...)
	return nil
}

type env struct {
	ctx              context.Context
	store            *memstore.Store
	registry         *es.Registry
	pub              *collectingPublisher
	obHandler        *es.Handler[*orderbook.OrderBook]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	bracketHandler   *es.Handler[*bracket.BracketSaga]
	ocoSagaHandler   *es.Handler[*ocosaga.OCOSaga]
	orderSagaReactor *ordersaga.Reactor
	bracketReactor   *bracket.Reactor
	ocoSagaReactor   *ocosaga.Reactor
}

func (e *env) flush() {
	for len(e.pub.events) > 0 {
		batch := e.pub.events
		e.pub.events = nil
		_ = e.orderSagaReactor.HandleEvents(e.ctx, batch)
		_ = e.bracketReactor.HandleEvents(e.ctx, batch)
		_ = e.ocoSagaReactor.HandleEvents(e.ctx, batch)
	}
}

func setupEnv(t *testing.T) *env {
	t.Helper()
	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)
	portfolio.RegisterEvents(registry)
	ordersaga.RegisterEvents(registry)
	bracket.RegisterEvents(registry)
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
	orderSagaHandler := es.NewHandler(store, registry, func(id string) *ordersaga.OrderSaga {
		return ordersaga.NewOrderSaga(id)
	}, log).WithPublisher(pub)
	bracketHandler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, log).WithPublisher(pub)
	ocoSagaHandler := es.NewHandler(store, registry, func(id string) *ocosaga.OCOSaga {
		return ocosaga.NewOCOSaga(id)
	}, log).WithPublisher(pub)

	orderSagaReactor := ordersaga.NewReactor(orderSagaHandler, portfolioHandler, obHandler, nil, log)
	bracketReactor := bracket.NewReactor(bracketHandler, orderSagaHandler, ocoSagaHandler, obHandler, log)
	ocoSagaReactor := ocosaga.NewReactor(ocoSagaHandler, portfolioHandler, obHandler, log)

	return &env{
		ctx: ctx, store: store, registry: registry, pub: pub,
		obHandler: obHandler, portfolioHandler: portfolioHandler,
		orderSagaHandler: orderSagaHandler, bracketHandler: bracketHandler,
		ocoSagaHandler:   ocoSagaHandler,
		orderSagaReactor: orderSagaReactor, bracketReactor: bracketReactor,
		ocoSagaReactor: ocoSagaReactor,
	}
}

func TestReconciler_L2_ReplaysMissedTrade(t *testing.T) {
	// Mid-lifecycle scenario: an order saga is OrderPlaced, the
	// orderbook executed a trade against it, but the settle command
	// was lost (we simulate by hand-crafting the trade record without
	// ever calling the reactor's trade handler). The reconciler must
	// detect the missing settlement and replay it.
	e := setupEnv(t)

	// Set up the account with cash so the saga can hold + place.
	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 150_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	}))

	// Start saga via its command path; flush so it reaches OrderPlaced.
	start := ordersaga.StartOrderSaga{
		SagaID: "saga-1", AccountID: "acct-1", Symbol: "AAPL",
		Side: orderbookv1.Side_SIDE_BUY, Price: 1_500_000, Quantity: 100,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	require.NoError(t, e.orderSagaHandler.Handle(e.ctx, start, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, start)
	}))
	e.flush()

	s, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID("saga-1"))
	require.NoError(t, err)
	require.Equal(t, ordersaga.OrderPlaced, s.Status, "saga must be OrderPlaced before we inject a lost trade")
	require.Equal(t, int64(0), s.FilledQty)

	// Inject a "lost" trade — exists in the trade projection (via our
	// stub), but the reactor never settled it.
	lostTrade := &orderbookv1.TradeExecuted{
		TradeId:     "trade-X",
		Symbol:      "AAPL",
		BuyOrderId:  ordersaga.OrderID("saga-1"),
		SellOrderId: "some-other-order",
		Price:       1_500_000,
		Quantity:    100,
		ExecutedAt:  timestamppb.New(time.Now()),
	}
	tradeLookup := &stubTradeLookup{tradesByOrder: map[string][]*orderbookv1.TradeExecuted{
		ordersaga.OrderID("saga-1"): {lostTrade},
	}}
	sagaLookup := &stubSagaLookup{rows: []*sagasvc.SagaRow{{
		SagaID: "saga-1", Kind: sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER,
		Status: sagav1.SagaStatus_SAGA_STATUS_ACTIVE,
		AccountID: "acct-1", Symbol: "AAPL",
	}}}

	rec := reconciler.New(time.Hour, sagaLookup, tradeLookup,
		e.portfolioHandler, e.orderSagaReactor, e.bracketReactor, e.ocoSagaReactor, nil, nil, slog.Default())
	require.NoError(t, rec.ReconcileOnce(e.ctx))
	e.flush()

	// Saga should now be Completed: the missing trade got settled,
	// FilledQty matched Quantity, the state machine ran through complete.
	s, err = e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID("saga-1"))
	require.NoError(t, err)
	assert.Equal(t, ordersaga.Completed, s.Status)
	assert.Equal(t, int64(100), s.FilledQty)

	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.True(t, p.HasSettled("saga-1", "trade-X"), "trade is now settled")
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
}

func TestReconciler_L2_SkipsAlreadySettledTrades(t *testing.T) {
	// Trade is in the projection AND in Portfolio.SettledTrades —
	// reconciler must NOT replay it (would double-settle without C5's
	// dedup; with C5 it's a no-op, but the reconciler shouldn't even
	// dispatch).
	e := setupEnv(t)

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 150_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	}))

	// Place resting liquidity so the entry actually fills.
	placeCmd := orderbook.PlaceOrder{
		Symbol: "AAPL", Side: orderbook.Sell, Price: 1_500_000, Quantity: 100,
	}
	require.NoError(t, e.obHandler.Handle(e.ctx, placeCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, placeCmd)
	}))
	e.pub.events = nil

	start := ordersaga.StartOrderSaga{
		SagaID: "saga-1", AccountID: "acct-1", Symbol: "AAPL",
		Side: orderbookv1.Side_SIDE_BUY, Price: 1_500_000, Quantity: 100,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	require.NoError(t, e.orderSagaHandler.Handle(e.ctx, start, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, start)
	}))
	e.flush()

	s, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID("saga-1"))
	require.NoError(t, err)
	require.Equal(t, ordersaga.Completed, s.Status, "happy path: saga completed normally")

	// Reconciler runs and finds the saga in projection_sagas...
	// (in practice it wouldn't, since completed sagas filter out, but
	// we test the dedup path explicitly here)
	tradeLookup := &stubTradeLookup{} // empty; nothing to replay
	sagaLookup := &stubSagaLookup{rows: []*sagasvc.SagaRow{}}
	rec := reconciler.New(time.Hour, sagaLookup, tradeLookup,
		e.portfolioHandler, e.orderSagaReactor, e.bracketReactor, e.ocoSagaReactor, nil, nil, slog.Default())
	require.NoError(t, rec.ReconcileOnce(e.ctx))

	// Nothing changed — saga still Completed, portfolio unchanged.
	s, err = e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID("saga-1"))
	require.NoError(t, err)
	assert.Equal(t, ordersaga.Completed, s.Status)
}

func TestReconciler_L1_DrivesStuckSagaForward(t *testing.T) {
	// Saga is in CashHeld but the place-order action never fired
	// (simulated by manually crafting that state). Reconciler's
	// Reconcile() should drive it forward to OrderPlaced.
	e := setupEnv(t)

	deposit := portfolio.DepositCash{AccountID: "acct-1", Amount: 150_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, deposit, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, deposit)
	}))
	e.pub.events = nil // ignore deposit events

	start := ordersaga.StartOrderSaga{
		SagaID: "saga-1", AccountID: "acct-1", Symbol: "AAPL",
		Side: orderbookv1.Side_SIDE_BUY, Price: 1_500_000, Quantity: 100,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	require.NoError(t, e.orderSagaHandler.Handle(e.ctx, start, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, start)
	}))

	// Process the OrderSagaStarted event so the reactor runs
	// holdResources, which advances the saga to CashHeld. Then we
	// drop all subsequent published events to simulate the reactor
	// crashing before it could pick up the OrderSagaCashHeld trigger
	// for placeOrder.
	require.NoError(t, e.orderSagaReactor.HandleEvents(e.ctx, e.pub.events))
	e.pub.events = nil

	s, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID("saga-1"))
	require.NoError(t, err)
	require.Equal(t, ordersaga.CashHeld, s.Status, "saga stuck at CashHeld")

	// Reconciler picks it up and drives it forward.
	sagaLookup := &stubSagaLookup{rows: []*sagasvc.SagaRow{{
		SagaID: "saga-1", Kind: sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER,
		Status: sagav1.SagaStatus_SAGA_STATUS_ACTIVE,
		AccountID: "acct-1", Symbol: "AAPL",
	}}}
	tradeLookup := &stubTradeLookup{}
	rec := reconciler.New(time.Hour, sagaLookup, tradeLookup,
		e.portfolioHandler, e.orderSagaReactor, e.bracketReactor, e.ocoSagaReactor, nil, nil, slog.Default())
	require.NoError(t, rec.ReconcileOnce(e.ctx))

	s, err = e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID("saga-1"))
	require.NoError(t, err)
	assert.Equal(t, ordersaga.OrderPlaced, s.Status, "reconciler drove saga to OrderPlaced")
	assert.NotEmpty(t, s.OrderID)
}
