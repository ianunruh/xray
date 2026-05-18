package twapsaga_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/internal/twapsaga"
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
	store            *memstore.Store
	registry         *es.Registry
	pub              *collectingPublisher
	obHandler        *es.Handler[*orderbook.OrderBook]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	twapHandler      *es.Handler[*twapsaga.TWAPSaga]
	orderSagaReactor *ordersaga.Reactor
	twapReactor      *twapsaga.Reactor
	clock            *fakeClock
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func (e *env) flush() {
	for len(e.pub.events) > 0 {
		batch := e.pub.events
		e.pub.events = nil
		_ = e.orderSagaReactor.HandleEvents(e.ctx, batch)
		_ = e.twapReactor.HandleEvents(e.ctx, batch)
	}
}

func setupEnv(t *testing.T) *env {
	t.Helper()

	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)
	portfolio.RegisterEvents(registry)
	ordersaga.RegisterEvents(registry)
	twapsaga.RegisterEvents(registry)

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
	twapHandler := es.NewHandler(store, registry, func(id string) *twapsaga.TWAPSaga {
		return twapsaga.NewTWAPSaga(id)
	}, log).WithPublisher(pub)

	orderSagaReactor := ordersaga.NewReactor(orderSagaHandler, portfolioHandler, obHandler, nil, log)
	// Park the fake clock 10ms ahead of wall time. That's enough to win
	// the race with ExecuteStartTWAPSaga's internal time.Now (so slice 0
	// is due immediately) but not so far that future slices look due
	// before the test advances explicitly.
	clock := &fakeClock{now: time.Now().Add(10 * time.Millisecond)}
	twapReactor := twapsaga.NewReactor(twapHandler, orderSagaHandler, log).WithClock(clock.Now)

	return &env{
		ctx:              ctx,
		store:            store,
		registry:         registry,
		pub:              pub,
		obHandler:        obHandler,
		portfolioHandler: portfolioHandler,
		orderSagaHandler: orderSagaHandler,
		twapHandler:      twapHandler,
		orderSagaReactor: orderSagaReactor,
		twapReactor:      twapReactor,
		clock:            clock,
	}
}

func deposit(t *testing.T, e *env, accountID string, amount int64) {
	t.Helper()
	cmd := portfolio.DepositCash{AccountID: accountID, Amount: amount}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, cmd)
	}))
}

func placeLimitOrder(t *testing.T, e *env, symbol string, side orderbook.Side, price, qty int64) {
	t.Helper()
	cmd := orderbook.PlaceOrder{
		Symbol:   symbol,
		Side:     side,
		Price:    price,
		Quantity: qty,
	}
	require.NoError(t, e.obHandler.Handle(e.ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, cmd)
	}))
}

func startTWAP(t *testing.T, e *env, accountID, sagaID, symbol string, side orderbookv1.Side, totalQty int64, sliceCount int32, intervalMs, limitPrice int64) {
	t.Helper()
	cmd := twapsaga.StartTWAPSaga{
		SagaID:          sagaID,
		AccountID:       accountID,
		Symbol:          symbol,
		Side:            side,
		TotalQuantity:   totalQty,
		SliceCount:      sliceCount,
		SliceIntervalMs: intervalMs,
		LimitPrice:      limitPrice,
	}
	require.NoError(t, e.twapHandler.Handle(e.ctx, cmd, func(s *twapsaga.TWAPSaga) ([]es.Event, error) {
		return twapsaga.ExecuteStartTWAPSaga(s, cmd)
	}))
}

func loadTWAP(t *testing.T, e *env, sagaID string) *twapsaga.TWAPSaga {
	t.Helper()
	s, err := e.twapHandler.Load(e.ctx, twapsaga.AggregateID(sagaID))
	require.NoError(t, err)
	return s
}

func TestTWAP_HappyPath_AllSlicesFill(t *testing.T) {
	// 3 slices of 10 shares each at $150 limit, 1s between slices.
	// Plenty of resting liquidity at $150 — every IOC slice should fill.
	e := setupEnv(t)
	deposit(t, e, "acct-1", 100_000_000) // $10k, more than enough

	// Resting sell stack: 100 @ $150 covers all three slices.
	placeLimitOrder(t, e, "AAPL", orderbook.Sell, 1500000, 100)
	e.pub.events = nil

	startTWAP(t, e, "acct-1", "tw-1", "AAPL", orderbookv1.Side_SIDE_BUY,
		30, 3, 1000, 1500000)
	e.flush()

	// Slice 0 launched immediately (start == now).
	s := loadTWAP(t, e, "tw-1")
	require.Equal(t, twapsaga.Active, s.Status, "should be Active after first slice")
	require.Equal(t, int32(1), s.SlicesLaunched())
	require.True(t, s.Slices[0].Completed, "IOC slice 0 should be terminal after flush")
	require.Equal(t, int64(10), s.Slices[0].FilledQuantity)
	require.Equal(t, int64(10), s.TotalFilled)

	// Advance clock past slice 1's due time → reconciler tick spawns it.
	e.clock.Advance(1100 * time.Millisecond)
	require.NoError(t, e.twapReactor.Reconcile(e.ctx, "tw-1"))
	e.flush()

	s = loadTWAP(t, e, "tw-1")
	require.Equal(t, int32(2), s.SlicesLaunched())
	require.Equal(t, int64(20), s.TotalFilled)

	// Advance to slice 2 → reconciler tick spawns and completes the TWAP.
	e.clock.Advance(1100 * time.Millisecond)
	require.NoError(t, e.twapReactor.Reconcile(e.ctx, "tw-1"))
	e.flush()

	s = loadTWAP(t, e, "tw-1")
	assert.Equal(t, twapsaga.Completed, s.Status, "should be Completed after all 3 slices")
	assert.Equal(t, int64(30), s.TotalFilled)
	assert.Equal(t, int32(3), s.SlicesLaunched())
	// All shares accounted for: 30 in holdings, 0 held (IOC = no resting holds).
	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID("acct-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(30), p.Holdings["AAPL"].Quantity)
}

func TestTWAP_UnderfillRollsForward(t *testing.T) {
	// 3 slices of 10, but only 15 shares of liquidity total.
	// Slice 0 fills 10. Slice 1 fills 5 (only 5 left at the limit).
	// Slice 2 has nothing to fill, so it goes 0.
	e := setupEnv(t)
	deposit(t, e, "acct-1", 100_000_000)

	placeLimitOrder(t, e, "AAPL", orderbook.Sell, 1500000, 15)
	e.pub.events = nil

	startTWAP(t, e, "acct-1", "tw-1", "AAPL", orderbookv1.Side_SIDE_BUY,
		30, 3, 1000, 1500000)
	e.flush()

	s := loadTWAP(t, e, "tw-1")
	require.Equal(t, int64(10), s.Slices[0].FilledQuantity, "slice 0 hits 10 from rest")

	// Slice 1: planned cumulative through slice 1 = 20, total filled = 10,
	// so next slice qty = 10. Only 5 liquidity remains → fills 5, rolls 5.
	e.clock.Advance(1100 * time.Millisecond)
	require.NoError(t, e.twapReactor.Reconcile(e.ctx, "tw-1"))
	e.flush()

	s = loadTWAP(t, e, "tw-1")
	require.Equal(t, int64(5), s.Slices[1].FilledQuantity, "slice 1 hits 5 remaining liquidity")
	require.Equal(t, int64(15), s.TotalFilled)

	// Slice 2: planned cumulative through 2 = 30, filled = 15, next qty = 15.
	// No liquidity — IOC fills 0. TWAP completes with 15 filled.
	e.clock.Advance(1100 * time.Millisecond)
	require.NoError(t, e.twapReactor.Reconcile(e.ctx, "tw-1"))
	e.flush()

	s = loadTWAP(t, e, "tw-1")
	assert.Equal(t, twapsaga.Completed, s.Status)
	assert.Equal(t, int64(15), s.TotalFilled, "stayed at 15 — no more liquidity")
	assert.Equal(t, int64(15), s.Slices[2].LaunchedQuantity, "slice 2 launched at rolled-up 15")
	assert.Equal(t, int64(0), s.Slices[2].FilledQuantity)
}

func TestTWAP_MarkFailedStopsFutureSlices(t *testing.T) {
	e := setupEnv(t)
	deposit(t, e, "acct-1", 100_000_000)

	placeLimitOrder(t, e, "AAPL", orderbook.Sell, 1500000, 100)
	e.pub.events = nil

	startTWAP(t, e, "acct-1", "tw-1", "AAPL", orderbookv1.Side_SIDE_BUY,
		30, 3, 1000, 1500000)
	e.flush()

	require.Equal(t, int32(1), loadTWAP(t, e, "tw-1").SlicesLaunched(), "slice 0 launched")

	// Cancel before slice 1.
	require.NoError(t, e.twapReactor.MarkFailed(e.ctx, "tw-1", "cancelled by user"))
	e.flush()

	s := loadTWAP(t, e, "tw-1")
	require.Equal(t, twapsaga.Failed, s.Status)

	// Advance clock + tick reconciler — no more slices should fire.
	e.clock.Advance(5 * time.Second)
	require.NoError(t, e.twapReactor.Reconcile(e.ctx, "tw-1"))
	e.flush()

	s = loadTWAP(t, e, "tw-1")
	assert.Equal(t, twapsaga.Failed, s.Status, "still Failed")
	assert.Equal(t, int32(1), s.SlicesLaunched(), "no new slices launched after fail")
}
