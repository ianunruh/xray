package ordersaga_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
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

func (p *collectingPublisher) flush(ctx context.Context, reactor *ordersaga.Reactor) {
	for len(p.events) > 0 {
		batch := p.events
		p.events = nil
		reactor.HandleEvents(ctx, batch)
	}
}

type reactorTestEnv struct {
	ctx              context.Context
	obHandler        *es.Handler[*orderbook.OrderBook]
	sagaHandler      *es.Handler[*ordersaga.OrderSaga]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	reactor          *ordersaga.Reactor
	store            *memstore.Store
	registry         *es.Registry
	pub              *collectingPublisher
}

func (e *reactorTestEnv) flush() {
	e.pub.flush(e.ctx, e.reactor)
}

func newFullTestRegistry() *es.Registry {
	r := es.NewRegistry()
	orderbook.RegisterEvents(r)
	portfolio.RegisterEvents(r)
	ordersaga.RegisterEvents(r)
	return r
}

func setupReactorTest(t *testing.T) *reactorTestEnv {
	t.Helper()

	registry := newFullTestRegistry()
	store := memstore.New()
	ctx := context.Background()
	pub := &collectingPublisher{}

	obHandler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default()).WithPublisher(pub)

	sagaHandler := es.NewHandler(store, registry, func(id string) *ordersaga.OrderSaga {
		return ordersaga.NewOrderSaga(id)
	}, slog.Default()).WithPublisher(pub)

	portfolioHandler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := ordersaga.NewReactor(sagaHandler, portfolioHandler, obHandler, slog.Default())

	return &reactorTestEnv{
		ctx:              ctx,
		obHandler:        obHandler,
		sagaHandler:      sagaHandler,
		portfolioHandler: portfolioHandler,
		reactor:          reactor,
		store:            store,
		registry:         registry,
		pub:              pub,
	}
}

func depositCash(t *testing.T, env *reactorTestEnv, accountID string, amount int64) {
	t.Helper()
	cmd := portfolio.DepositCash{AccountID: accountID, Amount: amount}
	err := env.portfolioHandler.Handle(env.ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, cmd)
	})
	require.NoError(t, err)
}

func startOrderSaga(t *testing.T, env *reactorTestEnv, sagaID, accountID, symbol string, side orderbookv1.Side, price, qty int64) {
	t.Helper()
	cmd := ordersaga.StartOrderSaga{
		SagaID:      sagaID,
		AccountID:   accountID,
		Symbol:      symbol,
		Side:        side,
		Price:       price,
		Quantity:    qty,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	err := env.sagaHandler.Handle(env.ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	})
	require.NoError(t, err)
}

func startReplaceOrderSaga(t *testing.T, env *reactorTestEnv, sagaID, accountID, symbol string, side orderbookv1.Side, price, qty int64, replaceOrderID string) {
	t.Helper()
	cmd := ordersaga.StartOrderSaga{
		SagaID:         sagaID,
		AccountID:      accountID,
		Symbol:         symbol,
		Side:           side,
		Price:          price,
		Quantity:       qty,
		OrderType:      orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce:    orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
		ReplaceOrderID: replaceOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	})
	require.NoError(t, err)
}

func placeLimitOrder(t *testing.T, env *reactorTestEnv, symbol string, side orderbook.Side, price, qty int64) string {
	t.Helper()
	cmd := orderbook.PlaceOrder{
		Symbol:   symbol,
		Side:     side,
		Price:    price,
		Quantity: qty,
	}
	var orderID string
	err := env.obHandler.Handle(env.ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		events, err := orderbook.ExecutePlaceOrder(book, cmd)
		if err != nil {
			return nil, err
		}
		for _, evt := range events {
			if placed, ok := evt.Data.(*orderbookv1.OrderPlaced); ok {
				orderID = placed.OrderId
				break
			}
		}
		return events, nil
	})
	require.NoError(t, err)
	return orderID
}

func loadPortfolio(t *testing.T, env *reactorTestEnv, accountID string) *portfolio.Portfolio {
	t.Helper()
	p, err := env.portfolioHandler.Load(env.ctx, portfolio.AggregateID(accountID))
	require.NoError(t, err)
	return p
}

func loadSaga(t *testing.T, env *reactorTestEnv, sagaID string) *ordersaga.OrderSaga {
	t.Helper()
	s, err := env.sagaHandler.Load(env.ctx, ordersaga.AggregateID(sagaID))
	require.NoError(t, err)
	return s
}

func TestReactor_FullLifecycle(t *testing.T) {
	env := setupReactorTest(t)

	// Deposit $1500 (150.00 * 100 shares = $15,000).
	depositCash(t, env, "acct-1", 150000000)

	// Place resting sell liquidity at $150.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	// Start order saga: buy 100 AAPL at $150.
	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)

	// Flush: reactor holds cash, places order, order matches, records fill, completes.
	env.flush()

	// Verify saga completed.
	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status)
	assert.Equal(t, int64(100), s.FilledQty)

	// Verify portfolio.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(150000000), p.Holdings["AAPL"].TotalCost)
}

func TestReactor_StatelessReactor_FreshInstanceHandlesMidLifecycleEvents(t *testing.T) {
	// Simulates a process restart: a fresh ordersaga reactor (with no
	// state inherited from the previous run) should still correctly
	// settle trades and complete the saga when it observes
	// TradeExecuted events for an in-flight order.
	env := setupReactorTest(t)
	depositCash(t, env, "acct-1", 150000000)

	// Resting liquidity for 60 of the 100 — saga will be partially
	// filled and remain OrderPlaced after the first flush.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 60)
	env.pub.events = nil
	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	require.Equal(t, ordersaga.OrderPlaced, loadSaga(t, env, "saga-1").Status)

	// "Restart" the reactor — fresh instance with no in-memory state.
	env.reactor = ordersaga.NewReactor(env.sagaHandler, env.portfolioHandler, env.obHandler, slog.Default())

	// Fill the remaining 40; the fresh reactor must settle and complete.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 40)
	env.flush()

	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status, "fresh reactor completed the in-flight saga")
	assert.Equal(t, int64(100), s.FilledQty)

	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
}

func TestReactor_PartialFills(t *testing.T) {
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 150000000)

	// Place only 60 shares of sell liquidity.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 60)
	env.pub.events = nil

	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	// Saga should be OrderPlaced (partially filled, not complete).
	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.OrderPlaced, s.Status)
	assert.Equal(t, int64(60), s.FilledQty)

	// Portfolio should reflect the 60-share partial fill.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(60), p.Holdings["AAPL"].Quantity)
	// 60 * $150 = $9,000 settled, $6,000 still held.
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(60000000), p.CashHeld)

	// Now fill the remaining 40.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 40)
	env.flush()

	s = loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status)

	p = loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
}

func TestReactor_OrderCancelled_CashReleased(t *testing.T) {
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 150000000)

	// Start order saga with no matching liquidity.
	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	// Saga should be OrderPlaced (resting, no fills).
	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.OrderPlaced, s.Status)
	orderID := s.OrderID
	require.NotEmpty(t, orderID)

	// Cancel the order.
	cancelCmd := orderbook.CancelOrder{Symbol: "AAPL", OrderID: orderID}
	err := env.obHandler.Handle(env.ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	require.NoError(t, err)
	env.flush()

	// Saga should fail.
	s = loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Failed, s.Status)

	// Cash should be fully released.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(150000000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
}

func TestReactor_InsufficientFunds_SagaFails(t *testing.T) {
	env := setupReactorTest(t)

	// Only deposit $100 but try to buy 100 shares at $150.
	depositCash(t, env, "acct-1", 1000000)

	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)

	// Flush repeatedly to exhaust retries.
	for i := 0; i < ordersaga.MaxActionAttempts+1; i++ {
		env.flush()
	}

	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Failed, s.Status)

	// Cash should not have been touched.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(1000000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
}

func TestReactor_PriceImprovement_RemainingCashReleased(t *testing.T) {
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 150000000)

	// Resting sell at $149 (better price than our limit of $150).
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1490000, 100)
	env.pub.events = nil

	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status)

	// Cash held was $150 * 100 = $150,000, filled at $149 * 100 = $149,000.
	// Remaining $1,000 should be released.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(1000000), p.CashBalance) // $100 returned
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(149000000), p.Holdings["AAPL"].TotalCost) // cost basis at actual fill price
}

func TestReactor_SellOrder_SharesHeld(t *testing.T) {
	env := setupReactorTest(t)

	// Deposit cash and buy shares so we have a holding to sell.
	depositCash(t, env, "acct-1", 150000000)
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil
	startOrderSaga(t, env, "saga-buy", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	// Verify we own 100 AAPL.
	p := loadPortfolio(t, env, "acct-1")
	require.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)

	// Place resting buy liquidity for the sell order to match against.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 50)
	env.pub.events = nil

	// Start sell order saga: sell 50 AAPL at $155.
	startOrderSaga(t, env, "saga-sell", "acct-1", "AAPL", orderbookv1.Side_SIDE_SELL, 1550000, 50)
	env.flush()

	// Verify sell saga completed.
	s := loadSaga(t, env, "saga-sell")
	assert.Equal(t, ordersaga.Completed, s.Status)
	assert.Equal(t, int64(50), s.FilledQty)
	assert.Equal(t, int64(50), s.AmountHeld)

	// Verify portfolio: 50 shares sold, cash credited.
	p = loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(50), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(77500000), p.CashBalance) // 50 * $155 = $77,500
	assert.Empty(t, p.SharesHeld)
	assert.Empty(t, p.ShareHoldsBySaga)
}

func TestReactor_SellOrder_PartialFills(t *testing.T) {
	env := setupReactorTest(t)

	// Buy 100 shares.
	depositCash(t, env, "acct-1", 150000000)
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil
	startOrderSaga(t, env, "saga-buy", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	// Place only 60 shares of buy liquidity.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 60)
	env.pub.events = nil

	startOrderSaga(t, env, "saga-sell", "acct-1", "AAPL", orderbookv1.Side_SIDE_SELL, 1550000, 100)
	env.flush()

	// Saga should be OrderPlaced (partially filled).
	s := loadSaga(t, env, "saga-sell")
	assert.Equal(t, ordersaga.OrderPlaced, s.Status)
	assert.Equal(t, int64(60), s.FilledQty)

	// Portfolio should reflect partial sell.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(40), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(40), p.SharesHeld["AAPL"])
	assert.Equal(t, int64(93000000), p.CashBalance) // 60 * $155

	// Fill remaining 40.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 40)
	env.flush()

	s = loadSaga(t, env, "saga-sell")
	assert.Equal(t, ordersaga.Completed, s.Status)

	p = loadPortfolio(t, env, "acct-1")
	assert.Nil(t, p.Holdings["AAPL"])
	assert.Empty(t, p.SharesHeld)
	assert.Equal(t, int64(155000000), p.CashBalance) // 100 * $155
}

func TestReactor_SellOrder_Cancelled_SharesReleased(t *testing.T) {
	env := setupReactorTest(t)

	// Buy 100 shares.
	depositCash(t, env, "acct-1", 150000000)
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil
	startOrderSaga(t, env, "saga-buy", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	// Start sell saga with no matching liquidity.
	startOrderSaga(t, env, "saga-sell", "acct-1", "AAPL", orderbookv1.Side_SIDE_SELL, 1550000, 100)
	env.flush()

	// Saga should be OrderPlaced (resting, no fills).
	s := loadSaga(t, env, "saga-sell")
	assert.Equal(t, ordersaga.OrderPlaced, s.Status)
	orderID := s.OrderID
	require.NotEmpty(t, orderID)

	// Shares should be held.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(100), p.SharesHeld["AAPL"])

	// Cancel the order.
	cancelCmd := orderbook.CancelOrder{Symbol: "AAPL", OrderID: orderID}
	err := env.obHandler.Handle(env.ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	require.NoError(t, err)
	env.flush()

	// Saga should fail.
	s = loadSaga(t, env, "saga-sell")
	assert.Equal(t, ordersaga.Failed, s.Status)

	// Shares should be fully released.
	p = loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Empty(t, p.SharesHeld)
	assert.Equal(t, int64(0), p.CashBalance)
}

func TestReactor_SellOrder_InsufficientShares_SagaFails(t *testing.T) {
	env := setupReactorTest(t)

	// Buy only 50 shares but try to sell 100.
	depositCash(t, env, "acct-1", 75000000)
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 50)
	env.pub.events = nil
	startOrderSaga(t, env, "saga-buy", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 50)
	env.flush()

	p := loadPortfolio(t, env, "acct-1")
	require.Equal(t, int64(50), p.Holdings["AAPL"].Quantity)

	startOrderSaga(t, env, "saga-sell", "acct-1", "AAPL", orderbookv1.Side_SIDE_SELL, 1550000, 100)

	// Flush repeatedly to exhaust retries.
	for i := 0; i < ordersaga.MaxActionAttempts+1; i++ {
		env.flush()
	}

	s := loadSaga(t, env, "saga-sell")
	assert.Equal(t, ordersaga.Failed, s.Status)

	// Shares should not have been touched.
	p = loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(50), p.Holdings["AAPL"].Quantity)
	assert.Empty(t, p.SharesHeld)
}

func TestReactor_BothSidesTracked(t *testing.T) {
	env := setupReactorTest(t)

	// Portfolio A: has cash, will buy.
	depositCash(t, env, "acct-A", 150000000)
	// Portfolio B: has shares (bought previously), will sell.
	depositCash(t, env, "acct-B", 150000000)
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil
	startOrderSaga(t, env, "saga-buy-setup", "acct-B", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	p := loadPortfolio(t, env, "acct-B")
	require.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)

	// Start buy limit on A.
	startOrderSaga(t, env, "saga-buy", "acct-A", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	buyS := loadSaga(t, env, "saga-buy")
	require.Equal(t, ordersaga.OrderPlaced, buyS.Status)

	// Start market sell on B — should match against A's resting buy.
	startOrderSaga(t, env, "saga-sell", "acct-B", "AAPL", orderbookv1.Side_SIDE_SELL, 1500000, 100)
	env.flush()

	// Both sagas should complete.
	buyS = loadSaga(t, env, "saga-buy")
	assert.Equal(t, ordersaga.Completed, buyS.Status)
	assert.Equal(t, int64(100), buyS.FilledQty)

	sellS := loadSaga(t, env, "saga-sell")
	assert.Equal(t, ordersaga.Completed, sellS.Status)
	assert.Equal(t, int64(100), sellS.FilledQty)

	// Portfolio A: spent $150k, owns 100 shares.
	pA := loadPortfolio(t, env, "acct-A")
	assert.Equal(t, int64(0), pA.CashBalance)
	assert.Equal(t, int64(0), pA.CashHeld)
	assert.Equal(t, int64(100), pA.Holdings["AAPL"].Quantity)

	// Portfolio B: sold 100 shares, received $150k.
	pB := loadPortfolio(t, env, "acct-B")
	assert.Equal(t, int64(150000000), pB.CashBalance)
	assert.Empty(t, pB.SharesHeld)
	assert.Nil(t, pB.Holdings["AAPL"])
}

func TestReactor_ReplaceOrder_FullLifecycle(t *testing.T) {
	env := setupReactorTest(t)

	// Deposit enough for both sagas to hold cash simultaneously.
	// Old saga holds $150k, new saga holds $151k before the old is released.
	depositCash(t, env, "acct-1", 301000000)

	// Place a resting buy order via saga at $150.
	startOrderSaga(t, env, "saga-old", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	oldSaga := loadSaga(t, env, "saga-old")
	require.Equal(t, ordersaga.OrderPlaced, oldSaga.Status)
	oldOrderID := oldSaga.OrderID
	require.NotEmpty(t, oldOrderID)

	// Place sell liquidity at $151 so the replacement order can match.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1510000, 100)
	env.pub.events = nil

	// Start a replace saga: cancel old order at $150, place new buy at $151.
	startReplaceOrderSaga(t, env, "saga-new", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1510000, 100, oldOrderID)
	env.flush()

	// Old saga should fail (its order was cancelled by the replace).
	oldSaga = loadSaga(t, env, "saga-old")
	assert.Equal(t, ordersaga.Failed, oldSaga.Status)

	// New saga should complete (matched against the sell liquidity).
	newSaga := loadSaga(t, env, "saga-new")
	assert.Equal(t, ordersaga.Completed, newSaga.Status)
	assert.Equal(t, int64(100), newSaga.FilledQty)

	// Portfolio: old saga released $150k, new saga settled $151k at fill.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(151000000), p.Holdings["AAPL"].TotalCost)
	assert.Equal(t, int64(150000000), p.CashBalance) // $301k - $151k settled
}

func TestReactor_ReplaceOrder_OldOrderGone(t *testing.T) {
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 150000000)

	// Start a replace saga targeting a nonexistent order ID.
	startReplaceOrderSaga(t, env, "saga-replace", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100, "nonexistent-order")
	for i := 0; i < ordersaga.MaxActionAttempts+1; i++ {
		env.flush()
	}

	// Saga should fail after max retries (ReplaceOrder fails with order not found).
	s := loadSaga(t, env, "saga-replace")
	assert.Equal(t, ordersaga.Failed, s.Status)

	// Cash should be fully released.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(150000000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
}

func TestReactor_ReplaceOrder_NoMatch_RestingThenCancel(t *testing.T) {
	env := setupReactorTest(t)

	// Deposit enough for both sagas to hold cash simultaneously.
	depositCash(t, env, "acct-1", 301000000)

	// Place a resting buy order via saga at $150.
	startOrderSaga(t, env, "saga-old", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	oldSaga := loadSaga(t, env, "saga-old")
	require.Equal(t, ordersaga.OrderPlaced, oldSaga.Status)
	oldOrderID := oldSaga.OrderID

	// Replace with a new buy at $151, but no sell liquidity — new order rests.
	startReplaceOrderSaga(t, env, "saga-new", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1510000, 100, oldOrderID)
	env.flush()

	// Old saga should fail.
	oldSaga = loadSaga(t, env, "saga-old")
	assert.Equal(t, ordersaga.Failed, oldSaga.Status)

	// New saga should be OrderPlaced (resting, no fills).
	newSaga := loadSaga(t, env, "saga-new")
	assert.Equal(t, ordersaga.OrderPlaced, newSaga.Status)
	newOrderID := newSaga.OrderID
	require.NotEmpty(t, newOrderID)

	// Cash: old saga released $150k, new saga holds $151k.
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(151000000), p.CashHeld)
	assert.Equal(t, int64(150000000), p.CashBalance) // $301k - $151k held

	// Cancel the new resting order.
	cancelCmd := orderbook.CancelOrder{Symbol: "AAPL", OrderID: newOrderID}
	err := env.obHandler.Handle(env.ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	require.NoError(t, err)
	env.flush()

	newSaga = loadSaga(t, env, "saga-new")
	assert.Equal(t, ordersaga.Failed, newSaga.Status)

	p = loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(301000000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
}

func startMarketBuySaga(t *testing.T, env *reactorTestEnv, sagaID, accountID, symbol string, qty int64) {
	t.Helper()
	cmd := ordersaga.StartOrderSaga{
		SagaID:      sagaID,
		AccountID:   accountID,
		Symbol:      symbol,
		Side:        orderbookv1.Side_SIDE_BUY,
		Price:       0,
		Quantity:    qty,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_MARKET,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_IOC,
	}
	err := env.sagaHandler.Handle(env.ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	})
	require.NoError(t, err)
}

func TestReactor_MarketBuy_SweepMultipleLevels(t *testing.T) {
	// A market BUY that sweeps across multiple ask levels must hold the
	// walked-book cost plus a slippage buffer, then release the buffer
	// remainder on completion — never leave CashHeld negative.
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 1000000000) // $100,000 — plenty

	// Two ask levels: 50 @ $150 and 50 @ $160.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 50)
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1600000, 50)
	env.pub.events = nil

	startMarketBuySaga(t, env, "saga-1", "acct-1", "AAPL", 100)
	env.flush()

	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status)
	assert.Equal(t, int64(100), s.FilledQty)

	// Walked cost: 50*1,500,000 + 50*1,600,000 = 155,000,000.
	// Hold with 5% buffer (10500 bps): 155,000,000 * 1.05 = 162,750,000.
	assert.Equal(t, int64(162750000), s.AmountHeld)

	// Actual spend: 155,000,000. CashHeld must end at 0 (buffer released).
	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Empty(t, p.HoldsBySaga)
	assert.Equal(t, int64(845000000), p.CashBalance) // 1,000,000,000 - 155,000,000
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	assert.Equal(t, int64(155000000), p.Holdings["AAPL"].TotalCost)
}

func TestReactor_MarketBuy_NoLiquidity_SagaFails(t *testing.T) {
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 1000000000)

	startMarketBuySaga(t, env, "saga-1", "acct-1", "AAPL", 100)
	for i := 0; i < ordersaga.MaxActionAttempts+1; i++ {
		env.flush()
	}

	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Failed, s.Status)

	p := loadPortfolio(t, env, "acct-1")
	assert.Equal(t, int64(1000000000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
}

func TestReactor_CausationChain_EndToEnd(t *testing.T) {
	// Drive a full order saga (hold cash → place order → match → settle →
	// record fill → complete) and assert every event the saga produces
	// shares the same correlation_id, and every event's causation_id
	// references some earlier event in the same chain.
	env := setupReactorTest(t)
	depositCash(t, env, "acct-1", 150000000)

	// Pre-existing resting sell on a separate correlation. Its events
	// should NOT appear in the saga's chain.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	// Start the saga inside a fresh correlation we know upfront so we can
	// filter events by it. The reactor's ctx-propagation should carry
	// this correlation all the way through the chain.
	ctx, correlationID := es.NewCorrelation(env.ctx)
	cmd := ordersaga.StartOrderSaga{
		SagaID:      "saga-1",
		AccountID:   "acct-1",
		Symbol:      "AAPL",
		Side:        orderbookv1.Side_SIDE_BUY,
		Price:       1500000,
		Quantity:    100,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	require.NoError(t, env.sagaHandler.Handle(ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	}))
	env.flush()

	raw, err := env.store.LoadAll(env.ctx)
	require.NoError(t, err)

	var chain []es.RawEvent
	idsInChain := make(map[string]string)
	for _, r := range raw {
		if r.CorrelationID == correlationID {
			chain = append(chain, r)
			idsInChain[r.ID] = r.Type
		}
	}

	// Sanity: a full lifecycle emits at least started, cash-held, saga
	// cash-held, order-placed, trade-executed, settled, fill-recorded,
	// completed.
	require.GreaterOrEqual(t, len(chain), 6, "saga chain too short: %+v", chain)

	// First event in the chain originated from the user RPC and has no
	// parent event.
	first := chain[0]
	assert.NotEmpty(t, first.ID, "first event must have an ID")
	assert.Empty(t, first.CausationID, "origin event has no causation")
	assert.Equal(t, "OrderSagaStarted", first.Type, "first event in chain should be OrderSagaStarted")

	// Every subsequent event's CausationID must point at some prior event
	// in the same chain — proves causation propagates through the reactor
	// across aggregate boundaries (saga → portfolio → orderbook → saga).
	for _, r := range chain[1:] {
		require.NotEmpty(t, r.CausationID, "non-origin event %s (%s) missing causation", r.ID, r.Type)
		_, found := idsInChain[r.CausationID]
		assert.True(t, found,
			"event %s (%s) causation_id=%s should reference an event in same chain",
			r.ID, r.Type, r.CausationID)
	}

	// Resting sell's events must NOT have been pulled into this chain.
	for _, r := range raw {
		if r.Type == "OrderPlaced" && r.CorrelationID != correlationID {
			// This is the pre-existing resting sell; expected.
			assert.NotEmpty(t, r.CorrelationID, "even origin events get a fresh correlation")
			break
		}
	}
}

// --- Short-selling tests ---

// startShortOpenSaga starts a SELL+SHORT (sell-to-open) limit-order saga.
func startShortOpenSaga(t *testing.T, env *reactorTestEnv, sagaID, accountID, symbol string, price, qty int64) {
	t.Helper()
	cmd := ordersaga.StartOrderSaga{
		SagaID:       sagaID,
		AccountID:    accountID,
		Symbol:       symbol,
		Side:         orderbookv1.Side_SIDE_SELL,
		Price:        price,
		Quantity:     qty,
		OrderType:    orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce:  orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
		PositionSide: orderbookv1.PositionSide_POSITION_SIDE_SHORT,
	}
	require.NoError(t, env.sagaHandler.Handle(env.ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	}))
}

// startShortCoverSaga starts a BUY+SHORT (buy-to-cover) limit-order saga.
func startShortCoverSaga(t *testing.T, env *reactorTestEnv, sagaID, accountID, symbol string, price, qty int64) {
	t.Helper()
	cmd := ordersaga.StartOrderSaga{
		SagaID:       sagaID,
		AccountID:    accountID,
		Symbol:       symbol,
		Side:         orderbookv1.Side_SIDE_BUY,
		Price:        price,
		Quantity:     qty,
		OrderType:    orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce:  orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
		PositionSide: orderbookv1.PositionSide_POSITION_SIDE_SHORT,
	}
	require.NoError(t, env.sagaHandler.Handle(env.ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	}))
}

func TestReactor_ShortOpen_Limit_FullLifecycle(t *testing.T) {
	env := setupReactorTest(t)

	// Deposit $10000. A 100-share short at $150 has $15000 notional;
	// 50% margin = $7500 collateral.
	depositCash(t, env, "acct-1", 100000000)

	// Resting bid for the short to lift.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)
	env.pub.events = nil

	startShortOpenSaga(t, env, "saga-short", "acct-1", "AAPL", 1500000, 100)
	env.flush()

	s := loadSaga(t, env, "saga-short")
	assert.Equal(t, ordersaga.Completed, s.Status)
	assert.Equal(t, int64(100), s.FilledQty)

	p := loadPortfolio(t, env, "acct-1")
	short := p.ShortPositions["AAPL"]
	require.NotNil(t, short)
	assert.Equal(t, int64(100), short.Quantity)
	assert.Equal(t, int64(1500000), short.AvgOpenPrice)
	assert.Equal(t, int64(150000000), short.ProceedsHeld)
	assert.Equal(t, int64(75000000), short.CollateralHeld)
	assert.Empty(t, p.CollateralHeldBySaga)
	// $10000 - $7500 collateral = $2500 cash.
	assert.Equal(t, int64(25000000), p.CashBalance)
}

func TestReactor_ShortOpen_InsufficientCollateral_SagaFails(t *testing.T) {
	env := setupReactorTest(t)

	// Deposit only $100 — far too little for collateral on a 100-share short at $150.
	depositCash(t, env, "acct-1", 1000000)

	startShortOpenSaga(t, env, "saga-short", "acct-1", "AAPL", 1500000, 100)
	env.flush()

	s := loadSaga(t, env, "saga-short")
	assert.Equal(t, ordersaga.Failed, s.Status)
}

func TestReactor_ShortCover_Profit_FullLifecycle(t *testing.T) {
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 100000000)
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)
	env.pub.events = nil
	startShortOpenSaga(t, env, "saga-short", "acct-1", "AAPL", 1500000, 100)
	env.flush()

	// Now cover at $120 (price dropped — profit).
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1200000, 100)
	env.pub.events = nil
	startShortCoverSaga(t, env, "saga-cover", "acct-1", "AAPL", 1200000, 100)
	env.flush()

	s := loadSaga(t, env, "saga-cover")
	assert.Equal(t, ordersaga.Completed, s.Status)

	p := loadPortfolio(t, env, "acct-1")
	assert.Empty(t, p.ShortPositions)
	// Realized PnL = (1500000 - 1200000) * 100 = 30M.
	// Final cash = $10000 deposit + $3000 profit = $13000.
	assert.Equal(t, int64(130000000), p.CashBalance)
}

func TestReactor_ShortCover_RefusedWithoutShort(t *testing.T) {
	env := setupReactorTest(t)

	// No short open — try to cover anyway.
	depositCash(t, env, "acct-1", 100000000)
	startShortCoverSaga(t, env, "saga-cover", "acct-1", "AAPL", 1500000, 100)
	env.flush()

	s := loadSaga(t, env, "saga-cover")
	assert.Equal(t, ordersaga.Failed, s.Status)
}

func TestReactor_ShortOpen_Cancelled_CollateralReleased(t *testing.T) {
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 100000000)

	// Start the saga but don't seed bid liquidity — it'll rest as an ask.
	startShortOpenSaga(t, env, "saga-short", "acct-1", "AAPL", 1500000, 100)
	env.flush()

	// Cancel the resting order.
	orderID := ordersaga.OrderID("saga-short")
	cancelCmd := orderbook.CancelOrder{Symbol: "AAPL", OrderID: orderID}
	require.NoError(t, env.obHandler.Handle(env.ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	}))
	env.flush()

	s := loadSaga(t, env, "saga-short")
	assert.Equal(t, ordersaga.Failed, s.Status)

	p := loadPortfolio(t, env, "acct-1")
	assert.Empty(t, p.CollateralHeldBySaga)
	assert.Equal(t, int64(100000000), p.CashBalance) // collateral fully returned
}

func TestReactor_ShortOpen_Market_BidLiquidity(t *testing.T) {
	env := setupReactorTest(t)

	depositCash(t, env, "acct-1", 100000000)

	// Seed bid liquidity for the market short to hit.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)
	env.pub.events = nil

	cmd := ordersaga.StartOrderSaga{
		SagaID:       "saga-mshort",
		AccountID:    "acct-1",
		Symbol:       "AAPL",
		Side:         orderbookv1.Side_SIDE_SELL,
		Quantity:     100,
		OrderType:    orderbookv1.OrderType_ORDER_TYPE_MARKET,
		TimeInForce:  orderbookv1.TimeInForce_TIME_IN_FORCE_IOC,
		PositionSide: orderbookv1.PositionSide_POSITION_SIDE_SHORT,
	}
	require.NoError(t, env.sagaHandler.Handle(env.ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	}))
	env.flush()

	s := loadSaga(t, env, "saga-mshort")
	assert.Equal(t, ordersaga.Completed, s.Status)
	p := loadPortfolio(t, env, "acct-1")
	require.NotNil(t, p.ShortPositions["AAPL"])
	assert.Equal(t, int64(100), p.ShortPositions["AAPL"].Quantity)
}
