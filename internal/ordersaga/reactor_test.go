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
	reactor.SetReady(ctx)

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

func TestReactor_Recovery_NoDoubleFillSettlement(t *testing.T) {
	// Simulate a restart after partial fills have been settled.
	// Before the fix, replaying orderbook TradeExecuted events would add
	// already-settled fills back to pendingFills, causing cash_held to go
	// negative from duplicate CashSettled events on the portfolio.

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
	reactor.SetReady(ctx)

	env := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		portfolioHandler: portfolioHandler, reactor: reactor,
		store: store, registry: registry, pub: pub,
	}

	depositCash(t, env, "acct-1", 150000000)

	// Place partial sell liquidity — only 60 of 100 shares.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 60)
	env.pub.events = nil

	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	// 60 shares filled & settled, saga still in OrderPlaced.
	s := loadSaga(t, env, "saga-1")
	require.Equal(t, ordersaga.OrderPlaced, s.Status)

	p := loadPortfolio(t, env, "acct-1")
	require.Equal(t, int64(60000000), p.CashHeld)
	require.Equal(t, int64(0), p.CashBalance)

	// --- Simulate restart: new reactor replays all events ---
	reactor2 := ordersaga.NewReactor(sagaHandler, portfolioHandler, obHandler, slog.Default())

	rawEvents, err := store.LoadAll(ctx)
	require.NoError(t, err)

	events := make([]es.Event, 0, len(rawEvents))
	for _, raw := range rawEvents {
		evt, err := registry.Deserialize(raw)
		require.NoError(t, err)
		events = append(events, evt)
	}
	err = reactor2.HandleEvents(ctx, events)
	require.NoError(t, err)

	reactor2.SetReady(ctx)

	env2 := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		portfolioHandler: portfolioHandler, reactor: reactor2,
		store: store, registry: registry, pub: pub,
	}

	// Place remaining liquidity to complete the order.
	placeLimitOrder(t, env2, "AAPL", orderbook.Sell, 1500000, 40)
	env2.flush()

	s = loadSaga(t, env2, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status)

	p = loadPortfolio(t, env2, "acct-1")
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
}

func TestReactor_Recovery_TradeBeforeFillRecord_CrossBatch(t *testing.T) {
	// Replay can split events across HandleEvents batches. If TradeExecuted
	// arrives in an earlier batch than its OrderSagaFillRecorded, the trade
	// is queued in pendingFills with settledTrades empty. The later batch
	// must prune the pendingFill entry — otherwise SetReady triggers a
	// duplicate settlement on the portfolio.

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
	reactor.SetReady(ctx)

	env := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		portfolioHandler: portfolioHandler, reactor: reactor,
		store: store, registry: registry, pub: pub,
	}

	depositCash(t, env, "acct-1", 150000000)
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 60)
	env.pub.events = nil

	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	// 60 shares filled & settled; 40 still pending.
	p := loadPortfolio(t, env, "acct-1")
	require.Equal(t, int64(60000000), p.CashHeld)
	require.Equal(t, int64(0), p.CashBalance)

	// --- Restart: feed every event as its own batch ---
	reactor2 := ordersaga.NewReactor(sagaHandler, portfolioHandler, obHandler, slog.Default())

	rawEvents, err := store.LoadAll(ctx)
	require.NoError(t, err)

	for _, raw := range rawEvents {
		evt, err := registry.Deserialize(raw)
		require.NoError(t, err)
		require.NoError(t, reactor2.HandleEvents(ctx, []es.Event{evt}))
	}

	reactor2.SetReady(ctx)
	env.pub.events = nil

	env2 := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		portfolioHandler: portfolioHandler, reactor: reactor2,
		store: store, registry: registry, pub: pub,
	}
	env2.flush()

	// Critical: CashHeld must still be 60M for the 40 unfilled shares — not
	// 0 (double-settled, deleting the hold) or anything weird.
	p = loadPortfolio(t, env2, "acct-1")
	assert.Equal(t, int64(60000000), p.CashHeld, "first 60 must not have been re-settled")
	assert.Equal(t, int64(0), p.CashBalance)
	assert.Equal(t, int64(60), p.Holdings["AAPL"].Quantity, "holdings must not double-count the first 60")
}

func TestReactor_Recovery_FillsDuringReplay(t *testing.T) {
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

	// NOT ready yet.
	reactor := ordersaga.NewReactor(sagaHandler, portfolioHandler, obHandler, slog.Default())

	env := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		portfolioHandler: portfolioHandler, reactor: reactor,
		store: store, registry: registry, pub: pub,
	}

	depositCash(t, env, "acct-1", 150000000)
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	startOrderSaga(t, env, "saga-1", "acct-1", "AAPL", orderbookv1.Side_SIDE_BUY, 1500000, 100)
	env.flush()

	// Saga should still be Started (reactor not ready, no actions taken).
	s := loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Started, s.Status)

	// Now set ready — should trigger recovery.
	reactor.SetReady(ctx)
	env.flush()

	// Saga should progress through the full lifecycle.
	s = loadSaga(t, env, "saga-1")
	assert.Equal(t, ordersaga.Completed, s.Status)
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
