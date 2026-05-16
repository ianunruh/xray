package bracket_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/bracket"
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

type env struct {
	ctx              context.Context
	store            *memstore.Store
	registry         *es.Registry
	pub              *collectingPublisher
	obHandler        *es.Handler[*orderbook.OrderBook]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	bracketHandler   *es.Handler[*bracket.BracketSaga]
	orderSagaReactor *ordersaga.Reactor
	bracketReactor   *bracket.Reactor
}

// flush dispatches accumulated published events to both reactors until
// the publisher drains. Each event is delivered to BOTH reactors because
// in production every persistent consumer sees every event in the stream.
func (e *env) flush() {
	for len(e.pub.events) > 0 {
		batch := e.pub.events
		e.pub.events = nil
		_ = e.orderSagaReactor.HandleEvents(e.ctx, batch)
		_ = e.bracketReactor.HandleEvents(e.ctx, batch)
	}
}

func newRegistry() *es.Registry {
	r := es.NewRegistry()
	orderbook.RegisterEvents(r)
	portfolio.RegisterEvents(r)
	ordersaga.RegisterEvents(r)
	bracket.RegisterEvents(r)
	return r
}

func setupEnv(t *testing.T) *env {
	t.Helper()

	registry := newRegistry()
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

	orderSagaReactor := ordersaga.NewReactor(orderSagaHandler, portfolioHandler, obHandler, log)
	bracketReactor := bracket.NewReactor(bracketHandler, orderSagaHandler, portfolioHandler, obHandler, log)

	return &env{
		ctx:              ctx,
		store:            store,
		registry:         registry,
		pub:              pub,
		obHandler:        obHandler,
		portfolioHandler: portfolioHandler,
		orderSagaHandler: orderSagaHandler,
		bracketHandler:   bracketHandler,
		orderSagaReactor: orderSagaReactor,
		bracketReactor:   bracketReactor,
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

func startBracket(t *testing.T, e *env, accountID, sagaID, symbol string, side orderbookv1.Side, entryPrice, qty, tp, sl int64) {
	t.Helper()
	cmd := bracket.StartSaga{
		SagaID:          sagaID,
		AccountID:       accountID,
		Symbol:          symbol,
		EntrySide:       side,
		EntryPrice:      entryPrice,
		EntryQty:        qty,
		TakeProfitPrice: tp,
		StopLossPrice:   sl,
	}
	require.NoError(t, e.bracketHandler.Handle(e.ctx, cmd, func(b *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(b, cmd)
	}))
}

func loadBracket(t *testing.T, e *env, sagaID string) *bracket.BracketSaga {
	t.Helper()
	b, err := e.bracketHandler.Load(e.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	return b
}

func loadPortfolio(t *testing.T, e *env, accountID string) *portfolio.Portfolio {
	t.Helper()
	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID(accountID))
	require.NoError(t, err)
	return p
}

func TestBracket_FullLifecycle_TakeProfit(t *testing.T) {
	// Buy 100 @ $150 entry, TP $155, SL $145.
	// Entry matches resting sell; then take-profit matches incoming buy.
	e := setupEnv(t)
	deposit(t, e, "acct-1", 150_000_000) // just enough for the entry

	// Resting sell so the entry buy can fill.
	placeLimitOrder(t, e, "AAPL", orderbook.Sell, 1500000, 100)
	e.pub.events = nil

	startBracket(t, e, "acct-1", "br-1", "AAPL", orderbookv1.Side_SIDE_BUY,
		1500000, 100, 1550000, 1450000)
	e.flush()

	b := loadBracket(t, e, "br-1")
	require.Equal(t, bracket.PendingExit, b.Status, "should be PendingExit after entry fills")

	// Portfolio: entry settled, shares held for exit.
	p := loadPortfolio(t, e, "acct-1")
	require.Equal(t, int64(100), p.SharesHeld["AAPL"], "100 shares held for exit OCO")
	require.NotNil(t, p.Holdings["AAPL"])
	require.Equal(t, int64(100), p.Holdings["AAPL"].Quantity)
	require.Equal(t, int64(0), p.CashBalance, "all cash spent on entry")

	// Drop a buy at the TP price; TP sell exits.
	placeLimitOrder(t, e, "AAPL", orderbook.Buy, 1550000, 100)
	e.flush()

	b = loadBracket(t, e, "br-1")
	assert.Equal(t, bracket.Completed, b.Status)

	p = loadPortfolio(t, e, "acct-1")
	// Spent 150M on entry, got 155M back from TP. Net cash: 155M.
	assert.Equal(t, int64(155_000_000), p.CashBalance)
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Empty(t, p.SharesHeld)
	assert.Nil(t, p.Holdings["AAPL"])
}

func TestBracket_CancelDuringPendingEntry_ReleasesCash(t *testing.T) {
	e := setupEnv(t)
	deposit(t, e, "acct-1", 150_000_000)

	// Place bracket with no resting liquidity — entry rests on the book.
	startBracket(t, e, "acct-1", "br-1", "AAPL", orderbookv1.Side_SIDE_BUY,
		1500000, 100, 1550000, 1450000)
	e.flush()

	b := loadBracket(t, e, "br-1")
	require.Equal(t, bracket.PendingEntry, b.Status)

	p := loadPortfolio(t, e, "acct-1")
	require.Equal(t, int64(0), p.CashBalance, "all cash held for entry")
	require.Equal(t, int64(150_000_000), p.CashHeld)

	// Cancel the entry order on the orderbook — cascades through the
	// entry ordersaga to fail the bracket.
	cancelCmd := orderbook.CancelOrder{
		Symbol:  "AAPL",
		OrderID: ordersaga.OrderID(bracket.EntryOrderSagaID("br-1")),
	}
	require.NoError(t, e.obHandler.Handle(e.ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	}))
	e.flush()

	b = loadBracket(t, e, "br-1")
	assert.Equal(t, bracket.Failed, b.Status)

	p = loadPortfolio(t, e, "acct-1")
	assert.Equal(t, int64(150_000_000), p.CashBalance, "cash released")
	assert.Equal(t, int64(0), p.CashHeld)
	assert.Empty(t, p.SharesHeld)
}

func TestBracket_FailDuringPendingExit_ReleasesShares(t *testing.T) {
	e := setupEnv(t)
	deposit(t, e, "acct-1", 150_000_000)

	placeLimitOrder(t, e, "AAPL", orderbook.Sell, 1500000, 100)
	e.pub.events = nil

	startBracket(t, e, "acct-1", "br-1", "AAPL", orderbookv1.Side_SIDE_BUY,
		1500000, 100, 1550000, 1450000)
	e.flush()

	b := loadBracket(t, e, "br-1")
	require.Equal(t, bracket.PendingExit, b.Status)

	p := loadPortfolio(t, e, "acct-1")
	require.Equal(t, int64(100), p.SharesHeld["AAPL"])

	// User cancels: record the bracket as failed; reactor releases shares.
	failCmd := bracket.RecordSagaFailed{SagaID: "br-1", Reason: "user cancelled"}
	require.NoError(t, e.bracketHandler.Handle(e.ctx, failCmd, func(saga *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteRecordSagaFailed(saga, failCmd)
	}))
	e.flush()

	b = loadBracket(t, e, "br-1")
	assert.Equal(t, bracket.Failed, b.Status)

	p = loadPortfolio(t, e, "acct-1")
	assert.Empty(t, p.SharesHeld, "share hold released after failure")
	assert.Equal(t, int64(100), p.Holdings["AAPL"].Quantity, "shares still owned (entry already settled)")
}

func TestBracket_InsufficientCash_EntryFails(t *testing.T) {
	e := setupEnv(t)
	// Deposit not enough — bracket needs 150M but we have 1M.
	deposit(t, e, "acct-1", 1_000_000)

	startBracket(t, e, "acct-1", "br-1", "AAPL", orderbookv1.Side_SIDE_BUY,
		1500000, 100, 1550000, 1450000)
	for range ordersaga.MaxActionAttempts + 1 {
		e.flush()
	}

	b := loadBracket(t, e, "br-1")
	assert.Equal(t, bracket.Failed, b.Status)

	p := loadPortfolio(t, e, "acct-1")
	assert.Equal(t, int64(1_000_000), p.CashBalance, "cash untouched")
	assert.Equal(t, int64(0), p.CashHeld)
}
