package margincall_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/margincall"
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

type fakeMarker struct {
	prices map[string]int64
}

func (f *fakeMarker) GetMarkPrice(symbol string) (int64, time.Time, bool) {
	p, ok := f.prices[symbol]
	if !ok {
		return 0, time.Time{}, false
	}
	return p, time.Now(), true
}

func (f *fakeMarker) set(symbol string, price int64) {
	f.prices[symbol] = price
}

type env struct {
	ctx              context.Context
	pub              *collectingPublisher
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	shorts           *portfolio.InMemoryShortsBySymbol
	marker           *fakeMarker
	reactor          *margincall.Reactor
}

func newEnv(t *testing.T) *env {
	t.Helper()
	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)
	portfolio.RegisterEvents(registry)
	ordersaga.RegisterEvents(registry)

	store := memstore.New()
	pub := &collectingPublisher{}
	log := slog.Default()
	ctx := context.Background()

	portfolioHandler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, log).WithPublisher(pub)
	orderSagaHandler := es.NewHandler(store, registry, func(id string) *ordersaga.OrderSaga {
		return ordersaga.NewOrderSaga(id)
	}, log).WithPublisher(pub)

	shorts := portfolio.NewInMemoryShortsBySymbol()
	marker := &fakeMarker{prices: make(map[string]int64)}
	reactor := margincall.NewReactor(portfolioHandler, orderSagaHandler, shorts, marker, log)

	return &env{
		ctx: ctx, pub: pub,
		portfolioHandler: portfolioHandler,
		orderSagaHandler: orderSagaHandler,
		shorts:           shorts,
		marker:           marker,
		reactor:          reactor,
	}
}

func (e *env) flush() {
	for len(e.pub.events) > 0 {
		batch := e.pub.events
		e.pub.events = nil
		_ = e.shorts.HandleEvents(e.ctx, batch)
		_ = e.reactor.HandleEvents(e.ctx, batch)
	}
}

// seedShort opens a short on a portfolio aggregate so the reactor has
// something to evaluate against. Bypasses the sagasaga lifecycle.
func seedShort(t *testing.T, e *env, accountID, symbol string, qty, openPrice, collateral int64) {
	t.Helper()
	dep := portfolio.DepositCash{AccountID: accountID, Amount: 10 * collateral}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, dep, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, dep)
	}))
	hold := portfolio.HoldCollateral{
		AccountID: accountID, OrderSagaID: "seed-" + symbol,
		Symbol: symbol, Quantity: qty, Amount: collateral,
	}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, hold, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, hold)
	}))
	open := portfolio.OpenShort{
		AccountID: accountID, OrderSagaID: "seed-" + symbol, TradeID: "seed-trade-" + symbol,
		Symbol: symbol, Quantity: qty, PricePerShare: openPrice,
	}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, open, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteOpenShort(p, open)
	}))
}

func loadPortfolio(t *testing.T, e *env, accountID string) *portfolio.Portfolio {
	t.Helper()
	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID(accountID))
	require.NoError(t, err)
	return p
}

func loadSaga(t *testing.T, e *env, sagaID string) *ordersaga.OrderSaga {
	t.Helper()
	s, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID(sagaID))
	require.NoError(t, err)
	return s
}

// fireTrade injects a TradeExecuted event into the reactor as if the
// orderbook had emitted it. Updates marker first so margin math uses
// the new price.
func fireTrade(t *testing.T, e *env, symbol, tradeID string, price int64) {
	t.Helper()
	e.marker.set(symbol, price)
	evt := es.Event{
		Type:      "TradeExecuted",
		Timestamp: time.Now(),
		Data: &orderbookv1.TradeExecuted{
			TradeId: tradeID, Symbol: symbol, Price: price, Quantity: 1,
			ExecutedAt: timestamppb.Now(),
		},
	}
	require.NoError(t, e.reactor.HandleEvents(e.ctx, []es.Event{evt}))
	e.flush()
}

func TestReactor_TradeBreach_IssuesCallAndSpawnsLiquidation(t *testing.T) {
	e := newEnv(t)

	// Open a short: 100 AAPL @ $150 with $7500 collateral.
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	// Cash after seedShort: $750k deposit - $7500 collateral = $742.5k.
	// Mark spikes to $400 — short liability = 100 * 4M = 400M = $40k.
	// Maintenance = 30% * 400M = 120M.
	// Equity = $742.5k cash + $7500 collateral + $15k proceeds - $40k short MV
	//        = 750M + 75M + 150M - 400M = 575M = $57.5k.
	// 57.5k > 12k, NOT in call. Let me bump mark higher.
	// At mark $8M ($800): liability = 800M, maintenance = 240M.
	// Equity = 750M + 75M + 150M - 800M = 175M. > 240M? No, 175M < 240M -> BREACH.
	fireTrade(t, e, "AAPL", "trade-1", 8_000_000)

	p := loadPortfolio(t, e, "acct-1")
	require.NotNil(t, p.ActiveMarginCall, "margin call should be active")
	assert.Equal(t, "AAPL", p.ActiveMarginCall.TriggerSymbol)
	assert.Equal(t, int64(8_000_000), p.ActiveMarginCall.MarkPrice)

	// Liquidation saga should be spawned with deterministic ID.
	saga := loadSaga(t, e, "liquidation:acct-1:trade-1")
	assert.Equal(t, "AAPL", saga.Symbol)
	assert.Equal(t, int64(100), saga.Quantity)
	assert.Equal(t, orderbookv1.PositionSide_POSITION_SIDE_SHORT, saga.PositionSide)
	assert.Equal(t, orderbook.Buy, saga.Side)
}

func TestReactor_TradeNotBreached_NoCall(t *testing.T) {
	e := newEnv(t)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	// Mark drops to $100 — short is profitable, definitely not breached.
	fireTrade(t, e, "AAPL", "trade-1", 1_000_000)

	p := loadPortfolio(t, e, "acct-1")
	assert.Nil(t, p.ActiveMarginCall)
}

func TestReactor_RepeatedTradeWhileInCall_NoReissue(t *testing.T) {
	e := newEnv(t)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	fireTrade(t, e, "AAPL", "trade-1", 8_000_000)
	p := loadPortfolio(t, e, "acct-1")
	require.NotNil(t, p.ActiveMarginCall)
	origCallID := p.ActiveMarginCall.CallID

	// Another trade at the same level — should not re-issue.
	fireTrade(t, e, "AAPL", "trade-2", 8_000_000)
	p = loadPortfolio(t, e, "acct-1")
	require.NotNil(t, p.ActiveMarginCall)
	assert.Equal(t, origCallID, p.ActiveMarginCall.CallID, "no re-issue while same call active")
}

func TestReactor_ChainedLiquidation_OnRepeatTrade(t *testing.T) {
	e := newEnv(t)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	fireTrade(t, e, "AAPL", "trade-1", 8_000_000)
	// First liquidation spawned for trade-1.
	loadSaga(t, e, "liquidation:acct-1:trade-1")

	fireTrade(t, e, "AAPL", "trade-2", 8_000_000)
	// Chained liquidation for trade-2 (different sagaID, same target).
	saga2 := loadSaga(t, e, "liquidation:acct-1:trade-2")
	assert.Equal(t, "AAPL", saga2.Symbol)
}

func TestReactor_RemediatedByMarkRevert_EmitsCover(t *testing.T) {
	e := newEnv(t)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	fireTrade(t, e, "AAPL", "trade-up", 8_000_000)
	require.NotNil(t, loadPortfolio(t, e, "acct-1").ActiveMarginCall)

	// Mark drops back to comfortable level.
	fireTrade(t, e, "AAPL", "trade-down", 1_500_000)
	assert.Nil(t, loadPortfolio(t, e, "acct-1").ActiveMarginCall)
}

func TestReactor_NoShortInSymbol_Ignored(t *testing.T) {
	e := newEnv(t)
	// No short anywhere.
	fireTrade(t, e, "NVDA", "trade-1", 1_000_000_000)
	// No portfolio loaded means no aggregate exists; just verify
	// the reactor didn't blow up.
}

func TestReactor_TradeOnDifferentSymbol_DoesntFalsePositive(t *testing.T) {
	e := newEnv(t)
	// Short in AAPL.
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	// NVDA trade — irrelevant to AAPL short.
	fireTrade(t, e, "NVDA", "trade-1", 999_999_999)
	assert.Nil(t, loadPortfolio(t, e, "acct-1").ActiveMarginCall)
}

func TestReactor_CashDeposit_ClearsCall(t *testing.T) {
	e := newEnv(t)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	fireTrade(t, e, "AAPL", "trade-up", 8_000_000)
	require.NotNil(t, loadPortfolio(t, e, "acct-1").ActiveMarginCall)

	// Big cash deposit should restore margin without needing a trade.
	dep := portfolio.DepositCash{AccountID: "acct-1", Amount: 1_000_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, dep, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, dep)
	}))
	e.flush()

	assert.Nil(t, loadPortfolio(t, e, "acct-1").ActiveMarginCall)
}

func TestReactor_CashWithdrawal_TriggersCall(t *testing.T) {
	e := newEnv(t)
	// Seed short at a margin-comfortable level — not in call.
	// seedShort deposits 10*collateral = 750M and consumes 75M as
	// collateral, leaving 675M unencumbered cash.
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.marker.set("AAPL", 1_800_000) // mark slightly above open
	e.flush()
	require.Nil(t, loadPortfolio(t, e, "acct-1").ActiveMarginCall)

	// Drain all unencumbered cash. After: equity = 0 + 75M coll +
	// 150M proceeds - 180M short MV = 45M; maintenance = 0.30 * 180M
	// = 54M; 45M < 54M → in call.
	wd := portfolio.WithdrawCash{AccountID: "acct-1", Amount: 675_000_000}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, wd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteWithdrawCash(p, wd)
	}))
	e.flush()

	assert.NotNil(t, loadPortfolio(t, e, "acct-1").ActiveMarginCall)
}
