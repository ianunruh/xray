package margincall_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/margincall"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

type collectingPublisher struct {
	events []es.Event // queue drained each batch
	all    []es.Event // never cleared; used by integration tests for replay
}

func (p *collectingPublisher) Publish(_ context.Context, events []es.Event) error {
	p.events = append(p.events, events...)
	p.all = append(p.all, events...)
	return nil
}

type fakeMarker struct {
	prices map[string]int64
}

// stubSagaLookup is a no-op SagaLookup for tests that don't exercise
// the user-saga cancellation path. Tests that do can populate sagas.
type stubSagaLookup struct {
	sagas []portfolio.ActiveSaga
}

func (s *stubSagaLookup) ActiveSingleOrderSagas(_ context.Context, _ string) ([]portfolio.ActiveSaga, error) {
	return s.sagas, nil
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
	obHandler        *es.Handler[*orderbook.OrderBook]
	shorts           *portfolio.InMemoryShortsBySymbol
	longs            *portfolio.InMemoryLongsBySymbol
	sagas            *stubSagaLookup
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
	obHandler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, log).WithPublisher(pub)

	shorts := portfolio.NewInMemoryShortsBySymbol()
	longs := portfolio.NewInMemoryLongsBySymbol()
	marker := &fakeMarker{prices: make(map[string]int64)}
	sagas := &stubSagaLookup{}
	reactor := margincall.NewReactor(portfolioHandler, orderSagaHandler, obHandler, shorts, longs, sagas, marker,
		margincall.Config{Grace: 0}, log)

	return &env{
		ctx: ctx, pub: pub,
		portfolioHandler: portfolioHandler,
		orderSagaHandler: orderSagaHandler,
		obHandler:        obHandler,
		shorts:           shorts,
		longs:            longs,
		sagas:            sagas,
		marker:           marker,
		reactor:          reactor,
	}
}

func (e *env) flush() {
	for len(e.pub.events) > 0 {
		batch := e.pub.events
		e.pub.events = nil
		_ = e.shorts.HandleEvents(e.ctx, batch)
		_ = e.longs.HandleEvents(e.ctx, batch)
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

// seedLongOnMargin makes an account that bought stock on margin.
// Bypasses the saga; sets aggregate state directly.
func seedLongOnMargin(t *testing.T, e *env, accountID, symbol string, qty, cost int64) {
	t.Helper()
	// Skip a "deposit cash" and a "settle buy" sequence — just push
	// the aggregate to the state we need: held a long position via
	// CashSettled with no prior cash. Simulates buying on margin.
	settle := portfolio.SettleTrade{
		AccountID:    accountID,
		OrderSagaID:  "seed-buy-" + symbol,
		TradeID:      "seed-tr-" + symbol,
		Amount:       cost,
		Symbol:       symbol,
		Quantity:     qty,
		CostPerShare: cost / qty,
	}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, settle, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleTrade(p, settle)
	}))
}

func TestReactor_LongOnMarginBreach_SpawnsSellLiquidation(t *testing.T) {
	e := newEnv(t)
	// Stock bought on margin: 100 shares costing $1k total ($10/share).
	// CashBalance goes to -$1k after settlement → loan = $1k.
	seedLongOnMargin(t, e, "acct-1", "NVDA", 100, 10_000_000)
	e.flush()

	// Crash the mark to $5/share. Long MV = $500; equity = -$1k +
	// $500 = -$500. Maintenance = 25% * $500 = $125. Equity << maint.
	fireTrade(t, e, "NVDA", "trade-down", 50_000)

	p := loadPortfolio(t, e, "acct-1")
	require.NotNil(t, p.ActiveMarginCall, "margin call should fire")

	// Liquidation saga should be a SELL of the long, not a buy-cover.
	liqSaga, err := e.orderSagaHandler.Load(e.ctx,
		ordersaga.AggregateID("liquidation:acct-1:trade-down"))
	require.NoError(t, err)
	assert.Equal(t, orderbook.Sell, liqSaga.Side)
	assert.Equal(t, orderbookv1.PositionSide_POSITION_SIDE_LONG, liqSaga.PositionSide)
	assert.Equal(t, "NVDA", liqSaga.Symbol)
	assert.Equal(t, int64(100), liqSaga.Quantity)
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

func TestReactor_CancelsUserSagaOnIssue(t *testing.T) {
	e := newEnv(t)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	// Stand up a fake user-initiated order saga with an OrderID set
	// (i.e. the order is resting on the book). The cancel path will
	// look at saga.OrderID, so the saga aggregate's view is what
	// matters; we don't need a real order on the book for this unit
	// test of the cancellation dispatch.
	userSagaID := "user-saga-1"
	startCmd := ordersaga.StartOrderSaga{
		SagaID:    userSagaID,
		AccountID: "acct-1",
		Symbol:    "NVDA",
		Side:      orderbookv1.Side_SIDE_BUY,
		Price:     1_000_000,
		Quantity:  1,
		OrderType: orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		Initiator: sagav1.Initiator_INITIATOR_USER,
	}
	require.NoError(t, e.orderSagaHandler.Handle(e.ctx, startCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, startCmd)
	}))
	// Populate the lookup so the reactor finds it.
	e.sagas.sagas = []portfolio.ActiveSaga{{SagaID: userSagaID, AccountID: "acct-1"}}
	e.pub.events = nil

	// Trigger a margin call by spiking the mark.
	fireTrade(t, e, "AAPL", "trade-up", 8_000_000)

	// User saga should now be failed with margin_call reason.
	userSaga, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID(userSagaID))
	require.NoError(t, err)
	assert.Equal(t, ordersaga.Failed, userSaga.Status)
}

func TestReactor_SkipsMarginCallSagasOnCancel(t *testing.T) {
	e := newEnv(t)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	// Seed a prior margin-call-initiated saga. The cancel pass must
	// NOT touch it.
	mcSagaID := "mc-saga"
	startCmd := ordersaga.StartOrderSaga{
		SagaID:    mcSagaID,
		AccountID: "acct-1",
		Symbol:    "AAPL",
		Side:      orderbookv1.Side_SIDE_BUY,
		Quantity:  1,
		OrderType: orderbookv1.OrderType_ORDER_TYPE_MARKET,
		Initiator: sagav1.Initiator_INITIATOR_MARGIN_CALL,
	}
	require.NoError(t, e.orderSagaHandler.Handle(e.ctx, startCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, startCmd)
	}))
	e.sagas.sagas = []portfolio.ActiveSaga{{SagaID: mcSagaID, AccountID: "acct-1"}}
	e.pub.events = nil

	fireTrade(t, e, "AAPL", "trade-up", 8_000_000)

	mcSaga, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID(mcSagaID))
	require.NoError(t, err)
	assert.NotEqual(t, ordersaga.Failed, mcSaga.Status,
		"margin-call-initiated saga must be left alone")
}

// newEnvWithGrace mirrors newEnv but lets the test pick a grace
// window. Used for the grace-period tests; everything else uses the
// zero-grace default (immediate liquidation, simpler to assert).
func newEnvWithGrace(t *testing.T, grace time.Duration) *env {
	t.Helper()
	e := newEnv(t)
	e.reactor = margincall.NewReactor(e.portfolioHandler, e.orderSagaHandler, e.obHandler,
		e.shorts, e.longs, e.sagas, e.marker, margincall.Config{Grace: grace}, slog.Default())
	return e
}

func TestReactor_DefersLiquidationWhenGraceConfigured(t *testing.T) {
	e := newEnvWithGrace(t, 30*time.Second)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	fireTrade(t, e, "AAPL", "trade-up", 8_000_000)

	// Call is open but no liquidation saga should exist yet.
	p := loadPortfolio(t, e, "acct-1")
	require.NotNil(t, p.ActiveMarginCall)

	_, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID("liquidation:acct-1:trade-up"))
	require.NoError(t, err)
	// Aggregate Load returns a fresh empty saga for a missing ID;
	// verify Version() is 0 to confirm it doesn't exist.
	saga, _ := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID("liquidation:acct-1:trade-up"))
	assert.Equal(t, 0, saga.Version(), "liquidation saga should NOT have spawned yet")
}

func TestReactor_GraceExpiry_SpawnsLiquidation(t *testing.T) {
	e := newEnvWithGrace(t, 30*time.Second)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	fireTrade(t, e, "AAPL", "trade-up", 8_000_000)
	p := loadPortfolio(t, e, "acct-1")
	require.NotNil(t, p.ActiveMarginCall)
	issuedAt := p.ActiveMarginCall.IssuedAt

	// Tick before grace expires — no-op.
	require.NoError(t, e.reactor.EvaluateGraceExpiry(e.ctx, "acct-1", issuedAt.Add(10*time.Second)))
	saga, _ := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID(
		graceSagaID("acct-1", issuedAt, 0)))
	assert.Equal(t, 0, saga.Version(), "no spawn before grace")

	// Tick after grace expires — liquidation should spawn in bucket 1.
	require.NoError(t, e.reactor.EvaluateGraceExpiry(e.ctx, "acct-1", issuedAt.Add(45*time.Second)))
	saga, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID(
		graceSagaID("acct-1", issuedAt, 1)))
	require.NoError(t, err)
	assert.NotEqual(t, 0, saga.Version(), "liquidation saga spawned after grace")
}

func TestReactor_GraceExpiry_RetriesAcrossBucketsWhenFailed(t *testing.T) {
	// When the first attempt's saga ended in Failed (e.g. market IOC
	// against an empty book), a subsequent tick should spawn a NEW
	// saga rather than retry the same one (which would just hit
	// ErrInvalidState and never actually retry the liquidation).
	e := newEnvWithGrace(t, 30*time.Second)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	fireTrade(t, e, "AAPL", "trade-up", 8_000_000)
	p := loadPortfolio(t, e, "acct-1")
	require.NotNil(t, p.ActiveMarginCall)
	issuedAt := p.ActiveMarginCall.IssuedAt

	// First grace tick spawns bucket 1.
	require.NoError(t, e.reactor.EvaluateGraceExpiry(e.ctx, "acct-1", issuedAt.Add(35*time.Second)))
	bucket1ID := graceSagaID("acct-1", issuedAt, 1)
	s1, _ := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID(bucket1ID))
	require.NotEqual(t, 0, s1.Version())

	// Simulate the saga failing (no liquidity → IOC cancelled).
	failCmd := ordersaga.RecordFailed{SagaID: bucket1ID, Reason: "no liquidity"}
	require.NoError(t, e.orderSagaHandler.Handle(e.ctx, failCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteRecordFailed(s, failCmd)
	}))

	// Next grace tick (one bucket later). Should spawn bucket 2,
	// NOT reuse bucket 1 (which would fail with ErrInvalidState).
	require.NoError(t, e.reactor.EvaluateGraceExpiry(e.ctx, "acct-1", issuedAt.Add(65*time.Second)))
	bucket2ID := graceSagaID("acct-1", issuedAt, 2)
	s2, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID(bucket2ID))
	require.NoError(t, err)
	assert.NotEqual(t, 0, s2.Version(), "fresh bucket spawns a new attempt")
}

func TestReactor_GraceExpiry_CoversWhenBreachResolved(t *testing.T) {
	e := newEnvWithGrace(t, 30*time.Second)
	seedShort(t, e, "acct-1", "AAPL", 100, 1_500_000, 75_000_000)
	e.flush()

	fireTrade(t, e, "AAPL", "trade-up", 8_000_000)
	p := loadPortfolio(t, e, "acct-1")
	require.NotNil(t, p.ActiveMarginCall)

	// Mark snaps back below the call threshold before grace expires.
	e.marker.set("AAPL", 1_500_000)

	require.NoError(t, e.reactor.EvaluateGraceExpiry(e.ctx, "acct-1",
		p.ActiveMarginCall.IssuedAt.Add(45*time.Second)))

	p = loadPortfolio(t, e, "acct-1")
	assert.Nil(t, p.ActiveMarginCall, "call cleared by grace-expiry recheck")
}

// graceSagaID mirrors the reactor's bucket-based trigger derivation.
func graceSagaID(accountID string, issuedAt time.Time, bucket int64) string {
	return fmt.Sprintf("liquidation:%s:grace:%d:bucket:%d",
		accountID, issuedAt.UnixNano(), bucket)
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
