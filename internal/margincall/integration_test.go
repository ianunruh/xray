package margincall_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/margincall"
	"github.com/ianunruh/xray/internal/ocosaga"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

// integrationEnv stands up the full reactor + projection mesh in
// memory and drives events synchronously via a collectingPublisher
// drain loop. Models what the production NATS consumer wiring does
// minus the network — enough fidelity to catch cross-reactor
// regressions the per-reactor unit tests don't see.
type integrationEnv struct {
	ctx context.Context
	pub *collectingPublisher

	obHandler        *es.Handler[*orderbook.OrderBook]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	ocoSagaHandler   *es.Handler[*ocosaga.OCOSaga]
	bracketHandler   *es.Handler[*bracket.BracketSaga]

	marks       *orderbook.MarkProjection
	shorts      *portfolio.InMemoryShortsBySymbol
	activeCalls *portfolio.InMemoryActiveMarginCalls

	orderSagaReactor *ordersaga.Reactor
	ocoSagaReactor   *ocosaga.Reactor
	bracketReactor   *bracket.Reactor
	marginReactor    *margincall.Reactor

	sagaLookup *stubSagaLookup
}

func newIntegrationEnv(t *testing.T) *integrationEnv {
	t.Helper()
	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)
	portfolio.RegisterEvents(registry)
	ordersaga.RegisterEvents(registry)
	ocosaga.RegisterEvents(registry)
	bracket.RegisterEvents(registry)

	store := memstore.New()
	pub := &collectingPublisher{}
	log := slog.Default()
	ctx := context.Background()

	obHandler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, log).WithPublisher(pub)
	portfolioHandler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, log).WithPublisher(pub)
	orderSagaHandler := es.NewHandler(store, registry, func(id string) *ordersaga.OrderSaga {
		return ordersaga.NewOrderSaga(id)
	}, log).WithPublisher(pub)
	ocoSagaHandler := es.NewHandler(store, registry, func(id string) *ocosaga.OCOSaga {
		return ocosaga.NewOCOSaga(id)
	}, log).WithPublisher(pub)
	bracketHandler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, log).WithPublisher(pub)

	marks := orderbook.NewMarkProjection()
	shorts := portfolio.NewInMemoryShortsBySymbol()
	activeCalls := portfolio.NewInMemoryActiveMarginCalls()
	sagaLookup := &stubSagaLookup{}

	orderSagaReactor := ordersaga.NewReactor(orderSagaHandler, portfolioHandler, obHandler, log)
	ocoSagaReactor := ocosaga.NewReactor(ocoSagaHandler, portfolioHandler, obHandler, log)
	bracketReactor := bracket.NewReactor(bracketHandler, orderSagaHandler, ocoSagaHandler, obHandler, log)
	// Grace=0 for the integration test — we want to assert the
	// immediate-liquidation path end-to-end without driving a clock.
	marginReactor := margincall.NewReactor(portfolioHandler, orderSagaHandler, obHandler,
		shorts, sagaLookup, marks, margincall.Config{Grace: 0}, log)

	return &integrationEnv{
		ctx:              ctx,
		pub:              pub,
		obHandler:        obHandler,
		portfolioHandler: portfolioHandler,
		orderSagaHandler: orderSagaHandler,
		ocoSagaHandler:   ocoSagaHandler,
		bracketHandler:   bracketHandler,
		marks:            marks,
		shorts:           shorts,
		activeCalls:      activeCalls,
		orderSagaReactor: orderSagaReactor,
		ocoSagaReactor:   ocoSagaReactor,
		bracketReactor:   bracketReactor,
		marginReactor:    marginReactor,
		sagaLookup:       sagaLookup,
	}
}

// drain runs the projections + reactors over every queued event,
// then loops as long as new events show up. Models the per-batch
// dispatch the NATS consumer does, except synchronously.
func (e *integrationEnv) drain() {
	for len(e.pub.events) > 0 {
		batch := e.pub.events
		e.pub.events = nil

		// Projections first (in-memory state the reactors read).
		_ = e.marks.HandleEvents(e.ctx, batch)
		_ = e.shorts.HandleEvents(e.ctx, batch)
		_ = e.activeCalls.HandleEvents(e.ctx, batch)

		// Reactors (order matters less than projections — each acts
		// idempotently on its own slice of event types).
		_ = e.orderSagaReactor.HandleEvents(e.ctx, batch)
		_ = e.ocoSagaReactor.HandleEvents(e.ctx, batch)
		_ = e.bracketReactor.HandleEvents(e.ctx, batch)
		_ = e.marginReactor.HandleEvents(e.ctx, batch)
	}
}

func (e *integrationEnv) loadPortfolio(t *testing.T, accountID string) *portfolio.Portfolio {
	t.Helper()
	p, err := e.portfolioHandler.Load(e.ctx, portfolio.AggregateID(accountID))
	require.NoError(t, err)
	return p
}

// depositCash bypasses the saga and deposits directly. The integration
// test exercises post-deposit flow, not the funding path itself.
func (e *integrationEnv) depositCash(t *testing.T, accountID string, amount int64) {
	t.Helper()
	cmd := portfolio.DepositCash{AccountID: accountID, Amount: amount}
	require.NoError(t, e.portfolioHandler.Handle(e.ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteDepositCash(p, cmd)
	}))
}

func (e *integrationEnv) placeLimit(t *testing.T, symbol, accountID string, side orderbook.Side, price, qty int64) string {
	t.Helper()
	cmd := orderbook.PlaceOrder{
		Symbol:    symbol,
		Side:      side,
		Price:     price,
		Quantity:  qty,
		OrderType: orderbook.Limit,
		AccountID: accountID,
	}
	var orderID string
	err := e.obHandler.Handle(e.ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		evts, err := orderbook.ExecutePlaceOrder(book, cmd)
		if err != nil {
			return nil, err
		}
		for _, ev := range evts {
			if placed, ok := ev.Data.(*orderbookv1.OrderPlaced); ok {
				orderID = placed.OrderId
				break
			}
		}
		return evts, nil
	})
	require.NoError(t, err)
	return orderID
}

func (e *integrationEnv) startShortOpenSaga(t *testing.T, accountID, sagaID, symbol string, price, qty int64) {
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
		Initiator:    sagav1.Initiator_INITIATOR_USER,
	}
	require.NoError(t, e.orderSagaHandler.Handle(e.ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	}))
}

// TestIntegration_MarginCallEndToEnd is the full lifecycle:
//
//	deposit -> open short -> mark spike triggers call
//	-> liquidation saga spawns -> cover fills -> call resolves
//
// Lots of moving pieces; the test is intentionally narrative.
func TestIntegration_MarginCallEndToEnd(t *testing.T) {
	e := newIntegrationEnv(t)
	const acct = "short-acct"
	const symbol = "AAPL"

	// === Phase 1: account is funded with $75k (just enough to back
	// the collateral on a 100-share short with modest cushion) ===
	e.depositCash(t, acct, 750_000_000)

	// Counterparty providing bid liquidity for the short-open to lift.
	const cp = "counterparty"
	e.depositCash(t, cp, 100_000_000_000)
	e.placeLimit(t, symbol, cp, orderbook.Buy, 1_500_000, 100)
	e.drain()

	// === Phase 2: open a 100-share short at $150 ===
	e.startShortOpenSaga(t, acct, "user-short", symbol, 1_500_000, 100)
	e.drain()

	p := e.loadPortfolio(t, acct)
	require.NotNil(t, p.ShortPositions[symbol], "short should be open after fill")
	require.Equal(t, int64(100), p.ShortPositions[symbol].Quantity)
	require.Nil(t, p.ActiveMarginCall, "no call yet — mark is still at open price")

	// === Phase 3: pre-place ASK liquidity at $810 for the eventual
	// liquidation buy-to-cover. Has to exist BEFORE the spike, since
	// the liquidation saga places (and IOC-cancels) in the same
	// batch as the trade that triggers it. ===
	e.placeLimit(t, symbol, cp, orderbook.Sell, 8_100_000, 100)
	e.drain()

	// === Phase 4: spike the mark via a tiny trade at $800. Trade
	// price (not the standing ask) is what feeds the MarkProjection,
	// so a 1-share print is enough.
	const aggressor = "buyer"
	e.depositCash(t, aggressor, 100_000_000_000)
	e.placeLimit(t, symbol, cp, orderbook.Sell, 8_000_000, 1)
	e.placeLimit(t, symbol, aggressor, orderbook.Buy, 8_000_000, 1)
	e.drain()

	// All of the following happens inside the single drain above:
	// trade -> mark update -> call issued -> liquidation saga ->
	// cover trade -> call covered. So instead of asserting "call is
	// active right now" (it isn't — it's already resolved), we verify
	// the events fired by scanning the publisher's log.
	assertEventOfType[*portfoliov1.MarginCallIssued](t, e, "MarginCallIssued never fired")
	assertEventOfType[*portfoliov1.MarginCallCovered](t, e, "MarginCallCovered never fired")

	liqSaga := findLiquidationSaga(t, e)
	require.NotNil(t, liqSaga, "liquidation saga should have been spawned")
	assert.Equal(t, symbol, liqSaga.Symbol)
	assert.Equal(t, int64(100), liqSaga.Quantity)
	assert.Equal(t, sagav1.Initiator_INITIATOR_MARGIN_CALL, liqSaga.Initiator)
	assert.Equal(t, ordersaga.Completed, liqSaga.Status, "liquidation saga should have completed")

	// === Phase 6: short is gone, call is cleared ===
	p = e.loadPortfolio(t, acct)
	assert.Empty(t, p.ShortPositions, "short fully covered")
	assert.Nil(t, p.ActiveMarginCall, "call resolved after cover")

	// Loss is large enough that final cash should be deeply negative
	// (we shorted at $150 and covered around $810). Verify
	// directionally rather than exact-figure: cash must be much less
	// than the original $75k deposit.
	assert.Less(t, p.CashBalance, int64(750_000_000),
		"loss should have reduced cash from the initial deposit")
}

// assertEventOfType verifies at least one event of type T appeared
// in the publisher's log over the course of the test.
func assertEventOfType[T any](t *testing.T, e *integrationEnv, msg string) {
	t.Helper()
	for _, evt := range e.pub.all {
		if _, ok := evt.Data.(T); ok {
			return
		}
	}
	t.Errorf("%s", msg)
}

// findLiquidationSaga scans the publisher's event log for a
// MARGIN_CALL-initiated OrderSagaStarted, then loads that saga.
// Brute force — the integration test only creates one liquidation.
func findLiquidationSaga(t *testing.T, e *integrationEnv) *ordersaga.OrderSaga {
	t.Helper()
	for _, evt := range e.pub.all {
		started, ok := evt.Data.(*portfoliov1.OrderSagaStarted)
		if !ok {
			continue
		}
		if started.Initiator != sagav1.Initiator_INITIATOR_MARGIN_CALL {
			continue
		}
		if !strings.HasPrefix(started.SagaId, "liquidation:") {
			continue
		}
		saga, err := e.orderSagaHandler.Load(e.ctx, ordersaga.AggregateID(started.SagaId))
		require.NoError(t, err)
		return saga
	}
	return nil
}
