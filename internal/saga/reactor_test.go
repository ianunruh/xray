package saga_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/saga"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

// collectingPublisher records published events so tests can flush them to the
// reactor synchronously, simulating the NATS publish→consume cycle.
type collectingPublisher struct {
	events []es.Event
}

func (p *collectingPublisher) Publish(_ context.Context, events []es.Event) error {
	p.events = append(p.events, events...)
	return nil
}

func (p *collectingPublisher) flush(ctx context.Context, reactor *saga.Reactor) {
	for len(p.events) > 0 {
		batch := p.events
		p.events = nil
		reactor.HandleEvents(ctx, batch)
	}
}

type reactorTestEnv struct {
	ctx         context.Context
	obHandler   *es.Handler[*orderbook.OrderBook]
	sagaHandler *es.Handler[*saga.BracketSaga]
	reactor     *saga.Reactor
	store       *memstore.Store
	registry    *es.Registry
	pub         *collectingPublisher
}

func (e *reactorTestEnv) flush() {
	e.pub.flush(e.ctx, e.reactor)
}

func setupReactorTest(t *testing.T) *reactorTestEnv {
	t.Helper()

	registry := newTestRegistry()
	store := memstore.New()
	ctx := context.Background()
	pub := &collectingPublisher{}

	obHandler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default()).WithPublisher(pub)

	sagaHandler := es.NewHandler(store, registry, func(id string) *saga.BracketSaga {
		return saga.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := saga.NewReactor(sagaHandler, obHandler, slog.Default())
	reactor.SetReady()

	return &reactorTestEnv{
		ctx:         ctx,
		obHandler:   obHandler,
		sagaHandler: sagaHandler,
		reactor:     reactor,
		store:       store,
		registry:    registry,
		pub:         pub,
	}
}

func TestReactor_BracketOrder_FullLifecycle(t *testing.T) {
	env := setupReactorTest(t)

	// Place resting sell liquidity at $150 (for entry to match).
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	// Drain these events — they're not saga-related.
	env.pub.events = nil

	// Place the entry buy order at $150. This will match the resting sell.
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	// Start the saga.
	sagaID := "bracket-test-1"
	startCmd := saga.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Flush events to reactor: it will see SagaStarted + TradeExecuted (from entry match).
	// The reactor should detect the entry fill and place exit orders.
	env.flush()

	// Verify saga transitioned to PendingExit.
	sagaAgg, err := env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.PendingExit, sagaAgg.Status)
	assert.NotEmpty(t, sagaAgg.TakeProfitOrderID)
	assert.NotEmpty(t, sagaAgg.StopLossOrderID)

	// Now fill the take-profit: place a buy at $155 to match the TP sell exit.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)
	env.flush()

	// Verify saga completed.
	sagaAgg, err = env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.Completed, sagaAgg.Status)

	// Verify the full saga event stream.
	raw, err := env.store.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)

	types := make([]string, len(raw))
	for i, r := range raw {
		evt, err := env.registry.Deserialize(r)
		require.NoError(t, err)
		types[i] = evt.Type
	}
	assert.Equal(t, []string{"SagaStarted", "EntryFilled", "ExitFilled", "SagaCompleted"}, types)
}

func TestReactor_EntryCancelled_SagaFails(t *testing.T) {
	env := setupReactorTest(t)

	// Place entry order (no matching liquidity).
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)
	env.pub.events = nil

	// Start the saga.
	sagaID := "fail-test-1"
	startCmd := saga.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Flush SagaStarted to reactor.
	env.flush()

	// Cancel the entry order.
	cancelCmd := orderbook.CancelOrder{Symbol: "AAPL", OrderID: entryOrderID}
	err = env.obHandler.Handle(env.ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	require.NoError(t, err)

	// Flush OrderCancelled to reactor.
	env.flush()

	// Verify saga failed.
	sagaAgg, err := env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.Failed, sagaAgg.Status)
}

func TestReactor_PartialFill_WaitsForFullFill(t *testing.T) {
	env := setupReactorTest(t)

	// Place partial sell liquidity: 60 shares at $150.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 60)
	env.pub.events = nil

	// Entry buy order for 100 shares. Only 60 will fill.
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	// Start the saga.
	sagaID := "partial-test-1"
	startCmd := saga.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Flush: reactor sees 60-share trade, but entry needs 100.
	env.flush()

	// Saga should still be PendingEntry.
	sagaAgg, err := env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.PendingEntry, sagaAgg.Status)

	// Place more sell liquidity to fill the remaining 40.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 40)

	// The resting buy for 40 remaining will match. Flush.
	env.flush()

	// Now saga should be PendingExit.
	sagaAgg, err = env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.PendingExit, sagaAgg.Status)
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
