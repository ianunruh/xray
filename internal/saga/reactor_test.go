package saga_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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
	reactor.SetReady(ctx)

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

func setupReactorTestNotReady(t *testing.T) *reactorTestEnv {
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

func TestReactor_Recovery_EntryFilledDuringReplay(t *testing.T) {
	env := setupReactorTestNotReady(t)

	// Place resting sell liquidity at $150.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	// Place entry buy at $150 — matches immediately.
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	// Start the saga.
	sagaID := "recovery-entry-fill-1"
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

	// Flush events while NOT ready — reactor accumulates filledQty but won't place exits.
	env.flush()

	sagaAgg, err := env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.PendingEntry, sagaAgg.Status)

	// SetReady triggers recovery — should place exit orders and record EntryFilled.
	env.reactor.SetReady(env.ctx)
	env.flush()

	sagaAgg, err = env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.PendingExit, sagaAgg.Status)
	assert.NotEmpty(t, sagaAgg.TakeProfitOrderID)
	assert.NotEmpty(t, sagaAgg.StopLossOrderID)
}

func TestReactor_Recovery_EntryCancelledDuringReplay(t *testing.T) {
	env := setupReactorTestNotReady(t)

	// Place entry order (no matching liquidity).
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)
	env.pub.events = nil

	// Start the saga.
	sagaID := "recovery-cancel-1"
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
	env.flush()

	// Cancel the entry order.
	cancelCmd := orderbook.CancelOrder{Symbol: "AAPL", OrderID: entryOrderID}
	err = env.obHandler.Handle(env.ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	require.NoError(t, err)
	env.flush()

	// Saga should still be PendingEntry (side-effect suppressed during replay).
	sagaAgg, err := env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.PendingEntry, sagaAgg.Status)

	// SetReady triggers recovery — should record SagaFailed.
	env.reactor.SetReady(env.ctx)
	env.flush()

	sagaAgg, err = env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.Failed, sagaAgg.Status)
}

func TestReactor_Recovery_ExitFilledDuringReplay(t *testing.T) {
	// Phase 1: Use a ready reactor to drive the saga to PendingExit.
	env := setupReactorTest(t)

	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	sagaID := "recovery-exit-fill-1"
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
	env.flush()

	sagaAgg, err := env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	require.Equal(t, saga.PendingExit, sagaAgg.Status)

	// Fill the take-profit exit order (don't flush to the ready reactor).
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)
	env.pub.events = nil

	// Phase 2: Create a fresh not-ready reactor and replay all events from the store.
	reactor2 := saga.NewReactor(env.sagaHandler, env.obHandler, slog.Default())

	allRaw, err := env.store.LoadAll(env.ctx)
	require.NoError(t, err)

	var replayEvents []es.Event
	for _, raw := range allRaw {
		evt, err := env.registry.Deserialize(raw)
		require.NoError(t, err)
		replayEvents = append(replayEvents, evt)
	}

	err = reactor2.HandleEvents(env.ctx, replayEvents)
	require.NoError(t, err)

	// SetReady triggers recovery — should cancel sibling and record ExitFilled.
	reactor2.SetReady(env.ctx)

	sagaAgg, err = env.sagaHandler.Load(env.ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.Completed, sagaAgg.Status)
}

// failingStore wraps a store and fails Append calls for specific event types.
type failingStore struct {
	es.EventStore
	mu        sync.Mutex
	failTypes map[string]int // event type -> remaining failures
}

func (s *failingStore) Append(ctx context.Context, aggregateID string, expectedVersion int, events []es.RawEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, evt := range events {
		if n, ok := s.failTypes[evt.Type]; ok && n > 0 {
			s.failTypes[evt.Type] = n - 1
			return fmt.Errorf("simulated failure for %s", evt.Type)
		}
	}
	return s.EventStore.Append(ctx, aggregateID, expectedVersion, events)
}

func TestReactor_Retry_RecordExitFilledFailure(t *testing.T) {
	registry := newTestRegistry()
	realStore := memstore.New()
	fStore := &failingStore{EventStore: realStore, failTypes: map[string]int{}}
	ctx := context.Background()
	pub := &collectingPublisher{}

	obHandler := es.NewHandler(realStore, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default()).WithPublisher(pub)

	sagaHandler := es.NewHandler(fStore, registry, func(id string) *saga.BracketSaga {
		return saga.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := saga.NewReactor(sagaHandler, obHandler, slog.Default())
	reactor.SetReady(ctx)

	env := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		reactor: reactor, store: realStore, registry: registry, pub: pub,
	}

	// Drive saga to PendingExit.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	sagaID := "retry-exit-1"
	startCmd := saga.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 100,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := sagaHandler.Handle(ctx, startCmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)
	env.flush()

	sagaAgg, err := sagaHandler.Load(ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	require.Equal(t, saga.PendingExit, sagaAgg.Status)

	// Make ExitFilled fail once — the reactor will emit SagaActionFailed,
	// then retry successfully in the same flush cycle.
	fStore.mu.Lock()
	fStore.failTypes["ExitFilled"] = 1
	fStore.mu.Unlock()

	// Fill the TP exit.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)
	env.flush()

	// Verify saga completed (retry succeeded after transient failure).
	sagaAgg, err = sagaHandler.Load(ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.Completed, sagaAgg.Status)

	// Verify that SagaActionFailed appears in the event stream (retry happened).
	raw, err := realStore.Load(ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	var types []string
	for _, r := range raw {
		evt, err := registry.Deserialize(r)
		require.NoError(t, err)
		types = append(types, evt.Type)
	}
	assert.Equal(t, []string{
		"SagaStarted", "EntryFilled", "SagaActionFailed",
		"ExitFilled", "SagaCompleted",
	}, types)
}

func TestReactor_Retry_MaxRetriesExceeded(t *testing.T) {
	registry := newTestRegistry()
	realStore := memstore.New()
	fStore := &failingStore{EventStore: realStore, failTypes: map[string]int{}}
	ctx := context.Background()
	pub := &collectingPublisher{}

	obHandler := es.NewHandler(realStore, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default()).WithPublisher(pub)

	sagaHandler := es.NewHandler(fStore, registry, func(id string) *saga.BracketSaga {
		return saga.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := saga.NewReactor(sagaHandler, obHandler, slog.Default())
	reactor.SetReady(ctx)

	env := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		reactor: reactor, store: realStore, registry: registry, pub: pub,
	}

	// Drive saga to PendingExit.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	sagaID := "retry-max-1"
	startCmd := saga.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 100,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := sagaHandler.Handle(ctx, startCmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)
	env.flush()

	// Make ExitFilled always fail — force max retries.
	fStore.mu.Lock()
	fStore.failTypes["ExitFilled"] = 999
	fStore.mu.Unlock()

	// Fill the TP exit.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)

	// Flush repeatedly until retries are exhausted.
	for i := 0; i < saga.MaxActionAttempts+1; i++ {
		env.flush()
	}

	sagaAgg, err := sagaHandler.Load(ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, saga.Failed, sagaAgg.Status)
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
