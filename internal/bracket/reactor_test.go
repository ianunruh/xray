package bracket_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/orderbook"
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

func (p *collectingPublisher) flush(ctx context.Context, reactor *bracket.Reactor) {
	for len(p.events) > 0 {
		batch := p.events
		p.events = nil
		reactor.HandleEvents(ctx, batch)
	}
}

type reactorTestEnv struct {
	ctx         context.Context
	obHandler   *es.Handler[*orderbook.OrderBook]
	sagaHandler *es.Handler[*bracket.BracketSaga]
	reactor     *bracket.Reactor
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

	sagaHandler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := bracket.NewReactor(sagaHandler, obHandler, slog.Default())
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

	// Start the bracket.
	sagaID := "bracket-test-1"
	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Flush events to reactor: it will see SagaStarted + TradeExecuted (from entry match).
	// The reactor should detect the entry fill and place exit orders.
	env.flush()

	// Verify saga transitioned to PendingExit.
	sagaAgg, err := env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.PendingExit, sagaAgg.Status)
	assert.NotEmpty(t, sagaAgg.TakeProfitOrderID)
	assert.NotEmpty(t, sagaAgg.StopLossOrderID)

	// Now fill the take-profit: place a buy at $155 to match the TP sell exit.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)
	env.flush()

	// Verify saga completed.
	sagaAgg, err = env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.Completed, sagaAgg.Status)

	// Verify the full saga event stream.
	raw, err := env.store.Load(env.ctx, bracket.AggregateID(sagaID))
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

	// Start the bracket.
	sagaID := "fail-test-1"
	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
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
	sagaAgg, err := env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.Failed, sagaAgg.Status)
}

func TestReactor_PartialFill_WaitsForFullFill(t *testing.T) {
	env := setupReactorTest(t)

	// Place partial sell liquidity: 60 shares at $150.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 60)
	env.pub.events = nil

	// Entry buy order for 100 shares. Only 60 will fill.
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	// Start the bracket.
	sagaID := "partial-test-1"
	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Flush: reactor sees 60-share trade, but entry needs 100.
	env.flush()

	// Saga should still be PendingEntry.
	sagaAgg, err := env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.PendingEntry, sagaAgg.Status)

	// Place more sell liquidity to fill the remaining 40.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 40)

	// The resting buy for 40 remaining will match. Flush.
	env.flush()

	// Now saga should be PendingExit.
	sagaAgg, err = env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.PendingExit, sagaAgg.Status)
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

	sagaHandler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := bracket.NewReactor(sagaHandler, obHandler, slog.Default())

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

	// Start the bracket.
	sagaID := "recovery-entry-fill-1"
	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Flush events while NOT ready — reactor accumulates filledQty but won't place exits.
	env.flush()

	sagaAgg, err := env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.PendingEntry, sagaAgg.Status)

	// SetReady triggers recovery — should place exit orders and record EntryFilled.
	env.reactor.SetReady(env.ctx)
	env.flush()

	sagaAgg, err = env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.PendingExit, sagaAgg.Status)
	assert.NotEmpty(t, sagaAgg.TakeProfitOrderID)
	assert.NotEmpty(t, sagaAgg.StopLossOrderID)
}

func TestReactor_Recovery_EntryCancelledDuringReplay(t *testing.T) {
	env := setupReactorTestNotReady(t)

	// Place entry order (no matching liquidity).
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)
	env.pub.events = nil

	// Start the bracket.
	sagaID := "recovery-cancel-1"
	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
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
	sagaAgg, err := env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.PendingEntry, sagaAgg.Status)

	// SetReady triggers recovery — should record SagaFailed.
	env.reactor.SetReady(env.ctx)
	env.flush()

	sagaAgg, err = env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.Failed, sagaAgg.Status)
}

func TestReactor_Recovery_ExitFilledDuringReplay(t *testing.T) {
	// Phase 1: Use a ready reactor to drive the saga to PendingExit.
	env := setupReactorTest(t)

	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	sagaID := "recovery-exit-fill-1"
	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)
	env.flush()

	sagaAgg, err := env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	require.Equal(t, bracket.PendingExit, sagaAgg.Status)

	// Fill the take-profit exit order (don't flush to the ready reactor).
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)
	env.pub.events = nil

	// Phase 2: Create a fresh not-ready reactor and replay all events from the store.
	reactor2 := bracket.NewReactor(env.sagaHandler, env.obHandler, slog.Default())

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

	sagaAgg, err = env.sagaHandler.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.Completed, sagaAgg.Status)
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

	sagaHandler := es.NewHandler(fStore, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := bracket.NewReactor(sagaHandler, obHandler, slog.Default())
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
	startCmd := bracket.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 100,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := sagaHandler.Handle(ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)
	env.flush()

	sagaAgg, err := sagaHandler.Load(ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	require.Equal(t, bracket.PendingExit, sagaAgg.Status)

	// Make ExitFilled fail once — the reactor will emit SagaActionFailed,
	// then retry successfully in the same flush cycle.
	fStore.mu.Lock()
	fStore.failTypes["ExitFilled"] = 1
	fStore.mu.Unlock()

	// Fill the TP exit.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)
	env.flush()

	// Verify saga completed (retry succeeded after transient failure).
	sagaAgg, err = sagaHandler.Load(ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.Completed, sagaAgg.Status)

	// Verify that SagaActionFailed appears in the event stream (retry happened).
	raw, err := realStore.Load(ctx, bracket.AggregateID(sagaID))
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

	sagaHandler := es.NewHandler(fStore, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := bracket.NewReactor(sagaHandler, obHandler, slog.Default())
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
	startCmd := bracket.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 100,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := sagaHandler.Handle(ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
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
	for i := 0; i < bracket.MaxActionAttempts+1; i++ {
		env.flush()
	}

	sagaAgg, err := sagaHandler.Load(ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.Failed, sagaAgg.Status)
}

func TestReactor_ExitCancelTransientFailure_Retries(t *testing.T) {
	registry := newTestRegistry()
	realStore := memstore.New()
	obFail := &failingStore{EventStore: realStore, failTypes: map[string]int{}}
	ctx := context.Background()
	pub := &collectingPublisher{}

	obHandler := es.NewHandler(obFail, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default()).WithPublisher(pub)

	sagaHandler := es.NewHandler(realStore, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := bracket.NewReactor(sagaHandler, obHandler, slog.Default())
	reactor.SetReady(ctx)

	env := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		reactor: reactor, store: realStore, registry: registry, pub: pub,
	}

	// Drive saga to PendingExit.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	sagaID := "cancel-transient-1"
	startCmd := bracket.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 100,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := sagaHandler.Handle(ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)
	env.flush()

	require.Equal(t, bracket.PendingExit, mustLoadSaga(t, sagaHandler, ctx, sagaID).Status)

	// Make OrderCancelled fail once — cancel of sibling will fail transiently.
	obFail.mu.Lock()
	obFail.failTypes["OrderCancelled"] = 1
	obFail.mu.Unlock()

	// Fill the TP exit.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)
	env.flush()

	// Saga should complete after retry.
	assert.Equal(t, bracket.Completed, mustLoadSaga(t, sagaHandler, ctx, sagaID).Status)

	// Verify SagaActionFailed appears in event stream.
	raw, err := realStore.Load(ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	types := eventTypes(t, registry, raw)
	assert.Contains(t, types, "SagaActionFailed")
	assert.Contains(t, types, "SagaCompleted")
}

func TestReactor_ExitCancelOrderGone_Proceeds(t *testing.T) {
	env := setupReactorTest(t)

	// Drive saga to PendingExit.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)

	sagaID := "cancel-gone-1"
	startCmd := bracket.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 100,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)
	env.flush()

	sagaAgg := mustLoadSaga(t, env.sagaHandler, env.ctx, sagaID)
	require.Equal(t, bracket.PendingExit, sagaAgg.Status)

	// Fill the TP exit.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1550000, 100)

	// Also fill the SL by placing a sell that triggers the stop-market.
	// Place a buy at $145 that will match the SL stop-market when triggered.
	placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1450000, 100)
	// Place a sell at $145 to trigger the stop.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1450000, 1)

	env.flush()

	// Saga should complete even though the sibling cancel returned
	// ErrOrderNotFound/ErrNoRemainingQty.
	assert.Equal(t, bracket.Completed, mustLoadSaga(t, env.sagaHandler, env.ctx, sagaID).Status)
}

func TestReactor_HandleEvents_ReturnsError(t *testing.T) {
	registry := newTestRegistry()
	realStore := memstore.New()
	fStore := &failingStore{EventStore: realStore, failTypes: map[string]int{}}
	ctx := context.Background()
	pub := &collectingPublisher{}

	obHandler := es.NewHandler(realStore, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, slog.Default()).WithPublisher(pub)

	sagaHandler := es.NewHandler(fStore, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default()).WithPublisher(pub)

	reactor := bracket.NewReactor(sagaHandler, obHandler, slog.Default())
	reactor.SetReady(ctx)

	env := &reactorTestEnv{
		ctx: ctx, obHandler: obHandler, sagaHandler: sagaHandler,
		reactor: reactor, store: realStore, registry: registry, pub: pub,
	}

	// Place entry order (no matching liquidity).
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)
	env.pub.events = nil

	sagaID := "error-return-1"
	startCmd := bracket.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 100,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := sagaHandler.Handle(ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)
	env.flush()

	// Make ALL saga writes fail — entry cancel will fail, and then
	// emitActionFailed will also fail.
	fStore.mu.Lock()
	fStore.failTypes["SagaFailed"] = 999
	fStore.failTypes["SagaActionFailed"] = 999
	fStore.mu.Unlock()

	// Cancel the entry order.
	cancelCmd := orderbook.CancelOrder{Symbol: "AAPL", OrderID: entryOrderID}
	err = env.obHandler.Handle(ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	require.NoError(t, err)

	// Flush — should produce an error from HandleEvents.
	batch := pub.events
	pub.events = nil
	err = reactor.HandleEvents(ctx, batch)
	assert.Error(t, err, "HandleEvents should return error when emitActionFailed fails")
}

func TestReactor_DuplicateTradeIgnored(t *testing.T) {
	env := setupReactorTest(t)

	// Place resting sell liquidity.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 200)
	env.pub.events = nil

	// Entry buy for 200 shares — fills immediately.
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 200)

	sagaID := "dedup-test-1"
	startCmd := bracket.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 200,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Collect the events but DON'T flush yet.
	batch := env.pub.events
	env.pub.events = nil

	// Deliver the same batch twice — simulating redelivery.
	err = env.reactor.HandleEvents(env.ctx, batch)
	require.NoError(t, err)
	env.flush()

	err = env.reactor.HandleEvents(env.ctx, batch)
	require.NoError(t, err)
	env.flush()

	// Saga should be PendingExit (not double-triggered).
	sagaAgg := mustLoadSaga(t, env.sagaHandler, env.ctx, sagaID)
	assert.Equal(t, bracket.PendingExit, sagaAgg.Status)

	// Only one EntryFilled event should exist.
	raw, err := env.store.Load(env.ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	types := eventTypes(t, env.registry, raw)
	count := 0
	for _, tp := range types {
		if tp == "EntryFilled" {
			count++
		}
	}
	assert.Equal(t, 1, count, "should have exactly one EntryFilled event")
}

func TestReactor_Recovery_MissedFillDetected(t *testing.T) {
	env := setupReactorTestNotReady(t)

	// Place resting sell liquidity.
	placeLimitOrder(t, env, "AAPL", orderbook.Sell, 1500000, 100)
	env.pub.events = nil

	// Entry buy — fills immediately.
	entryOrderID := placeLimitOrder(t, env, "AAPL", orderbook.Buy, 1500000, 100)
	// Discard the trade events so the reactor never sees them.
	env.pub.events = nil

	sagaID := "missed-fill-1"
	startCmd := bracket.StartSaga{
		SagaID: sagaID, Symbol: "AAPL",
		EntrySide: orderbookv1.Side_SIDE_BUY, EntryPrice: 1500000, EntryQty: 100,
		TakeProfitPrice: 1550000, StopLossPrice: 1450000, EntryOrderID: entryOrderID,
	}
	err := env.sagaHandler.Handle(env.ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Flush only the SagaStarted — reactor never saw TradeExecuted.
	env.flush()

	// Saga should be PendingEntry (reactor doesn't know about the fill).
	sagaAgg := mustLoadSaga(t, env.sagaHandler, env.ctx, sagaID)
	require.Equal(t, bracket.PendingEntry, sagaAgg.Status)

	// SetReady triggers recovery — should detect fill from orderbook.
	env.reactor.SetReady(env.ctx)
	env.flush()

	// Saga should now be PendingExit with exit orders placed.
	sagaAgg = mustLoadSaga(t, env.sagaHandler, env.ctx, sagaID)
	assert.Equal(t, bracket.PendingExit, sagaAgg.Status)
	assert.NotEmpty(t, sagaAgg.TakeProfitOrderID)
	assert.NotEmpty(t, sagaAgg.StopLossOrderID)
}

func mustLoadSaga(t *testing.T, handler *es.Handler[*bracket.BracketSaga], ctx context.Context, sagaID string) *bracket.BracketSaga {
	t.Helper()
	s, err := handler.Load(ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	return s
}

func eventTypes(t *testing.T, registry *es.Registry, raw []es.RawEvent) []string {
	t.Helper()
	types := make([]string, len(raw))
	for i, r := range raw {
		evt, err := registry.Deserialize(r)
		require.NoError(t, err)
		types[i] = evt.Type
	}
	return types
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
