package saga_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/saga"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTestRegistry() *es.Registry {
	r := es.NewRegistry()
	r.Register("OrderPlaced", func() proto.Message { return new(orderbookv1.OrderPlaced) })
	r.Register("TradeExecuted", func() proto.Message { return new(orderbookv1.TradeExecuted) })
	r.Register("OrderCancelled", func() proto.Message { return new(orderbookv1.OrderCancelled) })
	r.Register("SagaStarted", func() proto.Message { return new(orderbookv1.SagaStarted) })
	r.Register("EntryFilled", func() proto.Message { return new(orderbookv1.EntryFilled) })
	r.Register("ExitFilled", func() proto.Message { return new(orderbookv1.ExitFilled) })
	r.Register("SagaCompleted", func() proto.Message { return new(orderbookv1.SagaCompleted) })
	r.Register("SagaFailed", func() proto.Message { return new(orderbookv1.SagaFailed) })
	return r
}

func TestStartSaga_ThroughHandler(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *saga.BracketSaga {
		return saga.NewBracketSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "test-saga-1"

	cmd := saga.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    "order-1",
	}

	err := handler.Handle(ctx, cmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteStartSaga(s, cmd)
	})
	require.NoError(t, err)

	raw, err := store.Load(ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	require.Len(t, raw, 1)

	evt, err := registry.Deserialize(raw[0])
	require.NoError(t, err)
	assert.Equal(t, "SagaStarted", evt.Type)

	data := evt.Data.(*orderbookv1.SagaStarted)
	assert.Equal(t, sagaID, data.SagaId)
	assert.Equal(t, "AAPL", data.Symbol)
	assert.Equal(t, orderbookv1.Side_SIDE_BUY, data.EntrySide)
	assert.Equal(t, int64(1500000), data.EntryPrice)
	assert.Equal(t, int64(100), data.EntryQuantity)
	assert.Equal(t, "order-1", data.EntryOrderId)
}

func TestRecordEntryFilled_InvalidState(t *testing.T) {
	s := saga.NewBracketSaga(saga.AggregateID("test"))

	_, err := saga.ExecuteRecordEntryFilled(s, saga.RecordEntryFilled{
		SagaID:            "test",
		TakeProfitOrderID: "tp-1",
		StopLossOrderID:   "sl-1",
	})
	assert.ErrorIs(t, err, saga.ErrInvalidState)
}

func TestRecordExitFilled_InvalidState(t *testing.T) {
	s := saga.NewBracketSaga(saga.AggregateID("test"))

	_, err := saga.ExecuteRecordExitFilled(s, saga.RecordExitFilled{
		SagaID:           "test",
		FilledOrderID:    "tp-1",
		CancelledOrderID: "sl-1",
	})
	assert.ErrorIs(t, err, saga.ErrInvalidState)
}

func TestRecordSagaFailed_InvalidState(t *testing.T) {
	s := saga.NewBracketSaga(saga.AggregateID("test"))

	_, err := saga.ExecuteRecordSagaFailed(s, saga.RecordSagaFailed{
		SagaID: "test",
		Reason: "cancelled",
	})
	assert.ErrorIs(t, err, saga.ErrInvalidState)
}

func TestFullSagaLifecycle(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *saga.BracketSaga {
		return saga.NewBracketSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "lifecycle-saga"

	// Start saga.
	startCmd := saga.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
		EntryOrderID:    "entry-1",
	}
	err := handler.Handle(ctx, startCmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Record entry filled.
	filledCmd := saga.RecordEntryFilled{
		SagaID:            sagaID,
		TakeProfitOrderID: "tp-1",
		StopLossOrderID:   "sl-1",
	}
	err = handler.Handle(ctx, filledCmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteRecordEntryFilled(s, filledCmd)
	})
	require.NoError(t, err)

	// Record exit filled + saga completed.
	exitCmd := saga.RecordExitFilled{
		SagaID:           sagaID,
		FilledOrderID:    "tp-1",
		CancelledOrderID: "sl-1",
	}
	err = handler.Handle(ctx, exitCmd, func(s *saga.BracketSaga) ([]es.Event, error) {
		return saga.ExecuteRecordExitFilled(s, exitCmd)
	})
	require.NoError(t, err)

	// Verify full event stream.
	raw, err := store.Load(ctx, saga.AggregateID(sagaID))
	require.NoError(t, err)
	require.Len(t, raw, 4) // SagaStarted, EntryFilled, ExitFilled, SagaCompleted

	types := make([]string, len(raw))
	for i, r := range raw {
		evt, err := registry.Deserialize(r)
		require.NoError(t, err)
		types[i] = evt.Type
	}
	assert.Equal(t, []string{"SagaStarted", "EntryFilled", "ExitFilled", "SagaCompleted"}, types)
}
