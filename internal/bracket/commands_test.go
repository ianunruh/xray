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
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTestRegistry() *es.Registry {
	r := es.NewRegistry()
	orderbook.RegisterEvents(r)
	bracket.RegisterEvents(r)
	return r
}

func TestStartSaga_ThroughHandler(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "test-saga-1"

	cmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
	}

	err := handler.Handle(ctx, cmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, cmd)
	})
	require.NoError(t, err)

	raw, err := store.Load(ctx, bracket.AggregateID(sagaID))
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
}

func TestRecordEntryFilled_InvalidState(t *testing.T) {
	s := bracket.NewBracketSaga(bracket.AggregateID("test"))

	_, err := bracket.ExecuteRecordEntryFilled(s, bracket.RecordEntryFilled{
		SagaID:            "test",
		TakeProfitOrderID: "tp-1",
		StopLossOrderID:   "sl-1",
	})
	assert.ErrorIs(t, err, bracket.ErrInvalidState)
}

func TestRecordExitFilled_InvalidState(t *testing.T) {
	s := bracket.NewBracketSaga(bracket.AggregateID("test"))

	_, err := bracket.ExecuteRecordExitFilled(s, bracket.RecordExitFilled{
		SagaID:           "test",
		FilledOrderID:    "tp-1",
		CancelledOrderID: "sl-1",
	})
	assert.ErrorIs(t, err, bracket.ErrInvalidState)
}

func TestRecordSagaFailed_InvalidState(t *testing.T) {
	s := bracket.NewBracketSaga(bracket.AggregateID("test"))

	_, err := bracket.ExecuteRecordSagaFailed(s, bracket.RecordSagaFailed{
		SagaID: "test",
		Reason: "cancelled",
	})
	assert.ErrorIs(t, err, bracket.ErrInvalidState)
}

func TestRecordSagaFailed_FromPendingExit(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "fail-exit-test"

	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
	}
	err := handler.Handle(ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	filledCmd := bracket.RecordEntryFilled{
		SagaID:            sagaID,
		TakeProfitOrderID: "tp-1",
		StopLossOrderID:   "sl-1",
	}
	err = handler.Handle(ctx, filledCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteRecordEntryFilled(s, filledCmd)
	})
	require.NoError(t, err)

	failCmd := bracket.RecordSagaFailed{SagaID: sagaID, Reason: "exit order rejected"}
	err = handler.Handle(ctx, failCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteRecordSagaFailed(s, failCmd)
	})
	require.NoError(t, err)

	sagaAgg, err := handler.Load(ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, bracket.Failed, sagaAgg.Status)
}

func TestRecordActionFailed_EmitsSagaActionFailed(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "action-failed-test"

	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
	}
	err := handler.Handle(ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	cmd := bracket.RecordActionFailed{SagaID: sagaID, Action: "place_exit_orders"}
	err = handler.Handle(ctx, cmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteRecordActionFailed(s, cmd)
	})
	require.NoError(t, err)

	raw, err := store.Load(ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)
	require.Len(t, raw, 2)

	evt, err := registry.Deserialize(raw[1])
	require.NoError(t, err)
	assert.Equal(t, "SagaActionFailed", evt.Type)

	data := evt.Data.(*orderbookv1.SagaActionFailed)
	assert.Equal(t, sagaID, data.SagaId)
	assert.Equal(t, "place_exit_orders", data.Action)
	assert.Equal(t, int32(1), data.Attempts)
}

func TestRecordActionFailed_MaxRetries_EmitsSagaFailed(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "max-retry-test"

	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
	}
	err := handler.Handle(ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	for i := 0; i < bracket.MaxActionAttempts; i++ {
		cmd := bracket.RecordActionFailed{SagaID: sagaID, Action: "place_exit_orders"}
		err = handler.Handle(ctx, cmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
			return bracket.ExecuteRecordActionFailed(s, cmd)
		})
		require.NoError(t, err)
	}

	raw, err := store.Load(ctx, bracket.AggregateID(sagaID))
	require.NoError(t, err)

	lastEvt, err := registry.Deserialize(raw[len(raw)-1])
	require.NoError(t, err)
	assert.Equal(t, "SagaFailed", lastEvt.Type)

	data := lastEvt.Data.(*orderbookv1.SagaFailed)
	assert.Contains(t, data.Reason, "max retries exceeded")
}

func TestRecordActionFailed_InvalidState(t *testing.T) {
	s := bracket.NewBracketSaga(bracket.AggregateID("test"))

	_, err := bracket.ExecuteRecordActionFailed(s, bracket.RecordActionFailed{
		SagaID: "test",
		Action: "place_exit_orders",
	})
	assert.ErrorIs(t, err, bracket.ErrInvalidState)
}

func TestFullSagaLifecycle(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()

	handler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "lifecycle-saga"

	// Start bracket.
	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		Symbol:          "AAPL",
		EntrySide:       orderbookv1.Side_SIDE_BUY,
		EntryPrice:      1500000,
		EntryQty:        100,
		TakeProfitPrice: 1550000,
		StopLossPrice:   1450000,
	}
	err := handler.Handle(ctx, startCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Record entry filled.
	filledCmd := bracket.RecordEntryFilled{
		SagaID:            sagaID,
		TakeProfitOrderID: "tp-1",
		StopLossOrderID:   "sl-1",
	}
	err = handler.Handle(ctx, filledCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteRecordEntryFilled(s, filledCmd)
	})
	require.NoError(t, err)

	// Record exit filled + saga completed.
	exitCmd := bracket.RecordExitFilled{
		SagaID:           sagaID,
		FilledOrderID:    "tp-1",
		CancelledOrderID: "sl-1",
	}
	err = handler.Handle(ctx, exitCmd, func(s *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteRecordExitFilled(s, exitCmd)
	})
	require.NoError(t, err)

	// Verify full event stream.
	raw, err := store.Load(ctx, bracket.AggregateID(sagaID))
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
