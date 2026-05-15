package ordersaga_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTestRegistry() *es.Registry {
	r := es.NewRegistry()
	ordersaga.RegisterEvents(r)
	return r
}

func TestStartOrderSaga(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *ordersaga.OrderSaga {
		return ordersaga.NewOrderSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "test-saga-1"

	cmd := ordersaga.StartOrderSaga{
		SagaID:      sagaID,
		AccountID:   "acct-1",
		Symbol:      "AAPL",
		Side:        orderbookv1.Side_SIDE_BUY,
		Price:       1500000,
		Quantity:    100,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}

	err := handler.Handle(ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	})
	require.NoError(t, err)

	s, err := handler.Load(ctx, ordersaga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, ordersaga.Started, s.Status)
	assert.Equal(t, "acct-1", s.AccountID)
	assert.Equal(t, "AAPL", s.Symbol)
	assert.Equal(t, int64(100), s.Quantity)
}

func TestFullLifecycle(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *ordersaga.OrderSaga {
		return ordersaga.NewOrderSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "lifecycle-1"

	// Start.
	startCmd := ordersaga.StartOrderSaga{
		SagaID: sagaID, AccountID: "acct-1", Symbol: "AAPL",
		Side: orderbookv1.Side_SIDE_BUY, Price: 1500000, Quantity: 100,
		OrderType: orderbookv1.OrderType_ORDER_TYPE_LIMIT, TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	err := handler.Handle(ctx, startCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, startCmd)
	})
	require.NoError(t, err)

	// Cash held.
	heldCmd := ordersaga.RecordCashHeld{SagaID: sagaID, AmountHeld: 15000000}
	err = handler.Handle(ctx, heldCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteRecordCashHeld(s, heldCmd)
	})
	require.NoError(t, err)

	// Order placed.
	placedCmd := ordersaga.RecordOrderPlaced{SagaID: sagaID, OrderID: "order-1"}
	err = handler.Handle(ctx, placedCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteRecordOrderPlaced(s, placedCmd)
	})
	require.NoError(t, err)

	// Fill recorded.
	fillCmd := ordersaga.RecordFill{
		SagaID: sagaID, TradeID: "trade-1",
		FillQuantity: 100, FillPrice: 1500000, CashSettled: 15000000,
	}
	err = handler.Handle(ctx, fillCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteRecordFill(s, fillCmd)
	})
	require.NoError(t, err)

	// Completed.
	completeCmd := ordersaga.RecordCompleted{SagaID: sagaID}
	err = handler.Handle(ctx, completeCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteRecordCompleted(s, completeCmd)
	})
	require.NoError(t, err)

	s, err := handler.Load(ctx, ordersaga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, ordersaga.Completed, s.Status)
	assert.Equal(t, int64(100), s.FilledQty)
	assert.Equal(t, int64(15000000), s.CashSettled)

	raw, err := store.Load(ctx, ordersaga.AggregateID(sagaID))
	require.NoError(t, err)

	types := make([]string, len(raw))
	for i, r := range raw {
		evt, err := registry.Deserialize(r)
		require.NoError(t, err)
		types[i] = evt.Type
	}
	assert.Equal(t, []string{
		"OrderSagaStarted", "OrderSagaCashHeld", "OrderSagaOrderPlaced",
		"OrderSagaFillRecorded", "OrderSagaCompleted",
	}, types)
}

func TestRecordCashHeld_InvalidState(t *testing.T) {
	s := ordersaga.NewOrderSaga(ordersaga.AggregateID("test"))
	_, err := ordersaga.ExecuteRecordCashHeld(s, ordersaga.RecordCashHeld{SagaID: "test", AmountHeld: 100})
	assert.ErrorIs(t, err, ordersaga.ErrInvalidState)
}

func TestRecordOrderPlaced_InvalidState(t *testing.T) {
	s := ordersaga.NewOrderSaga(ordersaga.AggregateID("test"))
	_, err := ordersaga.ExecuteRecordOrderPlaced(s, ordersaga.RecordOrderPlaced{SagaID: "test", OrderID: "o-1"})
	assert.ErrorIs(t, err, ordersaga.ErrInvalidState)
}

func TestRecordFill_InvalidState(t *testing.T) {
	s := ordersaga.NewOrderSaga(ordersaga.AggregateID("test"))
	_, err := ordersaga.ExecuteRecordFill(s, ordersaga.RecordFill{SagaID: "test", TradeID: "t-1", FillQuantity: 10, FillPrice: 100, CashSettled: 1000})
	assert.ErrorIs(t, err, ordersaga.ErrInvalidState)
}

func TestRecordActionFailed_MaxRetries(t *testing.T) {
	registry := newTestRegistry()
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *ordersaga.OrderSaga {
		return ordersaga.NewOrderSaga(id)
	}, slog.Default())

	ctx := context.Background()
	sagaID := "max-retry-test"

	startCmd := ordersaga.StartOrderSaga{
		SagaID: sagaID, AccountID: "acct-1", Symbol: "AAPL",
		Side: orderbookv1.Side_SIDE_BUY, Price: 1500000, Quantity: 100,
		OrderType: orderbookv1.OrderType_ORDER_TYPE_LIMIT, TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	err := handler.Handle(ctx, startCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, startCmd)
	})
	require.NoError(t, err)

	for i := 0; i < ordersaga.MaxActionAttempts; i++ {
		cmd := ordersaga.RecordActionFailed{SagaID: sagaID, Action: "hold_cash"}
		err = handler.Handle(ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
			return ordersaga.ExecuteRecordActionFailed(s, cmd)
		})
		require.NoError(t, err)
	}

	s, err := handler.Load(ctx, ordersaga.AggregateID(sagaID))
	require.NoError(t, err)
	assert.Equal(t, ordersaga.Failed, s.Status)
}
