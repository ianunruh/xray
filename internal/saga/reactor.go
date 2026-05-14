package saga

import (
	"context"
	"log/slog"
	"sync"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

type reactorState struct {
	sagaID          string
	symbol          string
	entryOrderID    string
	entryQty        int64
	filledQty       int64
	entrySide       orderbookv1.Side
	takeProfitPrice int64
	stopLossPrice   int64
	tpOrderID       string
	slOrderID       string
	status          Status
}

type Reactor struct {
	sagaHandler      *es.Handler[*BracketSaga]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger

	mu           sync.Mutex
	orderToSaga  map[string]string        // orderID -> sagaID
	sagas        map[string]*reactorState // sagaID -> state
	ready        bool
}

func NewReactor(sagaHandler *es.Handler[*BracketSaga], orderbookHandler *es.Handler[*orderbook.OrderBook], log *slog.Logger) *Reactor {
	return &Reactor{
		sagaHandler:      sagaHandler,
		orderbookHandler: orderbookHandler,
		log:              log,
		orderToSaga:      make(map[string]string),
		sagas:            make(map[string]*reactorState),
	}
}

func (r *Reactor) SetReady() {
	r.mu.Lock()
	r.ready = true
	r.mu.Unlock()
}

func (r *Reactor) HandleEvents(ctx context.Context, events []es.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// First pass: process saga events to build/update the correlation index.
	// This ensures the index is ready before we react to orderbook events,
	// even if they arrive in the same batch (e.g., TradeExecuted before SagaStarted).
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.SagaStarted:
			r.onSagaStarted(data)
		case *orderbookv1.EntryFilled:
			r.onEntryFilled(data)
		case *orderbookv1.ExitFilled:
			r.onExitFilled(data)
		case *orderbookv1.SagaCompleted:
			r.onSagaCompleted(data)
		case *orderbookv1.SagaFailed:
			r.onSagaFailed(data)
		}
	}

	// Second pass: react to orderbook events using the updated index.
	for _, evt := range events {
		switch data := evt.Data.(type) {
		case *orderbookv1.TradeExecuted:
			r.onTradeExecuted(ctx, data)
		case *orderbookv1.OrderCancelled:
			r.onOrderCancelled(ctx, data)
		}
	}

	return nil
}

func (r *Reactor) onSagaStarted(data *orderbookv1.SagaStarted) {
	state := &reactorState{
		sagaID:          data.SagaId,
		symbol:          data.Symbol,
		entryOrderID:    data.EntryOrderId,
		entryQty:        data.EntryQuantity,
		entrySide:       data.EntrySide,
		takeProfitPrice: data.TakeProfitPrice,
		stopLossPrice:   data.StopLossPrice,
		status:          PendingEntry,
	}
	r.sagas[data.SagaId] = state
	r.orderToSaga[data.EntryOrderId] = data.SagaId
}

func (r *Reactor) onEntryFilled(data *orderbookv1.EntryFilled) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.tpOrderID = data.TakeProfitOrderId
	state.slOrderID = data.StopLossOrderId
	state.status = PendingExit
	r.orderToSaga[data.TakeProfitOrderId] = data.SagaId
	r.orderToSaga[data.StopLossOrderId] = data.SagaId
}

func (r *Reactor) onExitFilled(_ *orderbookv1.ExitFilled) {
}

func (r *Reactor) onSagaCompleted(data *orderbookv1.SagaCompleted) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.status = Completed
	r.cleanupSaga(state)
}

func (r *Reactor) onSagaFailed(data *orderbookv1.SagaFailed) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.status = Failed
	r.cleanupSaga(state)
}

func (r *Reactor) cleanupSaga(state *reactorState) {
	delete(r.orderToSaga, state.entryOrderID)
	if state.tpOrderID != "" {
		delete(r.orderToSaga, state.tpOrderID)
	}
	if state.slOrderID != "" {
		delete(r.orderToSaga, state.slOrderID)
	}
	delete(r.sagas, state.sagaID)
}

func (r *Reactor) onTradeExecuted(ctx context.Context, data *orderbookv1.TradeExecuted) {
	orderID := r.matchOrderID(data)
	if orderID == "" {
		return
	}

	sagaID := r.orderToSaga[orderID]
	state, ok := r.sagas[sagaID]
	if !ok {
		return
	}

	switch {
	case orderID == state.entryOrderID && state.status == PendingEntry:
		r.handleEntryTrade(ctx, state, data.Quantity)
	case (orderID == state.tpOrderID || orderID == state.slOrderID) && state.status == PendingExit:
		r.handleExitTrade(ctx, state, orderID)
	}
}

func (r *Reactor) matchOrderID(data *orderbookv1.TradeExecuted) string {
	if _, ok := r.orderToSaga[data.BuyOrderId]; ok {
		return data.BuyOrderId
	}
	if _, ok := r.orderToSaga[data.SellOrderId]; ok {
		return data.SellOrderId
	}
	return ""
}

func (r *Reactor) handleEntryTrade(ctx context.Context, state *reactorState, qty int64) {
	state.filledQty += qty
	if state.filledQty < state.entryQty {
		return
	}

	if !r.ready {
		return
	}

	// Entry fully filled — place exit orders.
	exitSide := orderbookv1.Side_SIDE_SELL
	if state.entrySide == orderbookv1.Side_SIDE_SELL {
		exitSide = orderbookv1.Side_SIDE_BUY
	}

	tpOrderID, err := r.placeExitOrder(ctx, state.symbol, exitSide, state.takeProfitPrice, state.entryQty)
	if err != nil {
		r.log.Error("failed to place take-profit order", "saga_id", state.sagaID, "error", err)
		return
	}

	slOrderID, err := r.placeExitOrder(ctx, state.symbol, exitSide, state.stopLossPrice, state.entryQty)
	if err != nil {
		r.log.Error("failed to place stop-loss order", "saga_id", state.sagaID, "error", err)
		return
	}

	// Record entry filled on the saga aggregate.
	cmd := RecordEntryFilled{
		SagaID:            state.sagaID,
		TakeProfitOrderID: tpOrderID,
		StopLossOrderID:   slOrderID,
	}

	// Release lock while issuing commands to avoid deadlock with FanOutPublisher.
	r.mu.Unlock()
	err = r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordEntryFilled(saga, cmd)
	})
	r.mu.Lock()

	if err != nil {
		r.log.Error("failed to record entry filled", "saga_id", state.sagaID, "error", err)
		return
	}

	r.log.Info("bracket saga entry filled, exit orders placed",
		"saga_id", state.sagaID,
		"tp_order_id", tpOrderID,
		"sl_order_id", slOrderID)
}

func (r *Reactor) handleExitTrade(ctx context.Context, state *reactorState, filledOrderID string) {
	if !r.ready {
		return
	}

	cancelOrderID := state.slOrderID
	if filledOrderID == state.slOrderID {
		cancelOrderID = state.tpOrderID
	}

	// Cancel the other exit order.
	cancelCmd := orderbook.CancelOrder{
		Symbol:  state.symbol,
		OrderID: cancelOrderID,
	}

	r.mu.Unlock()
	err := r.orderbookHandler.Handle(ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	r.mu.Lock()

	if err != nil {
		r.log.Error("failed to cancel exit order", "saga_id", state.sagaID, "order_id", cancelOrderID, "error", err)
	}

	// Record exit filled + saga completed.
	cmd := RecordExitFilled{
		SagaID:           state.sagaID,
		FilledOrderID:    filledOrderID,
		CancelledOrderID: cancelOrderID,
	}

	r.mu.Unlock()
	err = r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordExitFilled(saga, cmd)
	})
	r.mu.Lock()

	if err != nil {
		r.log.Error("failed to record exit filled", "saga_id", state.sagaID, "error", err)
		return
	}

	r.log.Info("bracket saga completed",
		"saga_id", state.sagaID,
		"filled_order_id", filledOrderID,
		"cancelled_order_id", cancelOrderID)
}

func (r *Reactor) onOrderCancelled(ctx context.Context, data *orderbookv1.OrderCancelled) {
	sagaID, ok := r.orderToSaga[data.OrderId]
	if !ok {
		return
	}

	state, ok := r.sagas[sagaID]
	if !ok {
		return
	}

	// Only react to entry order cancellation while pending.
	if data.OrderId != state.entryOrderID || state.status != PendingEntry {
		return
	}

	if !r.ready {
		return
	}

	cmd := RecordSagaFailed{
		SagaID: state.sagaID,
		Reason: "entry order cancelled",
	}

	r.mu.Unlock()
	err := r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordSagaFailed(saga, cmd)
	})
	r.mu.Lock()

	if err != nil {
		r.log.Error("failed to record saga failed", "saga_id", state.sagaID, "error", err)
		return
	}

	r.log.Info("bracket saga failed — entry cancelled", "saga_id", state.sagaID)
}

func (r *Reactor) placeExitOrder(ctx context.Context, symbol string, side orderbookv1.Side, price, qty int64) (string, error) {
	cmd := orderbook.PlaceOrder{
		Symbol:      symbol,
		Side:        orderbook.SideFromProto(side),
		Price:       price,
		Quantity:    qty,
		OrderType:   orderbook.Limit,
		TimeInForce: orderbook.GTC,
	}

	var orderID string
	r.mu.Unlock()
	err := r.orderbookHandler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
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
	r.mu.Lock()

	return orderID, err
}
