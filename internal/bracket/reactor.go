package bracket

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/pkg/es"
)

type actionKind int

const (
	actionSpawnEntrySaga actionKind = iota
	actionPlaceExitOrders
	actionRecordExit
	actionFailEntryFailed
)

type action struct {
	sagaID string
	kind   actionKind
}

type reactorState struct {
	sagaID          string
	accountID       string
	symbol          string
	entrySide       orderbookv1.Side
	entryPrice      int64
	entryQty        int64
	takeProfitPrice int64
	stopLossPrice   int64
	tpOrderID       string
	slOrderID       string
	status          Status

	entrySpawned      bool
	entryDone         bool
	entryFailReason   string
	exitFilledOrderID string
	actionPending     bool
	pendingCancels    []string
}

type Reactor struct {
	sagaHandler      *es.Handler[*BracketSaga]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger

	mu    sync.Mutex
	sagas map[string]*reactorState
}

func NewReactor(
	sagaHandler *es.Handler[*BracketSaga],
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	log *slog.Logger,
) *Reactor {
	return &Reactor{
		sagaHandler:      sagaHandler,
		orderSagaHandler: orderSagaHandler,
		orderbookHandler: orderbookHandler,
		log:              log,
		sagas:            make(map[string]*reactorState),
	}
}

func (r *Reactor) HandleEvents(ctx context.Context, events []es.Event) error {
	r.mu.Lock()

	// Pass 1: bracket saga lifecycle events first so state exists before
	// we look up brackets in pass 2.
	for i := range events {
		r.applyBracketEvent(events[i])
	}

	// Pass 2: cross-stream observations (entry ordersaga completion;
	// TP/SL trades).
	for i := range events {
		r.applyExternalEvent(events[i])
	}

	actions := r.collectActions()
	r.mu.Unlock()

	return r.executeActions(ctx, actions)
}

func (r *Reactor) applyBracketEvent(evt es.Event) {
	switch data := evt.Data.(type) {
	case *orderbookv1.SagaStarted:
		r.onSagaStarted(data)
	case *orderbookv1.EntryFilled:
		r.onEntryFilled(data)
	case *orderbookv1.SagaCompleted:
		r.onSagaCompleted(data)
	case *orderbookv1.SagaFailed:
		r.onSagaFailed(data)
	case *orderbookv1.SagaActionFailed:
		r.onSagaActionFailed(data)
	}
}

func (r *Reactor) applyExternalEvent(evt es.Event) {
	switch data := evt.Data.(type) {
	case *portfoliov1.OrderSagaCompleted:
		r.onEntryOrderSagaCompleted(data)
	case *portfoliov1.OrderSagaFailed:
		r.onEntryOrderSagaFailed(data)
	case *orderbookv1.TradeExecuted:
		r.onExitTrade(data)
	}
}

func (r *Reactor) onSagaStarted(data *orderbookv1.SagaStarted) {
	r.sagas[data.SagaId] = &reactorState{
		sagaID:          data.SagaId,
		accountID:       data.AccountId,
		symbol:          data.Symbol,
		entrySide:       data.EntrySide,
		entryPrice:      data.EntryPrice,
		entryQty:        data.EntryQuantity,
		takeProfitPrice: data.TakeProfitPrice,
		stopLossPrice:   data.StopLossPrice,
		status:          PendingEntry,
	}
}

func (r *Reactor) onEntryFilled(data *orderbookv1.EntryFilled) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.tpOrderID = data.TakeProfitOrderId
	state.slOrderID = data.StopLossOrderId
	state.status = PendingExit
	state.actionPending = false
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

func (r *Reactor) onSagaActionFailed(data *orderbookv1.SagaActionFailed) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.actionPending = false
}

func (r *Reactor) onEntryOrderSagaCompleted(data *portfoliov1.OrderSagaCompleted) {
	bracketID, ok := bracketIDFromEntryOrderSagaID(data.SagaId)
	if !ok {
		return
	}
	state, ok := r.sagas[bracketID]
	if !ok || state.status != PendingEntry || state.entryDone {
		return
	}
	state.entryDone = true
	state.actionPending = false
}

func (r *Reactor) onEntryOrderSagaFailed(data *portfoliov1.OrderSagaFailed) {
	bracketID, ok := bracketIDFromEntryOrderSagaID(data.SagaId)
	if !ok {
		return
	}
	state, ok := r.sagas[bracketID]
	if !ok || state.status != PendingEntry {
		return
	}
	state.entryFailReason = data.Reason
	state.actionPending = false
}

func (r *Reactor) onExitTrade(data *orderbookv1.TradeExecuted) {
	for _, orderID := range []string{data.BuyOrderId, data.SellOrderId} {
		sagaID, ok := sagaIDFromExitOrderID(orderID)
		if !ok {
			continue
		}
		state, ok := r.sagas[sagaID]
		if !ok || state.status != PendingExit {
			continue
		}
		if (orderID == state.tpOrderID || orderID == state.slOrderID) && state.exitFilledOrderID == "" {
			state.exitFilledOrderID = orderID
		}
	}
}

func (r *Reactor) cleanupSaga(state *reactorState) {
	delete(r.sagas, state.sagaID)
}

func (r *Reactor) collectActions() []action {
	var actions []action
	for _, state := range r.sagas {
		if state.actionPending {
			continue
		}
		a, ok := r.actionForState(state)
		if !ok {
			continue
		}
		state.actionPending = true
		actions = append(actions, a)
	}
	return actions
}

func (r *Reactor) actionForState(state *reactorState) (action, bool) {
	switch {
	case state.status == PendingEntry && !state.entrySpawned:
		return action{sagaID: state.sagaID, kind: actionSpawnEntrySaga}, true
	case state.status == PendingEntry && state.entryFailReason != "":
		return action{sagaID: state.sagaID, kind: actionFailEntryFailed}, true
	case state.status == PendingEntry && state.entryDone:
		return action{sagaID: state.sagaID, kind: actionPlaceExitOrders}, true
	case state.status == PendingExit && state.exitFilledOrderID != "":
		return action{sagaID: state.sagaID, kind: actionRecordExit}, true
	}
	return action{}, false
}

func (r *Reactor) executeActions(ctx context.Context, actions []action) error {
	var errs []error
	for _, a := range actions {
		var err error
		switch a.kind {
		case actionSpawnEntrySaga:
			err = r.executeSpawnEntrySaga(ctx, a.sagaID)
		case actionPlaceExitOrders:
			err = r.executePlaceExitOrders(ctx, a.sagaID)
		case actionRecordExit:
			err = r.executeRecordExit(ctx, a.sagaID)
		case actionFailEntryFailed:
			err = r.executeFailEntryFailed(ctx, a.sagaID)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Reactor) executeSpawnEntrySaga(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != PendingEntry || state.entrySpawned {
		r.mu.Unlock()
		return nil
	}
	accountID := state.accountID
	symbol := state.symbol
	entrySide := state.entrySide
	entryPrice := state.entryPrice
	entryQty := state.entryQty
	r.mu.Unlock()

	cmd := ordersaga.StartOrderSaga{
		SagaID:      EntryOrderSagaID(sagaID),
		AccountID:   accountID,
		Symbol:      symbol,
		Side:        entrySide,
		Price:       entryPrice,
		Quantity:    entryQty,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	if err := r.orderSagaHandler.Handle(ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	}); err != nil {
		r.log.Error("bracket: failed to spawn entry ordersaga", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "spawn_entry_saga", err.Error())
	}

	r.mu.Lock()
	if s, ok := r.sagas[sagaID]; ok {
		s.entrySpawned = true
		s.actionPending = false
	}
	r.mu.Unlock()

	r.log.Info("bracket: entry ordersaga spawned", "bracket_id", sagaID)
	return nil
}

func (r *Reactor) executePlaceExitOrders(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != PendingEntry || !state.entryDone {
		r.mu.Unlock()
		return nil
	}
	symbol := state.symbol
	entrySide := state.entrySide
	tpPrice := state.takeProfitPrice
	slPrice := state.stopLossPrice
	entryQty := state.entryQty
	pendingCancels := state.pendingCancels
	state.pendingCancels = nil
	r.mu.Unlock()

	for _, orderID := range pendingCancels {
		if err := r.cancelOrder(ctx, symbol, orderID); err != nil {
			if !errors.Is(err, orderbook.ErrOrderNotFound) && !errors.Is(err, orderbook.ErrNoRemainingQty) {
				r.mu.Lock()
				if s, ok := r.sagas[sagaID]; ok {
					s.pendingCancels = append(s.pendingCancels, orderID)
				}
				r.mu.Unlock()
			}
		}
	}

	exitSide := orderbookv1.Side_SIDE_SELL
	if entrySide == orderbookv1.Side_SIDE_SELL {
		exitSide = orderbookv1.Side_SIDE_BUY
	}

	tpOrderID := TakeProfitOrderID(sagaID)
	if err := r.placeExitOrder(ctx, symbol, exitSide, tpPrice, entryQty, orderbook.Limit, 0, tpOrderID); err != nil {
		r.log.Error("failed to place take-profit order", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_exit_orders", err.Error())
	}

	slOrderID := StopLossOrderID(sagaID)
	if err := r.placeExitOrder(ctx, symbol, exitSide, 0, entryQty, orderbook.StopMarket, slPrice, slOrderID); err != nil {
		r.log.Error("failed to place stop-loss order", "saga_id", sagaID, "error", err)
		r.trackCancelFailure(ctx, sagaID, symbol, tpOrderID)
		return r.emitActionFailed(ctx, sagaID, "place_exit_orders", err.Error())
	}

	cmd := RecordEntryFilled{
		SagaID:            sagaID,
		TakeProfitOrderID: tpOrderID,
		StopLossOrderID:   slOrderID,
	}

	err := r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordEntryFilled(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record entry filled", "saga_id", sagaID, "error", err)
		r.trackCancelFailure(ctx, sagaID, symbol, tpOrderID)
		r.trackCancelFailure(ctx, sagaID, symbol, slOrderID)
		return r.emitActionFailed(ctx, sagaID, "place_exit_orders", err.Error())
	}

	r.log.Info("bracket saga entry filled, exit orders placed",
		"saga_id", sagaID,
		"tp_order_id", tpOrderID,
		"sl_order_id", slOrderID)
	return nil
}

func (r *Reactor) executeRecordExit(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != PendingExit || state.exitFilledOrderID == "" {
		r.mu.Unlock()
		return nil
	}
	filledOrderID := state.exitFilledOrderID
	symbol := state.symbol
	cancelOrderID := state.slOrderID
	if filledOrderID == state.slOrderID {
		cancelOrderID = state.tpOrderID
	}
	r.mu.Unlock()

	cancelCmd := orderbook.CancelOrder{
		Symbol:  symbol,
		OrderID: cancelOrderID,
	}
	err := r.orderbookHandler.Handle(ctx, cancelCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cancelCmd)
	})
	if err != nil {
		if errors.Is(err, orderbook.ErrOrderNotFound) || errors.Is(err, orderbook.ErrNoRemainingQty) {
			r.log.Warn("sibling order already consumed", "saga_id", sagaID, "order_id", cancelOrderID, "error", err)
		} else {
			r.log.Error("failed to cancel sibling order", "saga_id", sagaID, "order_id", cancelOrderID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "record_exit_filled", err.Error())
		}
	}

	cmd := RecordExitFilled{
		SagaID:           sagaID,
		FilledOrderID:    filledOrderID,
		CancelledOrderID: cancelOrderID,
	}
	err = r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordExitFilled(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record exit filled", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "record_exit_filled", err.Error())
	}

	r.log.Info("bracket saga completed",
		"saga_id", sagaID,
		"filled_order_id", filledOrderID,
		"cancelled_order_id", cancelOrderID)
	return nil
}

func (r *Reactor) executeFailEntryFailed(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != PendingEntry {
		r.mu.Unlock()
		return nil
	}
	reason := state.entryFailReason
	if reason == "" {
		reason = "entry ordersaga failed"
	}
	r.mu.Unlock()

	cmd := RecordSagaFailed{
		SagaID: sagaID,
		Reason: reason,
	}
	err := r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordSagaFailed(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record saga failed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "record_saga_failed", err.Error())
	}

	r.log.Info("bracket saga failed — entry ordersaga did not complete",
		"saga_id", sagaID, "reason", reason)
	return nil
}

func (r *Reactor) emitActionFailed(ctx context.Context, sagaID, action, reason string) error {
	cmd := RecordActionFailed{
		SagaID: sagaID,
		Action: action,
		Reason: reason,
	}
	err := r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordActionFailed(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to emit action failed event", "saga_id", sagaID, "action", action, "error", err)
		return fmt.Errorf("saga %s: failed to emit action failed for %s: %w", sagaID, action, err)
	}
	return nil
}

func (r *Reactor) trackCancelFailure(ctx context.Context, sagaID, symbol, orderID string) {
	if err := r.cancelOrder(ctx, symbol, orderID); err != nil {
		if !errors.Is(err, orderbook.ErrOrderNotFound) && !errors.Is(err, orderbook.ErrNoRemainingQty) {
			r.mu.Lock()
			if s, ok := r.sagas[sagaID]; ok {
				s.pendingCancels = append(s.pendingCancels, orderID)
			}
			r.mu.Unlock()
		}
	}
}

func (r *Reactor) cancelOrder(ctx context.Context, symbol, orderID string) error {
	cmd := orderbook.CancelOrder{
		Symbol:  symbol,
		OrderID: orderID,
	}
	return r.orderbookHandler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cmd)
	})
}

func (r *Reactor) placeExitOrder(ctx context.Context, symbol string, side orderbookv1.Side, price, qty int64, orderType orderbook.OrderType, stopPrice int64, orderID string) error {
	cmd := orderbook.PlaceOrder{
		Symbol:      symbol,
		Side:        orderbook.SideFromProto(side),
		Price:       price,
		StopPrice:   stopPrice,
		Quantity:    qty,
		OrderType:   orderType,
		TimeInForce: orderbook.GTC,
		OrderID:     orderID,
	}
	return r.orderbookHandler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, cmd)
	})
}
