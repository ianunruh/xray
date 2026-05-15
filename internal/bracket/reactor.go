package bracket

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

type actionKind int

const (
	actionPlaceExitOrders actionKind = iota
	actionRecordExit
	actionFailEntryCancelled
)

type action struct {
	sagaID string
	kind   actionKind
}

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

	entryCancelled    bool
	exitFilledOrderID string
	actionPending     bool
	pendingCancels    []string
}

type Reactor struct {
	sagaHandler      *es.Handler[*BracketSaga]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger

	mu          sync.Mutex
	orderToSaga map[string]string
	sagas       map[string]*reactorState
	lastVersion map[string]int
	ready       bool
}

func NewReactor(sagaHandler *es.Handler[*BracketSaga], orderbookHandler *es.Handler[*orderbook.OrderBook], log *slog.Logger) *Reactor {
	return &Reactor{
		sagaHandler:      sagaHandler,
		orderbookHandler: orderbookHandler,
		log:              log,
		orderToSaga:      make(map[string]string),
		sagas:            make(map[string]*reactorState),
		lastVersion:      make(map[string]int),
	}
}

func (r *Reactor) SetReady(ctx context.Context) {
	r.mu.Lock()
	r.ready = true

	var needsCheck []string
	for _, state := range r.sagas {
		if state.status == PendingEntry && !state.entryCancelled && state.filledQty < state.entryQty {
			needsCheck = append(needsCheck, state.sagaID)
		}
	}
	r.mu.Unlock()

	for _, sagaID := range needsCheck {
		r.checkEntryFillFromOrderbook(ctx, sagaID)
	}

	r.mu.Lock()
	actions := r.collectActions()
	r.mu.Unlock()

	r.executeActions(ctx, actions)
}

func (r *Reactor) checkEntryFillFromOrderbook(ctx context.Context, sagaID string) {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok {
		r.mu.Unlock()
		return
	}
	symbol := state.symbol
	entryOrderID := state.entryOrderID
	entryQty := state.entryQty
	r.mu.Unlock()

	book, err := r.orderbookHandler.Load(ctx, orderbook.AggregateID(symbol))
	if err != nil {
		r.log.Error("recovery: failed to load orderbook", "saga_id", sagaID, "error", err)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	state, ok = r.sagas[sagaID]
	if !ok || state.status != PendingEntry {
		return
	}

	order, ok := book.Orders[entryOrderID]
	if !ok {
		r.log.Info("recovery: entry order not found in orderbook", "saga_id", sagaID)
		state.entryCancelled = true
		return
	}

	totalFilled := entryQty - order.RemainingQty
	if totalFilled > state.filledQty {
		r.log.Info("recovery: adjusted filled qty from orderbook", "saga_id", sagaID, "filled", totalFilled)
		state.filledQty = totalFilled
	}
}

func (r *Reactor) HandleEvents(ctx context.Context, events []es.Event) error {
	r.mu.Lock()

	// Pass 1: apply saga lifecycle events so order mappings exist before
	// processing orderbook events.
	for i := range events {
		r.applySagaEvent(events[i])
	}

	// Pass 2: apply orderbook events (state mutation only, no side effects).
	for i := range events {
		r.applyOrderbookEvent(events[i])
	}

	actions := r.collectActions()
	r.mu.Unlock()

	return r.executeActions(ctx, actions)
}

func (r *Reactor) applySagaEvent(evt es.Event) {
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
	case *orderbookv1.SagaActionFailed:
		r.onSagaActionFailed(data)
	}
}

func (r *Reactor) applyOrderbookEvent(evt es.Event) {
	switch data := evt.Data.(type) {
	case *orderbookv1.TradeExecuted:
		r.applyTrade(evt, data)
	case *orderbookv1.OrderCancelled:
		r.applyCancel(evt, data)
	}
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
	state.actionPending = false
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

func (r *Reactor) onSagaActionFailed(data *orderbookv1.SagaActionFailed) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.actionPending = false
}

func (r *Reactor) applyTrade(evt es.Event, data *orderbookv1.TradeExecuted) {
	if v, ok := r.lastVersion[evt.AggregateID]; ok && evt.Version <= v {
		return
	}
	if evt.Version > 0 {
		r.lastVersion[evt.AggregateID] = evt.Version
	}

	orderID := r.matchOrderID(data)
	if orderID == "" {
		return
	}

	sagaID := r.orderToSaga[orderID]
	state, ok := r.sagas[sagaID]
	if !ok {
		return
	}

	if orderID == state.entryOrderID && state.status == PendingEntry {
		state.filledQty += data.Quantity
	}
	if (orderID == state.tpOrderID || orderID == state.slOrderID) && state.status == PendingExit {
		if state.exitFilledOrderID == "" {
			state.exitFilledOrderID = orderID
		}
	}
}

func (r *Reactor) applyCancel(evt es.Event, data *orderbookv1.OrderCancelled) {
	if v, ok := r.lastVersion[evt.AggregateID]; ok && evt.Version <= v {
		return
	}
	if evt.Version > 0 {
		r.lastVersion[evt.AggregateID] = evt.Version
	}

	sagaID, ok := r.orderToSaga[data.OrderId]
	if !ok {
		return
	}

	state, ok := r.sagas[sagaID]
	if !ok {
		return
	}

	if data.OrderId == state.entryOrderID && state.status == PendingEntry {
		state.entryCancelled = true
	}
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

func (r *Reactor) matchOrderID(data *orderbookv1.TradeExecuted) string {
	if _, ok := r.orderToSaga[data.BuyOrderId]; ok {
		return data.BuyOrderId
	}
	if _, ok := r.orderToSaga[data.SellOrderId]; ok {
		return data.SellOrderId
	}
	return ""
}

func (r *Reactor) collectActions() []action {
	if !r.ready {
		return nil
	}

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
	case state.status == PendingEntry && state.entryCancelled:
		return action{sagaID: state.sagaID, kind: actionFailEntryCancelled}, true
	case state.status == PendingEntry && state.filledQty >= state.entryQty:
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
		case actionPlaceExitOrders:
			err = r.executePlaceExitOrders(ctx, a.sagaID)
		case actionRecordExit:
			err = r.executeRecordExit(ctx, a.sagaID)
		case actionFailEntryCancelled:
			err = r.executeFailEntryCancelled(ctx, a.sagaID)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Reactor) executePlaceExitOrders(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != PendingEntry {
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

	tpOrderID, err := r.placeExitOrder(ctx, symbol, exitSide, tpPrice, entryQty, orderbook.Limit, 0)
	if err != nil {
		r.log.Error("failed to place take-profit order", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_exit_orders")
	}

	slOrderID, err := r.placeExitOrder(ctx, symbol, exitSide, 0, entryQty, orderbook.StopMarket, slPrice)
	if err != nil {
		r.log.Error("failed to place stop-loss order", "saga_id", sagaID, "error", err)
		r.trackCancelFailure(ctx, sagaID, symbol, tpOrderID)
		return r.emitActionFailed(ctx, sagaID, "place_exit_orders")
	}

	cmd := RecordEntryFilled{
		SagaID:            sagaID,
		TakeProfitOrderID: tpOrderID,
		StopLossOrderID:   slOrderID,
	}

	err = r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordEntryFilled(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record entry filled", "saga_id", sagaID, "error", err)
		r.trackCancelFailure(ctx, sagaID, symbol, tpOrderID)
		r.trackCancelFailure(ctx, sagaID, symbol, slOrderID)
		return r.emitActionFailed(ctx, sagaID, "place_exit_orders")
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
			return r.emitActionFailed(ctx, sagaID, "record_exit_filled")
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
		return r.emitActionFailed(ctx, sagaID, "record_exit_filled")
	}

	r.log.Info("bracket saga completed",
		"saga_id", sagaID,
		"filled_order_id", filledOrderID,
		"cancelled_order_id", cancelOrderID)
	return nil
}

func (r *Reactor) executeFailEntryCancelled(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != PendingEntry {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	cmd := RecordSagaFailed{
		SagaID: sagaID,
		Reason: "entry order cancelled",
	}
	err := r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordSagaFailed(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record saga failed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "record_saga_failed")
	}

	r.log.Info("bracket saga failed — entry cancelled", "saga_id", sagaID)
	return nil
}

func (r *Reactor) emitActionFailed(ctx context.Context, sagaID, action string) error {
	cmd := RecordActionFailed{
		SagaID: sagaID,
		Action: action,
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

func (r *Reactor) placeExitOrder(ctx context.Context, symbol string, side orderbookv1.Side, price, qty int64, orderType orderbook.OrderType, stopPrice int64) (string, error) {
	cmd := orderbook.PlaceOrder{
		Symbol:      symbol,
		Side:        orderbook.SideFromProto(side),
		Price:       price,
		StopPrice:   stopPrice,
		Quantity:    qty,
		OrderType:   orderType,
		TimeInForce: orderbook.GTC,
	}

	var orderID string
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
	return orderID, err
}
