package ordersaga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

type actionKind int

const (
	actionHoldCash actionKind = iota
	actionPlaceOrder
	actionRecordFills
	actionComplete
	actionReleaseCashAndFail
	actionReleaseResourcesOnFailure
)

type action struct {
	sagaID string
	kind   actionKind
}

type fill struct {
	tradeID  string
	quantity int64
	price    int64
}

type reactorState struct {
	sagaID      string
	accountID   string
	symbol      string
	side        orderbookv1.Side
	price       int64
	quantity    int64
	orderType      orderbook.OrderType
	timeInForce    orderbook.TimeInForce
	replaceOrderID string
	orderID        string
	amountHeld  int64
	filledQty   int64
	cashSettled int64
	status      Status

	pendingFills   []fill
	settledTrades  map[string]struct{}
	orderCancelled bool
	cancelReason   string
	actionPending  bool
}

type Reactor struct {
	sagaHandler      *es.Handler[*OrderSaga]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger

	mu          sync.Mutex
	orderToSaga map[string]string
	sagas       map[string]*reactorState
	lastVersion map[string]int
	ready       bool
}

func NewReactor(
	sagaHandler *es.Handler[*OrderSaga],
	portfolioHandler *es.Handler[*portfolio.Portfolio],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	log *slog.Logger,
) *Reactor {
	return &Reactor{
		sagaHandler:      sagaHandler,
		portfolioHandler: portfolioHandler,
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

	symbolSagas := make(map[string][]string)
	for _, state := range r.sagas {
		if state.status == OrderPlaced && !state.orderCancelled && state.filledQty < state.quantity {
			symbolSagas[state.symbol] = append(symbolSagas[state.symbol], state.sagaID)
		}
	}
	r.mu.Unlock()

	for symbol, sagaIDs := range symbolSagas {
		book, err := r.orderbookHandler.Load(ctx, orderbook.AggregateID(symbol))
		if err != nil {
			r.log.Error("recovery: failed to load orderbook", "symbol", symbol, "error", err)
			continue
		}
		r.mu.Lock()
		for _, sagaID := range sagaIDs {
			r.reconcileFillsFromBook(sagaID, book)
		}
		r.mu.Unlock()
	}

	r.mu.Lock()
	actions := r.collectActions()
	r.mu.Unlock()

	r.executeActions(ctx, actions)
}

// reconcileFillsFromBook adjusts a saga's filled quantity from the orderbook state. Caller must hold r.mu.
func (r *Reactor) reconcileFillsFromBook(sagaID string, book *orderbook.OrderBook) {
	state, ok := r.sagas[sagaID]
	if !ok || state.status != OrderPlaced {
		return
	}

	order, ok := book.Orders[state.orderID]
	if !ok {
		r.log.Info("recovery: order not found in orderbook", "saga_id", sagaID)
		state.orderCancelled = true
		return
	}

	totalFilled := state.quantity - order.RemainingQty
	if totalFilled > state.filledQty {
		// Expected after clean shutdown — orderbook fills may arrive after the last saga checkpoint.
		r.log.Debug("recovery: adjusted filled qty from orderbook", "saga_id", sagaID, "filled", totalFilled)
		state.filledQty = totalFilled
	}
}

func (r *Reactor) HandleEvents(ctx context.Context, events []es.Event) error {
	r.mu.Lock()

	for i := range events {
		r.applySagaEvent(events[i])
	}

	for i := range events {
		r.applyOrderbookEvent(events[i])
	}

	actions := r.collectActions()
	r.mu.Unlock()

	return r.executeActions(ctx, actions)
}

func (r *Reactor) applySagaEvent(evt es.Event) {
	switch data := evt.Data.(type) {
	case *portfoliov1.OrderSagaStarted:
		r.onSagaStarted(data)
	case *portfoliov1.OrderSagaCashHeld:
		r.onCashHeld(data)
	case *portfoliov1.OrderSagaOrderPlaced:
		r.onOrderPlaced(data)
	case *portfoliov1.OrderSagaFillRecorded:
		r.onFillRecorded(data)
	case *portfoliov1.OrderSagaCompleted:
		r.onSagaCompleted(data)
	case *portfoliov1.OrderSagaFailed:
		r.onSagaFailed(data)
	case *portfoliov1.OrderSagaActionFailed:
		r.onActionFailed(data)
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

func (r *Reactor) onSagaStarted(data *portfoliov1.OrderSagaStarted) {
	state := &reactorState{
		sagaID:      data.SagaId,
		accountID:   data.AccountId,
		symbol:      data.Symbol,
		side:        data.Side,
		price:       data.Price,
		quantity:    data.Quantity,
		orderType:      orderbook.OrderTypeFromProto(data.OrderType),
		timeInForce:    orderbook.TimeInForceFromProto(data.TimeInForce),
		replaceOrderID: data.ReplaceOrderId,
		status:         Started,
	}
	r.sagas[data.SagaId] = state
}

func (r *Reactor) onCashHeld(data *portfoliov1.OrderSagaCashHeld) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.amountHeld = data.AmountHeld
	state.status = CashHeld
	state.actionPending = false
}

func (r *Reactor) onOrderPlaced(data *portfoliov1.OrderSagaOrderPlaced) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.orderID = data.OrderId
	state.status = OrderPlaced
	state.actionPending = false
	r.orderToSaga[data.OrderId] = data.SagaId
}

func (r *Reactor) onFillRecorded(data *portfoliov1.OrderSagaFillRecorded) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.actionPending = false
	state.cashSettled += data.CashSettled
	if state.settledTrades == nil {
		state.settledTrades = make(map[string]struct{})
	}
	state.settledTrades[data.TradeId] = struct{}{}
	// Drop any pendingFill for this trade: if TradeExecuted and FillRecorded
	// arrive in different replay batches the trade can be queued before the
	// settle event marks it complete. Without this, executeRecordFills would
	// settle the same trade again on the portfolio.
	if len(state.pendingFills) > 0 {
		kept := state.pendingFills[:0]
		for _, f := range state.pendingFills {
			if f.tradeID != data.TradeId {
				kept = append(kept, f)
			}
		}
		state.pendingFills = kept
	}
}

func (r *Reactor) onSagaCompleted(data *portfoliov1.OrderSagaCompleted) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.status = Completed
	r.cleanupSaga(state)
}

func (r *Reactor) onSagaFailed(data *portfoliov1.OrderSagaFailed) {
	state, ok := r.sagas[data.SagaId]
	if !ok {
		return
	}
	state.status = Failed
	state.actionPending = false

	needsRelease := false
	if state.side == orderbookv1.Side_SIDE_SELL {
		needsRelease = state.amountHeld > state.filledQty
	} else {
		needsRelease = state.amountHeld > state.cashSettled
	}
	if !needsRelease {
		r.cleanupSaga(state)
	}
}

func (r *Reactor) onActionFailed(data *portfoliov1.OrderSagaActionFailed) {
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

	for _, orderID := range r.matchOrderIDs(data) {
		sagaID := r.orderToSaga[orderID]
		state, ok := r.sagas[sagaID]
		if !ok || state.status != OrderPlaced {
			continue
		}

		if _, settled := state.settledTrades[data.TradeId]; settled {
			state.filledQty += data.Quantity
			continue
		}

		state.filledQty += data.Quantity
		state.pendingFills = append(state.pendingFills, fill{
			tradeID:  data.TradeId,
			quantity: data.Quantity,
			price:    data.Price,
		})
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
	if !ok || state.status != OrderPlaced {
		return
	}

	state.orderCancelled = true
	if data.Reason != "" {
		state.cancelReason = data.Reason
	}
}

func (r *Reactor) cleanupSaga(state *reactorState) {
	if state.orderID != "" {
		delete(r.orderToSaga, state.orderID)
	}
	delete(r.sagas, state.sagaID)
}

func (r *Reactor) matchOrderIDs(data *orderbookv1.TradeExecuted) []string {
	var ids []string
	if _, ok := r.orderToSaga[data.BuyOrderId]; ok {
		ids = append(ids, data.BuyOrderId)
	}
	if _, ok := r.orderToSaga[data.SellOrderId]; ok {
		ids = append(ids, data.SellOrderId)
	}
	return ids
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
	case state.status == Started:
		return action{sagaID: state.sagaID, kind: actionHoldCash}, true
	case state.status == CashHeld:
		return action{sagaID: state.sagaID, kind: actionPlaceOrder}, true
	case state.status == OrderPlaced && state.orderCancelled:
		return action{sagaID: state.sagaID, kind: actionReleaseCashAndFail}, true
	case state.status == OrderPlaced && len(state.pendingFills) > 0 && state.filledQty >= state.quantity:
		return action{sagaID: state.sagaID, kind: actionComplete}, true
	case state.status == OrderPlaced && len(state.pendingFills) > 0:
		return action{sagaID: state.sagaID, kind: actionRecordFills}, true
	case state.status == Failed:
		return action{sagaID: state.sagaID, kind: actionReleaseResourcesOnFailure}, true
	}
	return action{}, false
}

func (r *Reactor) executeActions(ctx context.Context, actions []action) error {
	var errs []error
	for _, a := range actions {
		var err error
		switch a.kind {
		case actionHoldCash:
			err = r.executeHoldCash(ctx, a.sagaID)
		case actionPlaceOrder:
			err = r.executePlaceOrder(ctx, a.sagaID)
		case actionRecordFills:
			err = r.executeRecordFills(ctx, a.sagaID)
		case actionComplete:
			err = r.executeComplete(ctx, a.sagaID)
		case actionReleaseCashAndFail:
			err = r.executeReleaseCashAndFail(ctx, a.sagaID)
		case actionReleaseResourcesOnFailure:
			err = r.executeReleaseResourcesOnFailure(ctx, a.sagaID)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Reactor) executeHoldCash(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != Started {
		r.mu.Unlock()
		return nil
	}
	accountID := state.accountID
	symbol := state.symbol
	side := state.side
	oType := state.orderType
	price := state.price
	quantity := state.quantity
	r.mu.Unlock()

	if oType == orderbook.Market && side == orderbookv1.Side_SIDE_BUY {
		book, err := r.orderbookHandler.Load(ctx, orderbook.AggregateID(symbol))
		if err != nil {
			r.log.Error("failed to load orderbook for market order hold", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
		}
		price = book.Asks.BestPrice()
		if price == 0 {
			r.log.Error("no ask liquidity for market buy hold", "saga_id", sagaID)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", "no ask liquidity for market buy")
		}
	}

	cashAmount := computeHoldAmount(oType, side, price, quantity)
	shareQty := computeShareHoldQuantity(oType, side, quantity)

	if side == orderbookv1.Side_SIDE_SELL && shareQty > 0 {
		holdCmd := portfolio.HoldShares{
			AccountID:   accountID,
			OrderSagaID: sagaID,
			Symbol:      symbol,
			Quantity:    shareQty,
		}
		err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteHoldShares(p, holdCmd)
		})
		if err != nil {
			r.log.Error("failed to hold shares", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
		}

		cmd := RecordCashHeld{SagaID: sagaID, AmountHeld: shareQty}
		err = r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
			return ExecuteRecordCashHeld(saga, cmd)
		})
		if err != nil {
			r.log.Error("failed to record shares held", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
		}

		r.log.Info("order saga shares held", "saga_id", sagaID, "quantity", shareQty)
		return nil
	}

	if cashAmount == 0 {
		cmd := RecordCashHeld{SagaID: sagaID, AmountHeld: 0}
		err := r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
			return ExecuteRecordCashHeld(saga, cmd)
		})
		if err != nil {
			r.log.Error("failed to record cash held", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
		}
		r.log.Info("order saga cash hold skipped (no hold needed)", "saga_id", sagaID)
		return nil
	}

	holdCmd := portfolio.HoldCash{
		AccountID:   accountID,
		OrderSagaID: sagaID,
		Amount:      cashAmount,
	}
	err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCash(p, holdCmd)
	})
	if err != nil {
		r.log.Error("failed to hold cash", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
	}

	cmd := RecordCashHeld{SagaID: sagaID, AmountHeld: cashAmount}
	err = r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordCashHeld(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record cash held", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
	}

	r.log.Info("order saga cash held", "saga_id", sagaID, "amount", cashAmount)
	return nil
}

func (r *Reactor) executePlaceOrder(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != CashHeld {
		r.mu.Unlock()
		return nil
	}
	symbol := state.symbol
	side := state.side
	price := state.price
	quantity := state.quantity
	orderType := state.orderType
	timeInForce := state.timeInForce
	accountID := state.accountID
	replaceOrderID := state.replaceOrderID
	r.mu.Unlock()

	// Derive orderID from sagaID so that re-running this action after a
	// crash between PlaceOrder and RecordOrderPlaced computes the same ID,
	// and the orderbook's duplicate-OrderID check makes the retry a no-op.
	orderID := OrderID(sagaID)
	r.mu.Lock()
	r.orderToSaga[orderID] = sagaID
	r.mu.Unlock()

	// Capture pre-placement RemainingQty (sentinel -1 = no existing order)
	// to detect the recovery case where the order was placed before a
	// previous crash. The closure runs inside Handle's aggregate load, so
	// reading book.Orders here doesn't cost an extra round-trip.
	preExistingRemaining := int64(-1)
	captureExisting := func(book *orderbook.OrderBook) {
		if existing, ok := book.Orders[orderID]; ok {
			preExistingRemaining = existing.RemainingQty
		}
	}

	var err error
	if replaceOrderID != "" {
		replaceCmd := orderbook.ReplaceOrder{
			Symbol:      symbol,
			OldOrderID:  replaceOrderID,
			NewOrderID:  orderID,
			Side:        orderbook.SideFromProto(side),
			Price:       price,
			Quantity:    quantity,
			OrderType:   orderType,
			TimeInForce: timeInForce,
			AccountID:   accountID,
		}
		err = r.orderbookHandler.Handle(ctx, replaceCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
			captureExisting(book)
			return orderbook.ExecuteReplaceOrder(book, replaceCmd)
		})
	} else {
		placeCmd := orderbook.PlaceOrder{
			Symbol:      symbol,
			Side:        orderbook.SideFromProto(side),
			Price:       price,
			Quantity:    quantity,
			OrderType:   orderType,
			TimeInForce: timeInForce,
			AccountID:   accountID,
			OrderID:     orderID,
		}
		err = r.orderbookHandler.Handle(ctx, placeCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
			captureExisting(book)
			return orderbook.ExecutePlaceOrder(book, placeCmd)
		})
	}
	if err != nil {
		r.mu.Lock()
		delete(r.orderToSaga, orderID)
		r.mu.Unlock()
		r.log.Error("failed to place order", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_order", err.Error())
	}

	// Dirty-recovery detection: the order was already on the book AND had
	// fewer remaining shares than expected, meaning trades executed against
	// it before the previous crash. Those TradeExecuted events were skipped
	// during NATS replay (no orderToSaga mapping at the time), so the saga
	// reactor cannot reconstruct fills and the portfolio is missing
	// settlements. Fail the saga loudly rather than silently advancing into
	// OrderPlaced with filledQty=0 (which would later over-release the hold
	// and report an over-completed order). An operator must reconcile the
	// missed trades manually against the orderbook event log.
	if preExistingRemaining >= 0 && preExistingRemaining < quantity {
		return r.failDirtyRecovery(ctx, sagaID, accountID, symbol, side, orderID, quantity, preExistingRemaining)
	}

	cmd := RecordOrderPlaced{SagaID: sagaID, OrderID: orderID}
	err = r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordOrderPlaced(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record order placed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_order", err.Error())
	}

	r.log.Info("order saga order placed", "saga_id", sagaID, "order_id", orderID)
	return nil
}

func (r *Reactor) executeRecordFills(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != OrderPlaced {
		r.mu.Unlock()
		return nil
	}
	fills := state.pendingFills
	state.pendingFills = nil
	accountID := state.accountID
	symbol := state.symbol
	side := state.side
	r.mu.Unlock()

	for _, f := range fills {
		cashAmount := f.price * f.quantity

		if side == orderbookv1.Side_SIDE_SELL {
			settleCmd := portfolio.SettleSale{
				AccountID:     accountID,
				OrderSagaID:   sagaID,
				Symbol:        symbol,
				Quantity:      f.quantity,
				PricePerShare: f.price,
				Proceeds:      cashAmount,
			}
			err := r.portfolioHandler.Handle(ctx, settleCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteSettleSale(p, settleCmd)
			})
			if err != nil {
				r.log.Error("failed to settle sale on portfolio", "saga_id", sagaID, "trade_id", f.tradeID, "error", err)
				r.mu.Lock()
				if s, ok := r.sagas[sagaID]; ok {
					s.pendingFills = append(s.pendingFills, f)
				}
				r.mu.Unlock()
				return r.emitActionFailed(ctx, sagaID, "record_fills", err.Error())
			}
		} else {
			settleCmd := portfolio.SettleTrade{
				AccountID:    accountID,
				OrderSagaID:  sagaID,
				Amount:       cashAmount,
				Symbol:       symbol,
				Quantity:     f.quantity,
				CostPerShare: f.price,
			}
			err := r.portfolioHandler.Handle(ctx, settleCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteSettleTrade(p, settleCmd)
			})
			if err != nil {
				r.log.Error("failed to settle trade on portfolio", "saga_id", sagaID, "trade_id", f.tradeID, "error", err)
				r.mu.Lock()
				if s, ok := r.sagas[sagaID]; ok {
					s.pendingFills = append(s.pendingFills, f)
				}
				r.mu.Unlock()
				return r.emitActionFailed(ctx, sagaID, "record_fills", err.Error())
			}
		}

		fillCmd := RecordFill{
			SagaID:       sagaID,
			TradeID:      f.tradeID,
			FillQuantity: f.quantity,
			FillPrice:    f.price,
			CashSettled:  cashAmount,
		}
		err := r.sagaHandler.Handle(ctx, fillCmd, func(saga *OrderSaga) ([]es.Event, error) {
			return ExecuteRecordFill(saga, fillCmd)
		})
		if err != nil {
			r.log.Error("failed to record fill", "saga_id", sagaID, "trade_id", f.tradeID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "record_fills", err.Error())
		}

		r.mu.Lock()
		if s, ok := r.sagas[sagaID]; ok {
			s.cashSettled += cashAmount
		}
		r.mu.Unlock()
	}

	r.log.Info("order saga fills recorded", "saga_id", sagaID, "count", len(fills))
	return nil
}

func (r *Reactor) executeComplete(ctx context.Context, sagaID string) error {
	if err := r.executeRecordFills(ctx, sagaID); err != nil {
		return err
	}

	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != OrderPlaced {
		r.mu.Unlock()
		return nil
	}
	accountID := state.accountID
	symbol := state.symbol
	side := state.side
	amountHeld := state.amountHeld
	filledQty := state.filledQty
	cashSettled := state.cashSettled
	r.mu.Unlock()

	if side == orderbookv1.Side_SIDE_SELL {
		remainingShares := amountHeld - filledQty
		if remainingShares > 0 {
			releaseCmd := portfolio.ReleaseShares{
				AccountID:   accountID,
				OrderSagaID: sagaID,
				Symbol:      symbol,
				Quantity:    remainingShares,
			}
			err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseShares(p, releaseCmd)
			})
			if err != nil {
				r.log.Error("failed to release remaining shares", "saga_id", sagaID, "error", err)
				return r.emitActionFailed(ctx, sagaID, "complete", err.Error())
			}
		}
	} else {
		remaining := amountHeld - cashSettled
		if remaining > 0 {
			releaseCmd := portfolio.ReleaseCash{
				AccountID:   accountID,
				OrderSagaID: sagaID,
				Amount:      remaining,
			}
			err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseCash(p, releaseCmd)
			})
			if err != nil {
				r.log.Error("failed to release remaining cash", "saga_id", sagaID, "error", err)
				return r.emitActionFailed(ctx, sagaID, "complete", err.Error())
			}
		}
	}

	cmd := RecordCompleted{SagaID: sagaID}
	err := r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordCompleted(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record completed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "complete", err.Error())
	}

	r.log.Info("order saga completed", "saga_id", sagaID)
	return nil
}

func (r *Reactor) executeReleaseCashAndFail(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != OrderPlaced {
		r.mu.Unlock()
		return nil
	}
	accountID := state.accountID
	symbol := state.symbol
	side := state.side
	amountHeld := state.amountHeld
	filledQty := state.filledQty
	cashSettled := state.cashSettled
	r.mu.Unlock()

	if side == orderbookv1.Side_SIDE_SELL {
		remainingShares := amountHeld - filledQty
		if remainingShares > 0 {
			releaseCmd := portfolio.ReleaseShares{
				AccountID:   accountID,
				OrderSagaID: sagaID,
				Symbol:      symbol,
				Quantity:    remainingShares,
			}
			err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseShares(p, releaseCmd)
			})
			if err != nil {
				r.log.Error("failed to release shares on cancel", "saga_id", sagaID, "error", err)
				return r.emitActionFailed(ctx, sagaID, "release_cash_and_fail", err.Error())
			}
		}
		r.mu.Lock()
		if s, ok := r.sagas[sagaID]; ok {
			s.filledQty = s.amountHeld
		}
		r.mu.Unlock()
	} else {
		remaining := amountHeld - cashSettled
		if remaining > 0 {
			releaseCmd := portfolio.ReleaseCash{
				AccountID:   accountID,
				OrderSagaID: sagaID,
				Amount:      remaining,
			}
			err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseCash(p, releaseCmd)
			})
			if err != nil {
				r.log.Error("failed to release cash on cancel", "saga_id", sagaID, "error", err)
				return r.emitActionFailed(ctx, sagaID, "release_cash_and_fail", err.Error())
			}
		}
		r.mu.Lock()
		if s, ok := r.sagas[sagaID]; ok {
			s.cashSettled = s.amountHeld
		}
		r.mu.Unlock()
	}

	reason := "order cancelled"
	r.mu.Lock()
	if s, ok := r.sagas[sagaID]; ok && s.cancelReason != "" {
		reason = s.cancelReason
	}
	r.mu.Unlock()

	cmd := RecordFailed{SagaID: sagaID, Reason: reason}
	err := r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordFailed(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to record saga failed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "release_cash_and_fail", err.Error())
	}

	r.log.Info("order saga failed — order cancelled", "saga_id", sagaID)
	return nil
}

func (r *Reactor) executeReleaseResourcesOnFailure(ctx context.Context, sagaID string) error {
	r.mu.Lock()
	state, ok := r.sagas[sagaID]
	if !ok || state.status != Failed {
		r.mu.Unlock()
		return nil
	}
	accountID := state.accountID
	symbol := state.symbol
	side := state.side
	amountHeld := state.amountHeld
	filledQty := state.filledQty
	cashSettled := state.cashSettled
	r.mu.Unlock()

	if side == orderbookv1.Side_SIDE_SELL {
		remainingShares := amountHeld - filledQty
		if remainingShares > 0 {
			releaseCmd := portfolio.ReleaseShares{
				AccountID:   accountID,
				OrderSagaID: sagaID,
				Symbol:      symbol,
				Quantity:    remainingShares,
			}
			if err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseShares(p, releaseCmd)
			}); err != nil {
				r.log.Error("failed to release shares on failure", "saga_id", sagaID, "error", err)
				return err
			}
		}
	} else {
		remaining := amountHeld - cashSettled
		if remaining > 0 {
			releaseCmd := portfolio.ReleaseCash{
				AccountID:   accountID,
				OrderSagaID: sagaID,
				Amount:      remaining,
			}
			if err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseCash(p, releaseCmd)
			}); err != nil {
				r.log.Error("failed to release cash on failure", "saga_id", sagaID, "error", err)
				return err
			}
		}
	}

	r.mu.Lock()
	r.cleanupSaga(state)
	r.mu.Unlock()

	r.log.Info("order saga cleanup after failure", "saga_id", sagaID)
	return nil
}

// failDirtyRecovery handles a place_order retry where the orderbook has
// the order with fewer shares remaining than expected — fills happened
// before the previous crash that the reactor cannot reconstruct. It
// releases the portfolio's actual remaining hold (queried directly so the
// amount is correct even though reactor in-memory state has filledQty=0)
// and records the saga as failed. The downstream release in the failure
// flow becomes a no-op via the per-saga idempotency check.
func (r *Reactor) failDirtyRecovery(ctx context.Context, sagaID, accountID, symbol string, side orderbookv1.Side, orderID string, quantity, remaining int64) error {
	missedFills := quantity - remaining
	r.log.Error("place_order recovery detected unreconciled fills — failing saga",
		"saga_id", sagaID, "order_id", orderID,
		"expected_qty", quantity, "remaining_qty", remaining, "missed_fills", missedFills)

	p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(accountID))
	if err != nil {
		r.log.Error("dirty recovery: failed to load portfolio", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_order", err.Error())
	}

	if side == orderbookv1.Side_SIDE_SELL {
		if hold := p.ShareHoldsBySaga[sagaID]; hold != nil && hold.Quantity > 0 {
			releaseCmd := portfolio.ReleaseShares{
				AccountID:   accountID,
				OrderSagaID: sagaID,
				Symbol:      symbol,
				Quantity:    hold.Quantity,
			}
			if err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseShares(p, releaseCmd)
			}); err != nil {
				r.log.Error("dirty recovery: failed to release shares", "saga_id", sagaID, "error", err)
				return r.emitActionFailed(ctx, sagaID, "place_order", err.Error())
			}
		}
	} else {
		if amount := p.HoldsBySaga[sagaID]; amount > 0 {
			releaseCmd := portfolio.ReleaseCash{
				AccountID:   accountID,
				OrderSagaID: sagaID,
				Amount:      amount,
			}
			if err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseCash(p, releaseCmd)
			}); err != nil {
				r.log.Error("dirty recovery: failed to release cash", "saga_id", sagaID, "error", err)
				return r.emitActionFailed(ctx, sagaID, "place_order", err.Error())
			}
		}
	}

	reason := fmt.Sprintf("recovery: order had %d shares filled before crash; manual reconciliation required", missedFills)
	failCmd := RecordFailed{SagaID: sagaID, Reason: reason}
	if err := r.sagaHandler.Handle(ctx, failCmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordFailed(saga, failCmd)
	}); err != nil {
		r.log.Error("dirty recovery: failed to record saga failed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_order", err.Error())
	}
	return nil
}

func (r *Reactor) emitActionFailed(ctx context.Context, sagaID, action, reason string) error {
	cmd := RecordActionFailed{
		SagaID: sagaID,
		Action: action,
		Reason: reason,
	}
	err := r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordActionFailed(saga, cmd)
	})
	if err != nil {
		r.log.Error("failed to emit action failed event", "saga_id", sagaID, "action", action, "error", err)
		return fmt.Errorf("saga %s: failed to emit action failed for %s: %w", sagaID, action, err)
	}
	return nil
}

func computeHoldAmount(orderType orderbook.OrderType, side orderbookv1.Side, price, quantity int64) int64 {
	if side == orderbookv1.Side_SIDE_SELL {
		return 0
	}
	return price * quantity
}

func computeShareHoldQuantity(orderType orderbook.OrderType, side orderbookv1.Side, quantity int64) int64 {
	if side == orderbookv1.Side_SIDE_BUY {
		return 0
	}
	return quantity
}
