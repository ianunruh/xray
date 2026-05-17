package ordersaga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

// Reactor drives the order-saga lifecycle by reacting to events from
// the saga, portfolio, and orderbook streams. It holds no in-memory
// state — every decision is made by loading the relevant aggregates
// at event time. Replays are safe because every command we issue is
// either per-key idempotent (holds, releases, per-trade settlements)
// or status-guarded (saga lifecycle transitions).
type Reactor struct {
	sagaHandler      *es.Handler[*OrderSaga]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger
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
	}
}

func (r *Reactor) HandleEvents(ctx context.Context, events []es.Event) error {
	var errs []error
	for _, evt := range events {
		if err := r.handleOne(ctx, evt); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Reactor) handleOne(ctx context.Context, evt es.Event) error {
	ctx = es.WithCausation(ctx, evt)
	switch data := evt.Data.(type) {
	case *portfoliov1.OrderSagaStarted:
		return r.holdResources(ctx, data.SagaId)
	case *portfoliov1.OrderSagaCashHeld:
		return r.placeOrder(ctx, data.SagaId)
	case *portfoliov1.OrderSagaFillRecorded:
		return r.maybeComplete(ctx, data.SagaId)
	case *portfoliov1.OrderSagaFailed:
		return r.releaseRemainingHolds(ctx, data.SagaId)
	case *portfoliov1.OrderSagaActionFailed:
		return r.retry(ctx, data.SagaId)
	case *orderbookv1.TradeExecuted:
		return r.onTrade(ctx, data)
	case *orderbookv1.OrderCancelled:
		return r.onCancel(ctx, data)
	}
	return nil
}

// holdResources runs the initial Hold step for a freshly-started saga:
// cash hold for buys (walking the book + slippage buffer for market
// orders), share hold for sells, or a zero-amount placeholder when no
// hold is needed.
func (r *Reactor) holdResources(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != Started {
		return nil
	}

	side := orderbook.SideToProto(saga.Side)
	if saga.Side == orderbook.Sell {
		shareQty := saga.Quantity
		holdCmd := portfolio.HoldShares{
			AccountID:   saga.AccountID,
			OrderSagaID: sagaID,
			Symbol:      saga.Symbol,
			Quantity:    shareQty,
		}
		if err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteHoldShares(p, holdCmd)
		}); err != nil {
			r.log.Error("failed to hold shares", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
		}
		return r.recordCashHeld(ctx, sagaID, shareQty)
	}

	var cashAmount int64
	if saga.OrderType == orderbook.Market && side == orderbookv1.Side_SIDE_BUY {
		book, err := r.orderbookHandler.Load(ctx, orderbook.AggregateID(saga.Symbol))
		if err != nil {
			r.log.Error("failed to load orderbook for market order hold", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
		}
		swept, ok := book.EstimateMarketBuyCost(saga.Quantity)
		if !ok {
			r.log.Error("no ask liquidity for market buy hold", "saga_id", sagaID)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", "no ask liquidity for market buy")
		}
		// Pad for slippage between hold time and execution time. Round up
		// so even a 1-unit estimate gets a buffer.
		cashAmount = (swept*marketBuySlippageBps + slippageBpsScale - 1) / slippageBpsScale
	} else {
		cashAmount = computeHoldAmount(saga.OrderType, side, saga.Price, saga.Quantity)
	}

	if cashAmount > 0 {
		holdCmd := portfolio.HoldCash{
			AccountID:   saga.AccountID,
			OrderSagaID: sagaID,
			Amount:      cashAmount,
		}
		if err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteHoldCash(p, holdCmd)
		}); err != nil {
			r.log.Error("failed to hold cash", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
		}
	}
	return r.recordCashHeld(ctx, sagaID, cashAmount)
}

func (r *Reactor) recordCashHeld(ctx context.Context, sagaID string, amount int64) error {
	cmd := RecordCashHeld{SagaID: sagaID, AmountHeld: amount}
	if err := r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordCashHeld(saga, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record cash held", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "hold_cash", err.Error())
	}
	r.log.Info("order saga cash held", "saga_id", sagaID, "amount", amount)
	return nil
}

// placeOrder runs the Place step once the saga has its hold. Order IDs
// are deterministic, so retries are no-ops at the orderbook layer.
func (r *Reactor) placeOrder(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != CashHeld {
		return nil
	}

	orderID := OrderID(sagaID)
	if saga.ReplaceOrderID != "" {
		replaceCmd := orderbook.ReplaceOrder{
			Symbol:      saga.Symbol,
			OldOrderID:  saga.ReplaceOrderID,
			NewOrderID:  orderID,
			Side:        saga.Side,
			Price:       saga.Price,
			Quantity:    saga.Quantity,
			OrderType:   saga.OrderType,
			TimeInForce: saga.TimeInForce,
			AccountID:   saga.AccountID,
		}
		err = r.orderbookHandler.Handle(ctx, replaceCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
			return orderbook.ExecuteReplaceOrder(book, replaceCmd)
		})
	} else {
		placeCmd := orderbook.PlaceOrder{
			Symbol:      saga.Symbol,
			Side:        saga.Side,
			Price:       saga.Price,
			Quantity:    saga.Quantity,
			OrderType:   saga.OrderType,
			TimeInForce: saga.TimeInForce,
			AccountID:   saga.AccountID,
			OrderID:     orderID,
		}
		err = r.orderbookHandler.Handle(ctx, placeCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
			return orderbook.ExecutePlaceOrder(book, placeCmd)
		})
	}
	if err != nil {
		r.log.Error("failed to place order", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_order", err.Error())
	}

	cmd := RecordOrderPlaced{SagaID: sagaID, OrderID: orderID}
	if err := r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordOrderPlaced(saga, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record order placed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_order", err.Error())
	}
	r.log.Info("order saga order placed", "saga_id", sagaID, "order_id", orderID)
	return nil
}

// onTrade settles a single matched trade against the portfolio and
// records the fill on the saga. Both commands are per-trade idempotent
// so replays are safe.
func (r *Reactor) onTrade(ctx context.Context, data *orderbookv1.TradeExecuted) error {
	var firstErr error
	for _, orderID := range []string{data.BuyOrderId, data.SellOrderId} {
		sagaID, ok := sagaIDFromOrderID(orderID)
		if !ok {
			continue
		}
		if err := r.settleTrade(ctx, sagaID, data); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Reactor) settleTrade(ctx context.Context, sagaID string, data *orderbookv1.TradeExecuted) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != OrderPlaced {
		return nil
	}
	cashAmount := data.Price * data.Quantity

	if saga.Side == orderbook.Sell {
		settleCmd := portfolio.SettleSale{
			AccountID:     saga.AccountID,
			OrderSagaID:   sagaID,
			TradeID:       data.TradeId,
			Symbol:        saga.Symbol,
			Quantity:      data.Quantity,
			PricePerShare: data.Price,
			Proceeds:      cashAmount,
		}
		if err := r.portfolioHandler.Handle(ctx, settleCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteSettleSale(p, settleCmd)
		}); err != nil {
			r.log.Error("failed to settle sale on portfolio", "saga_id", sagaID, "trade_id", data.TradeId, "error", err)
			return r.emitActionFailed(ctx, sagaID, "record_fills", err.Error())
		}
	} else {
		settleCmd := portfolio.SettleTrade{
			AccountID:    saga.AccountID,
			OrderSagaID:  sagaID,
			TradeID:      data.TradeId,
			Amount:       cashAmount,
			Symbol:       saga.Symbol,
			Quantity:     data.Quantity,
			CostPerShare: data.Price,
		}
		if err := r.portfolioHandler.Handle(ctx, settleCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteSettleTrade(p, settleCmd)
		}); err != nil {
			r.log.Error("failed to settle trade on portfolio", "saga_id", sagaID, "trade_id", data.TradeId, "error", err)
			return r.emitActionFailed(ctx, sagaID, "record_fills", err.Error())
		}
	}

	fillCmd := RecordFill{
		SagaID:       sagaID,
		TradeID:      data.TradeId,
		FillQuantity: data.Quantity,
		FillPrice:    data.Price,
		CashSettled:  cashAmount,
	}
	if err := r.sagaHandler.Handle(ctx, fillCmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordFill(saga, fillCmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record fill", "saga_id", sagaID, "trade_id", data.TradeId, "error", err)
		return r.emitActionFailed(ctx, sagaID, "record_fills", err.Error())
	}
	return nil
}

// maybeComplete fires when a fill record arrives. If FilledQty has
// caught up to Quantity, release the unused hold and complete the saga.
func (r *Reactor) maybeComplete(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != OrderPlaced || saga.FilledQty < saga.Quantity {
		return nil
	}
	return r.completeSaga(ctx, saga)
}

func (r *Reactor) completeSaga(ctx context.Context, saga *OrderSaga) error {
	if saga.Side == orderbook.Sell {
		remainingShares := saga.AmountHeld - saga.FilledQty
		if remainingShares > 0 {
			releaseCmd := portfolio.ReleaseShares{
				AccountID:   saga.AccountID,
				OrderSagaID: saga.SagaID,
				Symbol:      saga.Symbol,
				Quantity:    remainingShares,
			}
			if err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseShares(p, releaseCmd)
			}); err != nil {
				r.log.Error("failed to release remaining shares", "saga_id", saga.SagaID, "error", err)
				return r.emitActionFailed(ctx, saga.SagaID, "complete", err.Error())
			}
		}
	} else {
		remaining := saga.AmountHeld - saga.CashSettled
		if remaining > 0 {
			releaseCmd := portfolio.ReleaseCash{
				AccountID:   saga.AccountID,
				OrderSagaID: saga.SagaID,
				Amount:      remaining,
			}
			if err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseCash(p, releaseCmd)
			}); err != nil {
				r.log.Error("failed to release remaining cash", "saga_id", saga.SagaID, "error", err)
				return r.emitActionFailed(ctx, saga.SagaID, "complete", err.Error())
			}
		}
	}

	cmd := RecordCompleted{SagaID: saga.SagaID}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordCompleted(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record completed", "saga_id", saga.SagaID, "error", err)
		return r.emitActionFailed(ctx, saga.SagaID, "complete", err.Error())
	}
	r.log.Info("order saga completed", "saga_id", saga.SagaID)
	return nil
}

// onCancel handles an OrderCancelled event for one of our orders: release
// the remaining hold and mark the saga failed.
func (r *Reactor) onCancel(ctx context.Context, data *orderbookv1.OrderCancelled) error {
	sagaID, ok := sagaIDFromOrderID(data.OrderId)
	if !ok {
		return nil
	}
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != OrderPlaced {
		return nil
	}

	if err := r.releaseRemainingHoldsForSaga(ctx, saga); err != nil {
		return err
	}

	reason := data.Reason
	if reason == "" {
		reason = "order cancelled"
	}
	failCmd := RecordFailed{SagaID: sagaID, Reason: reason}
	if err := r.sagaHandler.Handle(ctx, failCmd, func(s *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordFailed(s, failCmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record saga failed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "release_cash_and_fail", err.Error())
	}
	r.log.Info("order saga failed — order cancelled", "saga_id", sagaID)
	return nil
}

// releaseRemainingHolds runs in response to OrderSagaFailed events.
// If the failure path left a hold behind (e.g., place-order failed
// after a hold was placed), release it.
func (r *Reactor) releaseRemainingHolds(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != Failed {
		return nil
	}
	return r.releaseRemainingHoldsForSaga(ctx, saga)
}

func (r *Reactor) releaseRemainingHoldsForSaga(ctx context.Context, saga *OrderSaga) error {
	p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(saga.AccountID))
	if err != nil {
		return fmt.Errorf("load portfolio: %w", err)
	}

	if saga.Side == orderbook.Sell {
		hold, ok := p.ShareHoldsBySaga[saga.SagaID]
		if !ok || hold.Quantity <= 0 {
			return nil
		}
		releaseCmd := portfolio.ReleaseShares{
			AccountID:   saga.AccountID,
			OrderSagaID: saga.SagaID,
			Symbol:      hold.Symbol,
			Quantity:    hold.Quantity,
		}
		if err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteReleaseShares(p, releaseCmd)
		}); err != nil {
			r.log.Error("failed to release shares on failure", "saga_id", saga.SagaID, "error", err)
			return r.emitActionFailed(ctx, saga.SagaID, "release_resources_on_failure", err.Error())
		}
		return nil
	}

	remaining, ok := p.HoldsBySaga[saga.SagaID]
	if !ok || remaining <= 0 {
		return nil
	}
	releaseCmd := portfolio.ReleaseCash{
		AccountID:   saga.AccountID,
		OrderSagaID: saga.SagaID,
		Amount:      remaining,
	}
	if err := r.portfolioHandler.Handle(ctx, releaseCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseCash(p, releaseCmd)
	}); err != nil {
		r.log.Error("failed to release cash on failure", "saga_id", saga.SagaID, "error", err)
		return r.emitActionFailed(ctx, saga.SagaID, "release_resources_on_failure", err.Error())
	}
	return nil
}

// Reconcile drives a saga's state machine forward from whatever its
// current durable state is. Exported for the periodic reconciler;
// equivalent to handling an OrderSagaActionFailed event for this saga.
func (r *Reactor) Reconcile(ctx context.Context, sagaID string) error {
	return r.retry(ctx, sagaID)
}

// ReplayTrade re-runs the trade-fill handler for a previously-observed
// TradeExecuted. Used by the reconciler to settle trades whose original
// settle command was lost. Idempotent: per-trade dedup on the portfolio
// and ErrInvalidState guards on the saga make replays safe.
func (r *Reactor) ReplayTrade(ctx context.Context, data *orderbookv1.TradeExecuted) error {
	return r.onTrade(ctx, data)
}

// retry is the universal "previous action failed, look at the current
// state and re-derive what to do" handler.
func (r *Reactor) retry(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	switch saga.Status {
	case Started:
		return r.holdResources(ctx, sagaID)
	case CashHeld:
		return r.placeOrder(ctx, sagaID)
	case OrderPlaced:
		// Check if the saga is past completion; otherwise just wait for
		// the next trade or cancel event to drive things forward.
		if saga.FilledQty >= saga.Quantity {
			return r.completeSaga(ctx, saga)
		}
	case Failed:
		return r.releaseRemainingHoldsForSaga(ctx, saga)
	}
	return nil
}

func (r *Reactor) emitActionFailed(ctx context.Context, sagaID, action, reason string) error {
	cmd := RecordActionFailed{
		SagaID: sagaID,
		Action: action,
		Reason: reason,
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordActionFailed(saga, cmd)
	}); err != nil {
		r.log.Error("failed to emit action failed event", "saga_id", sagaID, "action", action, "error", err)
		return fmt.Errorf("saga %s: failed to emit action failed for %s: %w", sagaID, action, err)
	}
	return nil
}

// marketBuySlippageBps pads the hold computed from walking the ask book
// to cover slippage between hold time and execution time. 10500 bps =
// 1.05×.
const (
	marketBuySlippageBps = 10500
	slippageBpsScale     = 10000
)

func computeHoldAmount(_ orderbook.OrderType, side orderbookv1.Side, price, quantity int64) int64 {
	if side == orderbookv1.Side_SIDE_SELL {
		return 0
	}
	return price * quantity
}
