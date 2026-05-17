package ordersaga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/margin"
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
	case *portfoliov1.OrderSagaCollateralHeld:
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

// holdResources runs the initial Hold step. The four (side, position_side)
// combos each take a different portfolio command:
//
//	BUY  + LONG   -> HoldCash       (existing long buy)
//	SELL + LONG   -> HoldShares     (existing long sell)
//	SELL + SHORT  -> HoldCollateral (sell to open)
//	BUY  + SHORT  -> HoldShortCover (buy to cover)
func (r *Reactor) holdResources(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != Started {
		return nil
	}

	// Defensive margin-call gate. sagasvc.Place enforces the same
	// rule at submit time, but a call can fire between Place and
	// here (e.g. a trade trigger lands in the gap). The saga has
	// to either run or fail with a clean reason — silently
	// proceeding would compound the breach. MARGIN_CALL-initiated
	// sagas (the liquidations themselves) skip the check.
	if saga.Initiator != sagav1.Initiator_INITIATOR_MARGIN_CALL &&
		portfolio.IsExposureAdding(orderbook.SideToProto(saga.Side), saga.PositionSide) {
		p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(saga.AccountID))
		if err != nil {
			r.log.Error("hold_resources: load portfolio", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_resources", err.Error())
		}
		if p.ActiveMarginCall != nil {
			return r.emitActionFailed(ctx, sagaID, "in_margin_call",
				"account in margin call; cannot add exposure")
		}
	}

	switch {
	case saga.Side == orderbook.Buy && saga.PositionSide != orderbookv1.PositionSide_POSITION_SIDE_SHORT:
		return r.holdCashForBuy(ctx, saga)
	case saga.Side == orderbook.Sell && saga.PositionSide != orderbookv1.PositionSide_POSITION_SIDE_SHORT:
		return r.holdSharesForSell(ctx, saga)
	case saga.Side == orderbook.Sell && saga.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT:
		return r.holdCollateralForShortOpen(ctx, saga)
	case saga.Side == orderbook.Buy && saga.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT:
		return r.holdCapacityForCover(ctx, saga)
	}
	return nil
}

func (r *Reactor) holdCashForBuy(ctx context.Context, saga *OrderSaga) error {
	cashAmount, err := r.estimateBuyCashHold(ctx, saga)
	if err != nil {
		return r.emitActionFailed(ctx, saga.SagaID, "hold_cash", err.Error())
	}
	if cashAmount > 0 {
		holdCmd := portfolio.HoldCash{
			AccountID:   saga.AccountID,
			OrderSagaID: saga.SagaID,
			Symbol:      saga.Symbol,
			Amount:      cashAmount,
		}
		if err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteHoldCash(p, holdCmd)
		}); err != nil {
			r.log.Error("failed to hold cash", "saga_id", saga.SagaID, "error", err)
			return r.emitActionFailed(ctx, saga.SagaID, "hold_cash", err.Error())
		}
	}
	return r.recordCashHeld(ctx, saga.SagaID, cashAmount)
}

func (r *Reactor) estimateBuyCashHold(ctx context.Context, saga *OrderSaga) (int64, error) {
	if saga.OrderType != orderbook.Market {
		return saga.Price * saga.Quantity, nil
	}
	book, err := r.orderbookHandler.Load(ctx, orderbook.AggregateID(saga.Symbol))
	if err != nil {
		return 0, fmt.Errorf("load orderbook: %w", err)
	}
	swept, ok := book.EstimateMarketBuyCost(saga.Quantity)
	if !ok {
		return 0, errors.New("no ask liquidity for market buy")
	}
	return padForSlippage(swept), nil
}

func (r *Reactor) holdSharesForSell(ctx context.Context, saga *OrderSaga) error {
	holdCmd := portfolio.HoldShares{
		AccountID:   saga.AccountID,
		OrderSagaID: saga.SagaID,
		Symbol:      saga.Symbol,
		Quantity:    saga.Quantity,
	}
	if err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, holdCmd)
	}); err != nil {
		r.log.Error("failed to hold shares", "saga_id", saga.SagaID, "error", err)
		return r.emitActionFailed(ctx, saga.SagaID, "hold_shares", err.Error())
	}
	return r.recordCashHeld(ctx, saga.SagaID, saga.Quantity)
}

func (r *Reactor) holdCollateralForShortOpen(ctx context.Context, saga *OrderSaga) error {
	proceeds, err := r.estimateShortOpenProceeds(ctx, saga)
	if err != nil {
		return r.emitActionFailed(ctx, saga.SagaID, "hold_collateral", err.Error())
	}
	// Collateral is sized off the *estimated* sale proceeds; if the
	// fill actually clears at a worse price, ExecuteOpenShort's overflow
	// path debits CashBalance directly.
	collateral := margin.CollateralForShortOpen(proceeds/max64(saga.Quantity, 1), saga.Quantity)
	if saga.OrderType == orderbook.Market {
		// Pad for slippage on a market short the same way we pad market buys.
		collateral = padForSlippage(collateral)
	}

	holdCmd := portfolio.HoldCollateral{
		AccountID:   saga.AccountID,
		OrderSagaID: saga.SagaID,
		Symbol:      saga.Symbol,
		Quantity:    saga.Quantity,
		Amount:      collateral,
	}
	if err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldCollateral(p, holdCmd)
	}); err != nil {
		r.log.Error("failed to hold collateral", "saga_id", saga.SagaID, "error", err)
		return r.emitActionFailed(ctx, saga.SagaID, "hold_collateral", err.Error())
	}
	return r.recordCollateralHeld(ctx, saga.SagaID, collateral)
}

func (r *Reactor) estimateShortOpenProceeds(ctx context.Context, saga *OrderSaga) (int64, error) {
	if saga.OrderType != orderbook.Market {
		return saga.Price * saga.Quantity, nil
	}
	book, err := r.orderbookHandler.Load(ctx, orderbook.AggregateID(saga.Symbol))
	if err != nil {
		return 0, fmt.Errorf("load orderbook: %w", err)
	}
	proceeds, ok := book.EstimateMarketSellProceeds(saga.Quantity)
	if !ok {
		return 0, errors.New("no bid liquidity for market short open")
	}
	return proceeds, nil
}

func (r *Reactor) holdCapacityForCover(ctx context.Context, saga *OrderSaga) error {
	holdCmd := portfolio.HoldShortCover{
		AccountID:   saga.AccountID,
		OrderSagaID: saga.SagaID,
		Symbol:      saga.Symbol,
		Quantity:    saga.Quantity,
	}
	if err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShortCover(p, holdCmd)
	}); err != nil {
		r.log.Error("failed to hold short cover capacity", "saga_id", saga.SagaID, "error", err)
		return r.emitActionFailed(ctx, saga.SagaID, "hold_short_cover", err.Error())
	}
	return r.recordCashHeld(ctx, saga.SagaID, saga.Quantity)
}

// padForSlippage rounds up swept*1.05 to give market orders a buffer
// between hold time and execution time.
func padForSlippage(amount int64) int64 {
	return (amount*marketBuySlippageBps + slippageBpsScale - 1) / slippageBpsScale
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
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

func (r *Reactor) recordCollateralHeld(ctx context.Context, sagaID string, amount int64) error {
	cmd := RecordCollateralHeld{SagaID: sagaID, AmountHeld: amount}
	if err := r.sagaHandler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
		return ExecuteRecordCollateralHeld(saga, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record collateral held", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "hold_collateral", err.Error())
	}
	r.log.Info("order saga collateral held", "saga_id", sagaID, "amount", amount)
	return nil
}

// placeOrder runs the Place step once the saga has its hold. Order IDs
// are deterministic, so retries are no-ops at the orderbook layer.
func (r *Reactor) placeOrder(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if !isHeldStatus(saga.Status) {
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

	if err := r.applySettlement(ctx, saga, data, cashAmount); err != nil {
		r.log.Error("failed to settle on portfolio", "saga_id", sagaID, "trade_id", data.TradeId, "error", err)
		return r.emitActionFailed(ctx, sagaID, "record_fills", err.Error())
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
	if err := r.releaseUnusedHold(ctx, saga); err != nil {
		return err
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

	if cmd, ok := releaseCommandForFailure(p, saga); ok {
		if err := r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return executeRelease(p, cmd)
		}); err != nil {
			r.log.Error("failed to release hold on failure", "saga_id", saga.SagaID, "error", err)
			return r.emitActionFailed(ctx, saga.SagaID, "release_resources_on_failure", err.Error())
		}
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
	case CashHeld, CollateralHeld, SharesHeld:
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

// isHeldStatus reports whether the saga has completed any of the four
// kinds of pre-place hold and is ready to place its order.
func isHeldStatus(s Status) bool {
	switch s {
	case CashHeld, CollateralHeld, SharesHeld:
		return true
	}
	return false
}

// applySettlement dispatches a fill to the appropriate portfolio
// settlement command based on (Side, PositionSide).
func (r *Reactor) applySettlement(ctx context.Context, saga *OrderSaga, data *orderbookv1.TradeExecuted, cashAmount int64) error {
	isShort := saga.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT
	switch {
	case saga.Side == orderbook.Sell && !isShort:
		cmd := portfolio.SettleSale{
			AccountID: saga.AccountID, OrderSagaID: saga.SagaID, TradeID: data.TradeId,
			Symbol: saga.Symbol, Quantity: data.Quantity,
			PricePerShare: data.Price, Proceeds: cashAmount,
		}
		return r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteSettleSale(p, cmd)
		})
	case saga.Side == orderbook.Buy && !isShort:
		cmd := portfolio.SettleTrade{
			AccountID: saga.AccountID, OrderSagaID: saga.SagaID, TradeID: data.TradeId,
			Amount: cashAmount, Symbol: saga.Symbol, Quantity: data.Quantity,
			CostPerShare: data.Price,
		}
		return r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteSettleTrade(p, cmd)
		})
	case saga.Side == orderbook.Sell && isShort:
		cmd := portfolio.OpenShort{
			AccountID: saga.AccountID, OrderSagaID: saga.SagaID, TradeID: data.TradeId,
			Symbol: saga.Symbol, Quantity: data.Quantity, PricePerShare: data.Price,
		}
		return r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteOpenShort(p, cmd)
		})
	case saga.Side == orderbook.Buy && isShort:
		cmd := portfolio.CoverShort{
			AccountID: saga.AccountID, OrderSagaID: saga.SagaID, TradeID: data.TradeId,
			Symbol: saga.Symbol, Quantity: data.Quantity, CostPerShare: data.Price,
		}
		return r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteCoverShort(p, cmd)
		})
	}
	return nil
}

// releaseUnusedHold runs on saga completion to free whatever portion of
// the pre-fill hold wasn't consumed by fills.
func (r *Reactor) releaseUnusedHold(ctx context.Context, saga *OrderSaga) error {
	isShort := saga.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT
	switch {
	case saga.Side == orderbook.Sell && !isShort:
		remaining := saga.AmountHeld - saga.FilledQty
		if remaining > 0 {
			cmd := portfolio.ReleaseShares{
				AccountID: saga.AccountID, OrderSagaID: saga.SagaID,
				Symbol: saga.Symbol, Quantity: remaining,
			}
			return r.handleReleaseOnComplete(ctx, saga, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseShares(p, cmd)
			})
		}
	case saga.Side == orderbook.Buy && !isShort:
		remaining := saga.AmountHeld - saga.CashSettled
		if remaining > 0 {
			cmd := portfolio.ReleaseCash{
				AccountID: saga.AccountID, OrderSagaID: saga.SagaID, Amount: remaining,
			}
			return r.handleReleaseOnComplete(ctx, saga, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseCash(p, cmd)
			})
		}
	case saga.Side == orderbook.Sell && isShort:
		// Collateral was sized off the *estimated* sale proceeds; if the
		// fill was partial the leftover collateral stays on the saga's
		// CollateralHeldBySaga entry. ExecuteOpenShort consumes only what
		// matches the actual fill, leaving the remainder for release here.
		// (For a full fill there's nothing to release — applyShortOpened
		// already deleted the bucket.)
		p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(saga.AccountID))
		if err != nil {
			return r.emitActionFailed(ctx, saga.SagaID, "complete", err.Error())
		}
		if _, ok := p.CollateralHeldBySaga[saga.SagaID]; ok {
			cmd := portfolio.ReleaseCollateral{
				AccountID: saga.AccountID, OrderSagaID: saga.SagaID,
			}
			return r.handleReleaseOnComplete(ctx, saga, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseCollateral(p, cmd)
			})
		}
	case saga.Side == orderbook.Buy && isShort:
		remaining := saga.AmountHeld - saga.FilledQty
		if remaining > 0 {
			cmd := portfolio.ReleaseShortCover{
				AccountID: saga.AccountID, OrderSagaID: saga.SagaID,
				Symbol: saga.Symbol, Quantity: remaining,
			}
			return r.handleReleaseOnComplete(ctx, saga, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
				return portfolio.ExecuteReleaseShortCover(p, cmd)
			})
		}
	}
	return nil
}

func (r *Reactor) handleReleaseOnComplete(ctx context.Context, saga *OrderSaga, cmd es.Command, exec func(*portfolio.Portfolio) ([]es.Event, error)) error {
	if err := r.portfolioHandler.Handle(ctx, cmd, exec); err != nil {
		r.log.Error("failed to release unused hold", "saga_id", saga.SagaID, "error", err)
		return r.emitActionFailed(ctx, saga.SagaID, "complete", err.Error())
	}
	return nil
}

// releaseCommandForFailure picks the release command matching whatever
// hold the saga has open on the portfolio. Returns ok=false if there's
// nothing to release.
func releaseCommandForFailure(p *portfolio.Portfolio, saga *OrderSaga) (es.Command, bool) {
	if _, ok := p.CollateralHeldBySaga[saga.SagaID]; ok {
		return portfolio.ReleaseCollateral{
			AccountID: saga.AccountID, OrderSagaID: saga.SagaID,
		}, true
	}
	if hold, ok := p.ShareHoldsBySaga[saga.SagaID]; ok && hold.Quantity > 0 {
		return portfolio.ReleaseShares{
			AccountID: saga.AccountID, OrderSagaID: saga.SagaID,
			Symbol: hold.Symbol, Quantity: hold.Quantity,
		}, true
	}
	if hold, ok := p.ShortCoverHoldsBySaga[saga.SagaID]; ok && hold.Quantity > 0 {
		return portfolio.ReleaseShortCover{
			AccountID: saga.AccountID, OrderSagaID: saga.SagaID,
			Symbol: hold.Symbol, Quantity: hold.Quantity,
		}, true
	}
	if amount, ok := p.HoldsBySaga[saga.SagaID]; ok && amount > 0 {
		return portfolio.ReleaseCash{
			AccountID: saga.AccountID, OrderSagaID: saga.SagaID, Amount: amount,
		}, true
	}
	return nil, false
}

// executeRelease runs the right portfolio Execute* for the release
// command returned by releaseCommandForFailure.
func executeRelease(p *portfolio.Portfolio, cmd es.Command) ([]es.Event, error) {
	switch c := cmd.(type) {
	case portfolio.ReleaseCollateral:
		return portfolio.ExecuteReleaseCollateral(p, c)
	case portfolio.ReleaseShares:
		return portfolio.ExecuteReleaseShares(p, c)
	case portfolio.ReleaseShortCover:
		return portfolio.ExecuteReleaseShortCover(p, c)
	case portfolio.ReleaseCash:
		return portfolio.ExecuteReleaseCash(p, c)
	}
	return nil, fmt.Errorf("unhandled release command type: %T", cmd)
}
