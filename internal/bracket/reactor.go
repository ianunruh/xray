package bracket

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

// Reactor watches the saga, portfolio, and orderbook event streams and
// drives bracket orders through their lifecycle by issuing idempotent
// commands. It holds no in-memory state — every decision is made by
// loading the relevant aggregates at event time. Replays are safe
// because all commands are either status-guarded or per-key idempotent.
type Reactor struct {
	sagaHandler      *es.Handler[*BracketSaga]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger
}

func NewReactor(
	sagaHandler *es.Handler[*BracketSaga],
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga],
	portfolioHandler *es.Handler[*portfolio.Portfolio],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	log *slog.Logger,
) *Reactor {
	return &Reactor{
		sagaHandler:      sagaHandler,
		orderSagaHandler: orderSagaHandler,
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
	switch data := evt.Data.(type) {
	case *orderbookv1.SagaStarted:
		return r.onBracketStarted(ctx, data)
	case *orderbookv1.SagaFailed:
		return r.onBracketFailed(ctx, data.SagaId)
	case *orderbookv1.SagaActionFailed:
		return r.onBracketActionFailed(ctx, data.SagaId)
	case *portfoliov1.OrderSagaCompleted:
		return r.onEntryOrderSagaCompleted(ctx, data.SagaId)
	case *portfoliov1.OrderSagaFailed:
		return r.onEntryOrderSagaFailed(ctx, data.SagaId, data.Reason)
	case *orderbookv1.TradeExecuted:
		return r.onExitTrade(ctx, data)
	}
	return nil
}

// onBracketStarted spawns the entry ordersaga for a freshly-created
// bracket. Idempotent: ordersaga.StartOrderSaga errors out if the
// ordersaga already exists, which we treat as success.
func (r *Reactor) onBracketStarted(ctx context.Context, data *orderbookv1.SagaStarted) error {
	cmd := ordersaga.StartOrderSaga{
		SagaID:      EntryOrderSagaID(data.SagaId),
		AccountID:   data.AccountId,
		Symbol:      data.Symbol,
		Side:        data.EntrySide,
		Price:       data.EntryPrice,
		Quantity:    data.EntryQuantity,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}
	if err := r.orderSagaHandler.Handle(ctx, cmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, cmd)
	}); err != nil {
		if errors.Is(err, ordersaga.ErrInvalidState) {
			return nil
		}
		r.log.Error("bracket: failed to spawn entry ordersaga", "saga_id", data.SagaId, "error", err)
		return r.emitActionFailed(ctx, data.SagaId, "spawn_entry_saga", err.Error())
	}
	r.log.Info("bracket: entry ordersaga spawned", "bracket_id", data.SagaId)
	return nil
}

// onEntryOrderSagaCompleted advances a bracket from PendingEntry to
// PendingExit: holds shares for the OCO, places TP and SL, and records
// the entry-filled event.
func (r *Reactor) onEntryOrderSagaCompleted(ctx context.Context, orderSagaID string) error {
	bracketID, ok := bracketIDFromEntryOrderSagaID(orderSagaID)
	if !ok {
		return nil
	}
	b, err := r.sagaHandler.Load(ctx, AggregateID(bracketID))
	if err != nil {
		return fmt.Errorf("load bracket: %w", err)
	}
	if b.Status != PendingEntry {
		return nil
	}
	return r.prepareExit(ctx, b)
}

func (r *Reactor) prepareExit(ctx context.Context, b *BracketSaga) error {
	// Hold shares first so the exit OCO can settle into either leg.
	// Idempotent per saga.
	holdCmd := portfolio.HoldShares{
		AccountID:   b.AccountID,
		OrderSagaID: b.SagaID,
		Symbol:      b.Symbol,
		Quantity:    b.EntryQty,
	}
	if err := r.portfolioHandler.Handle(ctx, holdCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteHoldShares(p, holdCmd)
	}); err != nil {
		r.log.Error("failed to hold exit shares", "saga_id", b.SagaID, "error", err)
		return r.emitActionFailed(ctx, b.SagaID, "prepare_exit", err.Error())
	}

	exitSide := orderbookv1.Side_SIDE_SELL
	if b.EntrySide == orderbook.Sell {
		exitSide = orderbookv1.Side_SIDE_BUY
	}

	tpOrderID := TakeProfitOrderID(b.SagaID)
	if err := r.placeExitOrder(ctx, b.Symbol, exitSide, b.TakeProfitPrice, b.EntryQty, orderbook.Limit, 0, tpOrderID); err != nil {
		r.log.Error("failed to place take-profit order", "saga_id", b.SagaID, "error", err)
		return r.emitActionFailed(ctx, b.SagaID, "prepare_exit", err.Error())
	}

	slOrderID := StopLossOrderID(b.SagaID)
	if err := r.placeExitOrder(ctx, b.Symbol, exitSide, 0, b.EntryQty, orderbook.StopMarket, b.StopLossPrice, slOrderID); err != nil {
		r.log.Error("failed to place stop-loss order", "saga_id", b.SagaID, "error", err)
		return r.emitActionFailed(ctx, b.SagaID, "prepare_exit", err.Error())
	}

	cmd := RecordEntryFilled{
		SagaID:            b.SagaID,
		TakeProfitOrderID: tpOrderID,
		StopLossOrderID:   slOrderID,
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordEntryFilled(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record entry filled", "saga_id", b.SagaID, "error", err)
		return r.emitActionFailed(ctx, b.SagaID, "prepare_exit", err.Error())
	}

	r.log.Info("bracket saga entry filled, exit orders placed",
		"saga_id", b.SagaID,
		"tp_order_id", tpOrderID,
		"sl_order_id", slOrderID)
	return nil
}

// onEntryOrderSagaFailed records the bracket as failed when the entry
// ordersaga can't make progress (insufficient funds, cancelled, etc.).
func (r *Reactor) onEntryOrderSagaFailed(ctx context.Context, orderSagaID, reason string) error {
	bracketID, ok := bracketIDFromEntryOrderSagaID(orderSagaID)
	if !ok {
		return nil
	}
	b, err := r.sagaHandler.Load(ctx, AggregateID(bracketID))
	if err != nil {
		return fmt.Errorf("load bracket: %w", err)
	}
	if b.Status != PendingEntry {
		return nil
	}
	cmd := RecordSagaFailed{
		SagaID: bracketID,
		Reason: reason,
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordSagaFailed(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record bracket failed", "saga_id", bracketID, "error", err)
		return r.emitActionFailed(ctx, bracketID, "record_saga_failed", err.Error())
	}
	r.log.Info("bracket saga failed — entry ordersaga did not complete",
		"saga_id", bracketID, "reason", reason)
	return nil
}

// onExitTrade settles a TP or SL fill against the portfolio, attempts
// to cancel the sibling leg (no-op if already gone), and records the
// bracket as completed once the share hold is fully drained.
func (r *Reactor) onExitTrade(ctx context.Context, data *orderbookv1.TradeExecuted) error {
	var firstErr error
	for _, orderID := range []string{data.BuyOrderId, data.SellOrderId} {
		sagaID, ok := sagaIDFromExitOrderID(orderID)
		if !ok {
			continue
		}
		if err := r.settleExitFill(ctx, sagaID, orderID, data); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Reactor) settleExitFill(ctx context.Context, sagaID, orderID string, data *orderbookv1.TradeExecuted) error {
	b, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load bracket: %w", err)
	}
	if b.Status != PendingExit {
		return nil
	}
	if orderID != b.TakeProfitOrderID && orderID != b.StopLossOrderID {
		return nil
	}

	settleCmd := portfolio.SettleSale{
		AccountID:     b.AccountID,
		OrderSagaID:   sagaID,
		TradeID:       data.TradeId,
		Symbol:        b.Symbol,
		Quantity:      data.Quantity,
		PricePerShare: data.Price,
		Proceeds:      data.Price * data.Quantity,
	}
	if err := r.portfolioHandler.Handle(ctx, settleCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteSettleSale(p, settleCmd)
	}); err != nil {
		r.log.Error("failed to settle exit fill", "saga_id", sagaID, "trade_id", data.TradeId, "error", err)
		return r.emitActionFailed(ctx, sagaID, "settle_exit_fill", err.Error())
	}

	// Cancel the sibling. Idempotent at the orderbook (already-gone
	// errors are benign) so cancelling on every fill is fine.
	sibling := b.StopLossOrderID
	if orderID == b.StopLossOrderID {
		sibling = b.TakeProfitOrderID
	}
	if err := r.cancelOrder(ctx, b.Symbol, sibling); err != nil {
		if !errors.Is(err, orderbook.ErrOrderNotFound) && !errors.Is(err, orderbook.ErrNoRemainingQty) {
			r.log.Warn("failed to cancel OCO sibling", "saga_id", sagaID, "order_id", sibling, "error", err)
		}
	}

	// Reload portfolio post-settle to see whether the share hold is
	// fully drained — if so, the bracket is done.
	p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(b.AccountID))
	if err != nil {
		return fmt.Errorf("load portfolio: %w", err)
	}
	if hold, stillHeld := p.ShareHoldsBySaga[sagaID]; stillHeld && hold.Quantity > 0 {
		return nil
	}

	complete := RecordExitFilled{
		SagaID:           sagaID,
		FilledOrderID:    orderID,
		CancelledOrderID: sibling,
	}
	if err := r.sagaHandler.Handle(ctx, complete, func(s *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordExitFilled(s, complete)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("failed to record exit filled", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "record_exit_filled", err.Error())
	}
	r.log.Info("bracket saga completed", "saga_id", sagaID, "filled_order_id", orderID)
	return nil
}

// onBracketFailed releases any unsettled share hold left behind when a
// bracket fails during PendingExit (typically a user-initiated cancel).
func (r *Reactor) onBracketFailed(ctx context.Context, sagaID string) error {
	b, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load bracket: %w", err)
	}
	p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(b.AccountID))
	if err != nil {
		return fmt.Errorf("load portfolio: %w", err)
	}
	hold, ok := p.ShareHoldsBySaga[sagaID]
	if !ok || hold.Quantity <= 0 {
		return nil
	}
	cmd := portfolio.ReleaseShares{
		AccountID:   b.AccountID,
		OrderSagaID: sagaID,
		Symbol:      hold.Symbol,
		Quantity:    hold.Quantity,
	}
	if err := r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseShares(p, cmd)
	}); err != nil {
		r.log.Error("failed to release exit shares", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "release_exit_shares", err.Error())
	}
	r.log.Info("bracket saga: exit shares released after failure",
		"saga_id", sagaID, "released", hold.Quantity)
	return nil
}

// Reconcile drives a bracket's state machine forward from whatever its
// current durable state is. Exported for the periodic reconciler;
// equivalent to handling a SagaActionFailed event for this bracket.
func (r *Reactor) Reconcile(ctx context.Context, sagaID string) error {
	return r.onBracketActionFailed(ctx, sagaID)
}

// ReplayExitTrade re-runs the exit-fill handler for a previously-observed
// TradeExecuted. Used by the reconciler to settle TP/SL fills whose
// original settle command was lost. Per-trade portfolio dedup and
// ErrInvalidState guards on the bracket make replays safe.
func (r *Reactor) ReplayExitTrade(ctx context.Context, data *orderbookv1.TradeExecuted) error {
	return r.onExitTrade(ctx, data)
}

// onBracketActionFailed retries whichever phase is appropriate given the
// bracket's current aggregate state. SagaActionFailed is our trigger to
// re-derive what should happen next.
func (r *Reactor) onBracketActionFailed(ctx context.Context, sagaID string) error {
	b, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load bracket: %w", err)
	}
	switch b.Status {
	case PendingEntry:
		// Re-check the entry ordersaga; if it completed, re-attempt prepareExit.
		entry, err := r.orderSagaHandler.Load(ctx, ordersaga.AggregateID(EntryOrderSagaID(sagaID)))
		if err != nil {
			return fmt.Errorf("load entry ordersaga: %w", err)
		}
		if entry.Status == ordersaga.Completed {
			return r.prepareExit(ctx, b)
		}
		if entry.Version() == 0 {
			// Entry ordersaga was never spawned; retry the spawn.
			return r.onBracketStarted(ctx, &orderbookv1.SagaStarted{
				SagaId:        b.SagaID,
				AccountId:     b.AccountID,
				Symbol:        b.Symbol,
				EntrySide:     orderbook.SideToProto(b.EntrySide),
				EntryPrice:    b.EntryPrice,
				EntryQuantity: b.EntryQty,
			})
		}
	case Failed:
		// Re-check the release path for a failed PendingExit bracket.
		return r.onBracketFailed(ctx, sagaID)
	}
	return nil
}

func (r *Reactor) emitActionFailed(ctx context.Context, sagaID, action, reason string) error {
	cmd := RecordActionFailed{
		SagaID: sagaID,
		Action: action,
		Reason: reason,
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordActionFailed(saga, cmd)
	}); err != nil {
		r.log.Error("failed to emit action failed event", "saga_id", sagaID, "action", action, "error", err)
		return fmt.Errorf("saga %s: failed to emit action failed for %s: %w", sagaID, action, err)
	}
	return nil
}

func (r *Reactor) cancelOrder(ctx context.Context, symbol, orderID string) error {
	cmd := orderbook.CancelOrder{Symbol: symbol, OrderID: orderID}
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
