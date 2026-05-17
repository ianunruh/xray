package ocosaga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

// Reactor drives an OCO saga's lifecycle by reacting to events from
// the saga, portfolio, and orderbook streams. Stateless — every
// decision is made by loading the relevant aggregates at event time.
// Commands are idempotent (per-saga holds/releases and per-trade
// settlements) so replays are safe.
type Reactor struct {
	sagaHandler      *es.Handler[*OCOSaga]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger
}

func NewReactor(
	sagaHandler *es.Handler[*OCOSaga],
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
	case *orderbookv1.OCOSagaStarted:
		return r.holdShares(ctx, data.SagaId)
	case *orderbookv1.OCOSagaSharesHeld:
		return r.placeExits(ctx, data.SagaId)
	case *orderbookv1.OCOSagaFailed:
		return r.releaseRemainingHolds(ctx, data.SagaId)
	case *orderbookv1.OCOSagaActionFailed:
		return r.retry(ctx, data.SagaId)
	case *orderbookv1.TradeExecuted:
		return r.onTrade(ctx, data)
	}
	return nil
}

func (r *Reactor) holdShares(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != Started {
		return nil
	}
	if saga.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT {
		cmd := portfolio.HoldShortCover{
			AccountID:   saga.AccountID,
			OrderSagaID: sagaID,
			Symbol:      saga.Symbol,
			Quantity:    saga.Quantity,
		}
		if err := r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteHoldShortCover(p, cmd)
		}); err != nil {
			r.log.Error("ocosaga: failed to hold short cover", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_short_cover", err.Error())
		}
	} else {
		cmd := portfolio.HoldShares{
			AccountID:   saga.AccountID,
			OrderSagaID: sagaID,
			Symbol:      saga.Symbol,
			Quantity:    saga.Quantity,
		}
		if err := r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteHoldShares(p, cmd)
		}); err != nil {
			r.log.Error("ocosaga: failed to hold shares", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "hold_shares", err.Error())
		}
	}
	recordCmd := RecordSharesHeld{SagaID: sagaID}
	if err := r.sagaHandler.Handle(ctx, recordCmd, func(s *OCOSaga) ([]es.Event, error) {
		return ExecuteRecordSharesHeld(s, recordCmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("ocosaga: failed to record shares held", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "hold_shares", err.Error())
	}
	r.log.Info("ocosaga: shares held", "saga_id", sagaID, "quantity", saga.Quantity)
	return nil
}

func (r *Reactor) placeExits(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != SharesHeld {
		return nil
	}

	ocoGroup := OCOGroupID(sagaID)
	tpOrderID := TakeProfitOrderID(sagaID)
	if err := r.placeExitOrder(ctx, saga.Symbol, saga.ExitSide, saga.TakeProfitPrice, saga.Quantity, orderbook.Limit, 0, tpOrderID, ocoGroup); err != nil {
		r.log.Error("ocosaga: failed to place take-profit", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_exits", err.Error())
	}
	slOrderID := StopLossOrderID(sagaID)
	if err := r.placeExitOrder(ctx, saga.Symbol, saga.ExitSide, 0, saga.Quantity, orderbook.StopMarket, saga.StopLossPrice, slOrderID, ocoGroup); err != nil {
		r.log.Error("ocosaga: failed to place stop-loss", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_exits", err.Error())
	}

	cmd := RecordExitPlaced{
		SagaID:            sagaID,
		TakeProfitOrderID: tpOrderID,
		StopLossOrderID:   slOrderID,
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *OCOSaga) ([]es.Event, error) {
		return ExecuteRecordExitPlaced(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("ocosaga: failed to record exit placed", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "place_exits", err.Error())
	}
	r.log.Info("ocosaga: exits placed",
		"saga_id", sagaID, "tp_order_id", tpOrderID, "sl_order_id", slOrderID)
	return nil
}

func (r *Reactor) onTrade(ctx context.Context, data *orderbookv1.TradeExecuted) error {
	var firstErr error
	for _, orderID := range []string{data.BuyOrderId, data.SellOrderId} {
		sagaID, ok := sagaIDFromOrderID(orderID)
		if !ok {
			continue
		}
		if err := r.settleFill(ctx, sagaID, orderID, data); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Reactor) settleFill(ctx context.Context, sagaID, orderID string, data *orderbookv1.TradeExecuted) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != ExitPlaced {
		return nil
	}
	if orderID != saga.TakeProfitOrderID && orderID != saga.StopLossOrderID {
		return nil
	}

	if saga.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT {
		coverCmd := portfolio.CoverShort{
			AccountID:    saga.AccountID,
			OrderSagaID:  sagaID,
			TradeID:      data.TradeId,
			Symbol:       saga.Symbol,
			Quantity:     data.Quantity,
			CostPerShare: data.Price,
		}
		if err := r.portfolioHandler.Handle(ctx, coverCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteCoverShort(p, coverCmd)
		}); err != nil {
			r.log.Error("ocosaga: failed to cover short", "saga_id", sagaID, "trade_id", data.TradeId, "error", err)
			return r.emitActionFailed(ctx, sagaID, "settle_fill", err.Error())
		}
	} else {
		settleCmd := portfolio.SettleSale{
			AccountID:     saga.AccountID,
			OrderSagaID:   sagaID,
			TradeID:       data.TradeId,
			Symbol:        saga.Symbol,
			Quantity:      data.Quantity,
			PricePerShare: data.Price,
			Proceeds:      data.Price * data.Quantity,
		}
		if err := r.portfolioHandler.Handle(ctx, settleCmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteSettleSale(p, settleCmd)
		}); err != nil {
			r.log.Error("ocosaga: failed to settle fill", "saga_id", sagaID, "trade_id", data.TradeId, "error", err)
			return r.emitActionFailed(ctx, sagaID, "settle_fill", err.Error())
		}
	}

	fillCmd := RecordFill{
		SagaID:       sagaID,
		TradeID:      data.TradeId,
		OrderID:      orderID,
		FillQuantity: data.Quantity,
		FillPrice:    data.Price,
	}
	if err := r.sagaHandler.Handle(ctx, fillCmd, func(s *OCOSaga) ([]es.Event, error) {
		return ExecuteRecordFill(s, fillCmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("ocosaga: failed to record fill", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "settle_fill", err.Error())
	}

	// Check completion after the fill is recorded.
	updated, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("reload saga: %w", err)
	}
	if updated.Status == ExitPlaced && updated.SettledQty >= updated.Quantity {
		return r.complete(ctx, updated)
	}
	return nil
}

func (r *Reactor) complete(ctx context.Context, saga *OCOSaga) error {
	cmd := RecordCompleted{SagaID: saga.SagaID}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *OCOSaga) ([]es.Event, error) {
		return ExecuteRecordCompleted(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("ocosaga: failed to record completed", "saga_id", saga.SagaID, "error", err)
		return r.emitActionFailed(ctx, saga.SagaID, "complete", err.Error())
	}
	r.log.Info("ocosaga: completed", "saga_id", saga.SagaID)
	return nil
}

// releaseRemainingHolds runs in response to OCOSagaFailed: if shares
// are still held for this saga, release the unsettled remainder.
func (r *Reactor) releaseRemainingHolds(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	if saga.Status != Failed {
		return nil
	}
	p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(saga.AccountID))
	if err != nil {
		return fmt.Errorf("load portfolio: %w", err)
	}
	if saga.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT {
		hold, ok := p.ShortCoverHoldsBySaga[sagaID]
		if !ok || hold.Quantity <= 0 {
			return nil
		}
		cmd := portfolio.ReleaseShortCover{
			AccountID:   saga.AccountID,
			OrderSagaID: sagaID,
			Symbol:      hold.Symbol,
			Quantity:    hold.Quantity,
		}
		if err := r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteReleaseShortCover(p, cmd)
		}); err != nil {
			r.log.Error("ocosaga: failed to release short cover", "saga_id", sagaID, "error", err)
			return r.emitActionFailed(ctx, sagaID, "release_short_cover", err.Error())
		}
		return nil
	}
	hold, ok := p.ShareHoldsBySaga[sagaID]
	if !ok || hold.Quantity <= 0 {
		return nil
	}
	cmd := portfolio.ReleaseShares{
		AccountID:   saga.AccountID,
		OrderSagaID: sagaID,
		Symbol:      hold.Symbol,
		Quantity:    hold.Quantity,
	}
	if err := r.portfolioHandler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteReleaseShares(p, cmd)
	}); err != nil {
		r.log.Error("ocosaga: failed to release remaining shares", "saga_id", sagaID, "error", err)
		return r.emitActionFailed(ctx, sagaID, "release_shares", err.Error())
	}
	r.log.Info("ocosaga: released remaining shares", "saga_id", sagaID, "released", hold.Quantity)
	return nil
}

// retry is the universal "previous action failed, re-derive what to do"
// handler — invoked on OCOSagaActionFailed events.
func (r *Reactor) retry(ctx context.Context, sagaID string) error {
	saga, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load saga: %w", err)
	}
	switch saga.Status {
	case Started:
		return r.holdShares(ctx, sagaID)
	case SharesHeld:
		return r.placeExits(ctx, sagaID)
	case ExitPlaced:
		if saga.SettledQty >= saga.Quantity {
			return r.complete(ctx, saga)
		}
	case Failed:
		return r.releaseRemainingHolds(ctx, sagaID)
	}
	return nil
}

func (r *Reactor) emitActionFailed(ctx context.Context, sagaID, action, reason string) error {
	cmd := RecordActionFailed{
		SagaID: sagaID,
		Action: action,
		Reason: reason,
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *OCOSaga) ([]es.Event, error) {
		return ExecuteRecordActionFailed(s, cmd)
	}); err != nil {
		r.log.Error("ocosaga: failed to emit action failed event",
			"saga_id", sagaID, "action", action, "error", err)
		return fmt.Errorf("saga %s: failed to emit action failed for %s: %w", sagaID, action, err)
	}
	return nil
}

func (r *Reactor) placeExitOrder(
	ctx context.Context,
	symbol string,
	side orderbook.Side,
	price, qty int64,
	orderType orderbook.OrderType,
	stopPrice int64,
	orderID, ocoGroupID string,
) error {
	cmd := orderbook.PlaceOrder{
		Symbol:      symbol,
		Side:        side,
		Price:       price,
		StopPrice:   stopPrice,
		Quantity:    qty,
		OrderType:   orderType,
		TimeInForce: orderbook.GTC,
		OrderID:     orderID,
		OCOGroupID:  ocoGroupID,
	}
	return r.orderbookHandler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, cmd)
	})
}

// Reconcile drives the OCO saga forward from current durable state.
// Used by the periodic reconciler.
func (r *Reactor) Reconcile(ctx context.Context, sagaID string) error {
	return r.retry(ctx, sagaID)
}

// ReplayTrade re-runs the fill handler for a previously-observed trade,
// used by the reconciler when a settle was lost.
func (r *Reactor) ReplayTrade(ctx context.Context, data *orderbookv1.TradeExecuted) error {
	return r.onTrade(ctx, data)
}
