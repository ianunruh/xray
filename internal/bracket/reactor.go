package bracket

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/ocosaga"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/pkg/es"
)

// Reactor watches the saga, portfolio, and orderbook event streams and
// drives bracket orders through their lifecycle by issuing idempotent
// commands. It holds no in-memory state — every decision is made by
// loading the relevant aggregates at event time. Replays are safe
// because all commands are either status-guarded or per-key idempotent.
//
// The reactor is a thin coordinator: it spawns an entry ordersaga,
// observes its completion, then spawns an exit OCOSaga and observes
// that. All portfolio interaction (cash holds, share holds, settle,
// release) lives inside the entry ordersaga and exit OCOSaga
// respectively.
type Reactor struct {
	sagaHandler      *es.Handler[*BracketSaga]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	ocoSagaHandler   *es.Handler[*ocosaga.OCOSaga]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger
}

func NewReactor(
	sagaHandler *es.Handler[*BracketSaga],
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga],
	ocoSagaHandler *es.Handler[*ocosaga.OCOSaga],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	log *slog.Logger,
) *Reactor {
	return &Reactor{
		sagaHandler:      sagaHandler,
		orderSagaHandler: orderSagaHandler,
		ocoSagaHandler:   ocoSagaHandler,
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
	case *orderbookv1.SagaActionFailed:
		return r.onBracketActionFailed(ctx, data.SagaId)
	case *portfoliov1.OrderSagaCompleted:
		return r.onEntryOrderSagaCompleted(ctx, data.SagaId)
	case *portfoliov1.OrderSagaFailed:
		return r.onEntryOrderSagaFailed(ctx, data.SagaId, data.Reason)
	case *orderbookv1.OCOSagaCompleted:
		return r.onExitOCOCompleted(ctx, data.SagaId)
	case *orderbookv1.OCOSagaFailed:
		return r.onExitOCOFailed(ctx, data.SagaId, data.Reason)
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
// PendingExit: spawns the exit OCO saga (which owns the share hold and
// TP/SL placement), then records the entry-filled event.
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
	exitSide := orderbookv1.Side_SIDE_SELL
	if b.EntrySide == orderbook.Sell {
		exitSide = orderbookv1.Side_SIDE_BUY
	}

	// Spawn the exit OCO saga. It owns HoldShares, TP+SL placement,
	// fill settlement, and release-on-failure. Idempotent: existing
	// OCO saga with the same ID is treated as success.
	ocoID := ExitOCOSagaID(b.SagaID)
	start := ocosaga.StartOCOSaga{
		SagaID:          ocoID,
		AccountID:       b.AccountID,
		Symbol:          b.Symbol,
		ExitSide:        exitSide,
		Quantity:        b.EntryQty,
		TakeProfitPrice: b.TakeProfitPrice,
		StopLossPrice:   b.StopLossPrice,
	}
	if err := r.ocoSagaHandler.Handle(ctx, start, func(s *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteStartOCOSaga(s, start)
	}); err != nil {
		if !errors.Is(err, ocosaga.ErrInvalidState) {
			r.log.Error("bracket: failed to spawn exit OCO saga", "saga_id", b.SagaID, "error", err)
			return r.emitActionFailed(ctx, b.SagaID, "spawn_exit_oco", err.Error())
		}
	}

	cmd := RecordEntryFilled{
		SagaID:            b.SagaID,
		TakeProfitOrderID: ocosaga.TakeProfitOrderID(ocoID),
		StopLossOrderID:   ocosaga.StopLossOrderID(ocoID),
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

	r.log.Info("bracket: entry filled, exit OCO spawned",
		"saga_id", b.SagaID, "oco_saga_id", ocoID)
	return nil
}

// onEntryOrderSagaFailed records the bracket as failed when the entry
// ordersaga can't make progress (insufficient funds, cancelled, etc.).
func (r *Reactor) onEntryOrderSagaFailed(ctx context.Context, orderSagaID, reason string) error {
	bracketID, ok := bracketIDFromEntryOrderSagaID(orderSagaID)
	if !ok {
		return nil
	}
	return r.failBracket(ctx, bracketID, PendingEntry, reason)
}

// onExitOCOCompleted records the bracket as completed when its child
// OCO saga finishes settling.
func (r *Reactor) onExitOCOCompleted(ctx context.Context, ocoSagaID string) error {
	bracketID, ok := bracketIDFromExitOCOSagaID(ocoSagaID)
	if !ok {
		return nil
	}
	b, err := r.sagaHandler.Load(ctx, AggregateID(bracketID))
	if err != nil {
		return fmt.Errorf("load bracket: %w", err)
	}
	if b.Status != PendingExit {
		return nil
	}
	cmd := RecordExitFilled{
		SagaID:           bracketID,
		FilledOrderID:    ocosaga.TakeProfitOrderID(ocoSagaID),
		CancelledOrderID: ocosaga.StopLossOrderID(ocoSagaID),
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordExitFilled(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("bracket: failed to record exit filled", "saga_id", bracketID, "error", err)
		return r.emitActionFailed(ctx, bracketID, "record_exit_filled", err.Error())
	}
	r.log.Info("bracket: exit OCO completed", "saga_id", bracketID, "oco_saga_id", ocoSagaID)
	return nil
}

// onExitOCOFailed records the bracket as failed when its child OCO
// saga fails. The OCOSaga reactor has already released any unsettled
// share hold; the bracket has no portfolio cleanup of its own.
func (r *Reactor) onExitOCOFailed(ctx context.Context, ocoSagaID, reason string) error {
	bracketID, ok := bracketIDFromExitOCOSagaID(ocoSagaID)
	if !ok {
		return nil
	}
	return r.failBracket(ctx, bracketID, PendingExit, reason)
}

func (r *Reactor) failBracket(ctx context.Context, bracketID string, expected Status, reason string) error {
	b, err := r.sagaHandler.Load(ctx, AggregateID(bracketID))
	if err != nil {
		return fmt.Errorf("load bracket: %w", err)
	}
	if b.Status != expected {
		return nil
	}
	cmd := RecordSagaFailed{SagaID: bracketID, Reason: reason}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *BracketSaga) ([]es.Event, error) {
		return ExecuteRecordSagaFailed(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		r.log.Error("bracket: failed to record bracket failed", "saga_id", bracketID, "error", err)
		return r.emitActionFailed(ctx, bracketID, "record_saga_failed", err.Error())
	}
	r.log.Info("bracket: saga failed", "saga_id", bracketID, "reason", reason)
	return nil
}

// Reconcile drives a bracket's state machine forward from whatever its
// current durable state is. Exported for the periodic reconciler.
func (r *Reactor) Reconcile(ctx context.Context, sagaID string) error {
	return r.onBracketActionFailed(ctx, sagaID)
}

// onBracketActionFailed retries whichever phase is appropriate given
// the bracket's current aggregate state. SagaActionFailed is our trigger
// to re-derive what should happen next.
func (r *Reactor) onBracketActionFailed(ctx context.Context, sagaID string) error {
	b, err := r.sagaHandler.Load(ctx, AggregateID(sagaID))
	if err != nil {
		return fmt.Errorf("load bracket: %w", err)
	}
	switch b.Status {
	case PendingEntry:
		entry, err := r.orderSagaHandler.Load(ctx, ordersaga.AggregateID(EntryOrderSagaID(sagaID)))
		if err != nil {
			return fmt.Errorf("load entry ordersaga: %w", err)
		}
		if entry.Status == ordersaga.Completed {
			return r.prepareExit(ctx, b)
		}
		if entry.Version() == 0 {
			return r.onBracketStarted(ctx, &orderbookv1.SagaStarted{
				SagaId:        b.SagaID,
				AccountId:     b.AccountID,
				Symbol:        b.Symbol,
				EntrySide:     orderbook.SideToProto(b.EntrySide),
				EntryPrice:    b.EntryPrice,
				EntryQuantity: b.EntryQty,
			})
		}
	case PendingExit:
		// Check the child OCO saga's terminal state; advance bracket if needed.
		ocoID := ExitOCOSagaID(sagaID)
		oco, err := r.ocoSagaHandler.Load(ctx, ocosaga.AggregateID(ocoID))
		if err != nil {
			return fmt.Errorf("load exit oco saga: %w", err)
		}
		if oco.Version() == 0 {
			// OCO was never spawned (crashed between entry-complete and spawn).
			return r.prepareExit(ctx, b)
		}
		switch oco.Status {
		case ocosaga.Completed:
			return r.onExitOCOCompleted(ctx, ocoID)
		case ocosaga.Failed:
			return r.onExitOCOFailed(ctx, ocoID, "exit oco saga failed")
		}
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
