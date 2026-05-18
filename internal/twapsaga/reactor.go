package twapsaga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/pkg/es"
)

// Reactor drives TWAP sagas. It observes child OrderSaga lifecycle
// events to record per-slice fills, then evaluates whether to launch
// the next slice or complete the parent. The reconciler periodically
// calls LaunchDueSlices() as a backstop in case a wakeup is missed
// (e.g., process restart during a slice's interval gap).
//
// Holds no in-memory state: every decision is made by loading the
// parent TWAP and any relevant child OrderSaga. Replays are safe — all
// commands are status-guarded and idempotent on slice index.
type Reactor struct {
	sagaHandler      *es.Handler[*TWAPSaga]
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	now              func() time.Time
	log              *slog.Logger
}

func NewReactor(
	sagaHandler *es.Handler[*TWAPSaga],
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga],
	log *slog.Logger,
) *Reactor {
	return &Reactor{
		sagaHandler:      sagaHandler,
		orderSagaHandler: orderSagaHandler,
		now:              time.Now,
		log:              log,
	}
}

// WithClock lets tests inject a deterministic clock for "is the next
// slice due yet?" decisions.
func (r *Reactor) WithClock(now func() time.Time) *Reactor {
	r.now = now
	return r
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
	case *sagav1.TWAPSagaStarted:
		// On freshly-created TWAP, try to launch slice 0 immediately if
		// due (which it always is — start = now).
		return r.advance(ctx, data.SagaId)
	case *sagav1.TWAPSagaActionFailed:
		return r.advance(ctx, data.SagaId)
	case *portfoliov1.OrderSagaCompleted:
		return r.onChildOrderSagaCompleted(ctx, data.SagaId)
	case *portfoliov1.OrderSagaFailed:
		return r.onChildOrderSagaFailed(ctx, data.SagaId, data.Reason)
	}
	return nil
}

// onChildOrderSagaCompleted records the slice fill, then drives the
// parent forward (launches next slice if due, or marks complete).
func (r *Reactor) onChildOrderSagaCompleted(ctx context.Context, childSagaID string) error {
	twapID, sliceIndex, ok := twapIDAndSliceFromChildSagaID(childSagaID)
	if !ok {
		return nil
	}
	if err := r.recordSliceTerminal(ctx, twapID, sliceIndex, childSagaID); err != nil {
		return err
	}
	return r.advance(ctx, twapID)
}

// onChildOrderSagaFailed treats the child failure as slice completion
// with whatever was filled before the failure. The TWAP continues
// scheduling — one slice failing (e.g., IOC didn't fill at the limit)
// doesn't abort the parent.
func (r *Reactor) onChildOrderSagaFailed(ctx context.Context, childSagaID, reason string) error {
	twapID, sliceIndex, ok := twapIDAndSliceFromChildSagaID(childSagaID)
	if !ok {
		return nil
	}
	r.log.Info("twap: child slice failed", "twap_id", twapID, "slice", sliceIndex, "reason", reason)
	if err := r.recordSliceTerminal(ctx, twapID, sliceIndex, childSagaID); err != nil {
		return err
	}
	return r.advance(ctx, twapID)
}

// recordSliceTerminal loads the child's final filled qty / cash settled
// and emits TWAPSliceCompleted. Idempotent on slice index.
func (r *Reactor) recordSliceTerminal(ctx context.Context, twapID string, sliceIndex int32, childSagaID string) error {
	twap, err := r.sagaHandler.Load(ctx, AggregateID(twapID))
	if err != nil {
		return fmt.Errorf("load twap: %w", err)
	}
	if twap.Version() == 0 {
		// Not actually a TWAP — child ID collision is extremely unlikely
		// given the prefix, but be defensive.
		return nil
	}
	// Idempotency check: this slice may already be recorded.
	for i := range twap.Slices {
		if twap.Slices[i].Index == sliceIndex && twap.Slices[i].Completed {
			return nil
		}
	}

	child, err := r.orderSagaHandler.Load(ctx, ordersaga.AggregateID(childSagaID))
	if err != nil {
		return fmt.Errorf("load child ordersaga: %w", err)
	}
	cmd := RecordSliceCompleted{
		SagaID:         twapID,
		SliceIndex:     sliceIndex,
		ChildSagaID:    childSagaID,
		FilledQuantity: child.FilledQty,
		CashSettled:    child.CashSettled,
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *TWAPSaga) ([]es.Event, error) {
		return ExecuteRecordSliceCompleted(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		return r.emitActionFailed(ctx, twapID, "record_slice_completed", err.Error())
	}
	r.log.Info("twap: slice recorded",
		"twap_id", twapID, "slice", sliceIndex,
		"filled", child.FilledQty, "settled", child.CashSettled)
	return nil
}

// advance drives a TWAP forward from its current state: launch the next
// due slice, or mark the saga complete if all slices are done.
func (r *Reactor) advance(ctx context.Context, twapID string) error {
	twap, err := r.sagaHandler.Load(ctx, AggregateID(twapID))
	if err != nil {
		return fmt.Errorf("load twap: %w", err)
	}
	if twap.Status != Active {
		return nil
	}

	// All slices launched? Maybe we can finalize.
	if twap.SlicesLaunched() >= twap.SliceCount {
		if twap.AllSlicesTerminal() {
			cmd := RecordSagaCompleted{SagaID: twapID}
			if err := r.sagaHandler.Handle(ctx, cmd, func(s *TWAPSaga) ([]es.Event, error) {
				return ExecuteRecordSagaCompleted(s, cmd)
			}); err != nil {
				if errors.Is(err, ErrInvalidState) {
					return nil
				}
				return r.emitActionFailed(ctx, twapID, "record_saga_completed", err.Error())
			}
			r.log.Info("twap: completed",
				"twap_id", twapID, "filled", twap.TotalFilled, "settled", twap.TotalSettled)
		}
		return nil
	}

	// Wait until the current (last-launched) slice is terminal before
	// launching the next. With IOC slices this is usually a single
	// reactor cycle, but the guard makes restart-recovery safe.
	if cur := twap.CurrentSlice(); cur != nil && !cur.Completed {
		return nil
	}

	// Is the next slice due yet?
	nextIndex := twap.SlicesLaunched()
	dueAt := twap.StartedAt.Add(twap.SliceInterval() * time.Duration(nextIndex))
	if r.now().Before(dueAt) {
		return nil
	}

	return r.launchSlice(ctx, twap, nextIndex)
}

func (r *Reactor) launchSlice(ctx context.Context, twap *TWAPSaga, sliceIndex int32) error {
	qty := twap.NextSliceQuantity()
	if qty <= 0 {
		// Already filled the cumulative target. Mark the remaining
		// slices as launched-and-immediately-completed so the saga can
		// roll to Completed. Cheap loop — slice counts are small (~10s).
		for i := twap.SlicesLaunched(); i < twap.SliceCount; i++ {
			childID := ChildSagaID(twap.SagaID, i)
			launchCmd := RecordSliceLaunched{
				SagaID:        twap.SagaID,
				SliceIndex:    i,
				ChildSagaID:   childID,
				SliceQuantity: 0,
			}
			if err := r.sagaHandler.Handle(ctx, launchCmd, func(s *TWAPSaga) ([]es.Event, error) {
				return ExecuteRecordSliceLaunched(s, launchCmd)
			}); err != nil {
				if !errors.Is(err, ErrInvalidState) {
					return r.emitActionFailed(ctx, twap.SagaID, "record_zero_slice_launched", err.Error())
				}
			}
			completeCmd := RecordSliceCompleted{
				SagaID:      twap.SagaID,
				SliceIndex:  i,
				ChildSagaID: childID,
			}
			if err := r.sagaHandler.Handle(ctx, completeCmd, func(s *TWAPSaga) ([]es.Event, error) {
				return ExecuteRecordSliceCompleted(s, completeCmd)
			}); err != nil {
				if !errors.Is(err, ErrInvalidState) {
					return r.emitActionFailed(ctx, twap.SagaID, "record_zero_slice_completed", err.Error())
				}
			}
		}
		return r.advance(ctx, twap.SagaID)
	}

	childID := ChildSagaID(twap.SagaID, sliceIndex)
	startCmd := ordersaga.StartOrderSaga{
		SagaID:       childID,
		AccountID:    twap.AccountID,
		Symbol:       twap.Symbol,
		Side:         orderbook.SideToProto(twap.Side),
		Price:        twap.LimitPrice,
		Quantity:     qty,
		OrderType:    orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce:  orderbookv1.TimeInForce_TIME_IN_FORCE_IOC,
		PositionSide: twap.PositionSide,
		Initiator:    twap.Initiator,
	}
	if err := r.orderSagaHandler.Handle(ctx, startCmd, func(s *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(s, startCmd)
	}); err != nil {
		if !errors.Is(err, ordersaga.ErrInvalidState) {
			r.log.Error("twap: failed to spawn child ordersaga",
				"twap_id", twap.SagaID, "slice", sliceIndex, "error", err)
			return r.emitActionFailed(ctx, twap.SagaID, "spawn_child_ordersaga", err.Error())
		}
		// Child already exists from a prior attempt — fall through to
		// record the launch on the parent (idempotent).
	}

	launchCmd := RecordSliceLaunched{
		SagaID:        twap.SagaID,
		SliceIndex:    sliceIndex,
		ChildSagaID:   childID,
		SliceQuantity: qty,
	}
	if err := r.sagaHandler.Handle(ctx, launchCmd, func(s *TWAPSaga) ([]es.Event, error) {
		return ExecuteRecordSliceLaunched(s, launchCmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		return r.emitActionFailed(ctx, twap.SagaID, "record_slice_launched", err.Error())
	}
	r.log.Info("twap: slice launched",
		"twap_id", twap.SagaID, "slice", sliceIndex, "qty", qty, "child", childID)
	return nil
}

// Reconcile is called by the periodic reconciler. Equivalent to a
// re-derivation pass: launch any due slices, or finalize if all done.
func (r *Reactor) Reconcile(ctx context.Context, sagaID string) error {
	return r.advance(ctx, sagaID)
}

// MarkFailed is called by the saga service to cancel an in-flight TWAP.
// Sets Status to Failed (which suppresses future slice launches);
// the caller is responsible for cancelling any in-flight child order.
func (r *Reactor) MarkFailed(ctx context.Context, sagaID, reason string) error {
	cmd := RecordSagaFailed{SagaID: sagaID, Reason: reason}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *TWAPSaga) ([]es.Event, error) {
		return ExecuteRecordSagaFailed(s, cmd)
	}); err != nil {
		if errors.Is(err, ErrInvalidState) {
			return nil
		}
		return fmt.Errorf("record twap failed: %w", err)
	}
	return nil
}

func (r *Reactor) emitActionFailed(ctx context.Context, sagaID, action, reason string) error {
	cmd := RecordActionFailed{
		SagaID: sagaID,
		Action: action,
		Reason: reason,
	}
	if err := r.sagaHandler.Handle(ctx, cmd, func(s *TWAPSaga) ([]es.Event, error) {
		return ExecuteRecordActionFailed(s, cmd)
	}); err != nil {
		r.log.Error("twap: failed to emit action failed",
			"saga_id", sagaID, "action", action, "error", err)
		return fmt.Errorf("saga %s: failed to emit action failed for %s: %w", sagaID, action, err)
	}
	return nil
}
