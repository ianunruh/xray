package twapsaga

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const MaxActionAttempts = 3

var (
	ErrInvalidPrice    = errors.New("price must be positive")
	ErrInvalidQuantity = errors.New("quantity must be positive")
	ErrInvalidState    = errors.New("saga is not in the expected state")
	ErrInvalidPlan     = errors.New("invalid TWAP plan")
)

type StartTWAPSaga struct {
	SagaID          string
	AccountID       string
	Symbol          string
	Side            orderbookv1.Side
	PositionSide    orderbookv1.PositionSide
	TotalQuantity   int64
	SliceCount      int32
	SliceIntervalMs int64
	LimitPrice      int64
	Initiator       sagav1.Initiator
}

func (c StartTWAPSaga) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteStartTWAPSaga(saga *TWAPSaga, cmd StartTWAPSaga) ([]es.Event, error) {
	if saga.Version() != 0 {
		return nil, ErrInvalidState
	}
	if cmd.TotalQuantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if cmd.SliceCount <= 0 {
		return nil, fmt.Errorf("%w: slice_count must be positive", ErrInvalidPlan)
	}
	if cmd.SliceIntervalMs < 0 {
		return nil, fmt.Errorf("%w: slice_interval_ms must be non-negative", ErrInvalidPlan)
	}
	if cmd.LimitPrice <= 0 {
		return nil, ErrInvalidPrice
	}
	if int64(cmd.SliceCount) > cmd.TotalQuantity {
		return nil, fmt.Errorf("%w: slice_count cannot exceed total_quantity", ErrInvalidPlan)
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventTWAPSagaStarted,
		Timestamp:   now,
		Data: &sagav1.TWAPSagaStarted{
			SagaId:          cmd.SagaID,
			AccountId:       cmd.AccountID,
			Symbol:          cmd.Symbol,
			Side:            cmd.Side,
			PositionSide:    cmd.PositionSide,
			TotalQuantity:   cmd.TotalQuantity,
			SliceCount:      cmd.SliceCount,
			SliceIntervalMs: cmd.SliceIntervalMs,
			LimitPrice:      cmd.LimitPrice,
			StartedAt:       timestamppb.New(now),
			Initiator:       cmd.Initiator,
		},
	}

	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordSliceLaunched struct {
	SagaID        string
	SliceIndex    int32
	ChildSagaID   string
	SliceQuantity int64
}

func (c RecordSliceLaunched) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordSliceLaunched(saga *TWAPSaga, cmd RecordSliceLaunched) ([]es.Event, error) {
	if saga.Status != Active {
		return nil, ErrInvalidState
	}
	if cmd.SliceIndex != saga.SlicesLaunched() {
		// Idempotency: a duplicate launch attempt for the same index is a
		// benign no-op for the caller.
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventTWAPSliceLaunched,
		Timestamp:   now,
		Data: &sagav1.TWAPSliceLaunched{
			SagaId:        cmd.SagaID,
			SliceIndex:    cmd.SliceIndex,
			ChildSagaId:   cmd.ChildSagaID,
			SliceQuantity: cmd.SliceQuantity,
			LaunchedAt:    timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordSliceCompleted struct {
	SagaID         string
	SliceIndex     int32
	ChildSagaID    string
	FilledQuantity int64
	CashSettled    int64
}

func (c RecordSliceCompleted) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordSliceCompleted(saga *TWAPSaga, cmd RecordSliceCompleted) ([]es.Event, error) {
	if saga.Status != Active {
		return nil, ErrInvalidState
	}
	// Idempotency: if the slice is already marked completed, skip.
	for i := range saga.Slices {
		if saga.Slices[i].Index == cmd.SliceIndex {
			if saga.Slices[i].Completed {
				return nil, ErrInvalidState
			}
			break
		}
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventTWAPSliceCompleted,
		Timestamp:   now,
		Data: &sagav1.TWAPSliceCompleted{
			SagaId:         cmd.SagaID,
			SliceIndex:     cmd.SliceIndex,
			ChildSagaId:    cmd.ChildSagaID,
			FilledQuantity: cmd.FilledQuantity,
			CashSettled:    cmd.CashSettled,
			CompletedAt:    timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordSagaCompleted struct {
	SagaID string
}

func (c RecordSagaCompleted) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordSagaCompleted(saga *TWAPSaga, cmd RecordSagaCompleted) ([]es.Event, error) {
	if saga.Status != Active {
		return nil, ErrInvalidState
	}
	if saga.SlicesLaunched() < saga.SliceCount {
		return nil, ErrInvalidState
	}
	if !saga.AllSlicesTerminal() {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventTWAPSagaCompleted,
		Timestamp:   now,
		Data: &sagav1.TWAPSagaCompleted{
			SagaId:      cmd.SagaID,
			CompletedAt: timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordSagaFailed struct {
	SagaID string
	Reason string
}

func (c RecordSagaFailed) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordSagaFailed(saga *TWAPSaga, cmd RecordSagaFailed) ([]es.Event, error) {
	if saga.Status != Active {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventTWAPSagaFailed,
		Timestamp:   now,
		Data: &sagav1.TWAPSagaFailed{
			SagaId:   cmd.SagaID,
			Reason:   cmd.Reason,
			FailedAt: timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordActionFailed struct {
	SagaID string
	Action string
	Reason string
}

func (c RecordActionFailed) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordActionFailed(saga *TWAPSaga, cmd RecordActionFailed) ([]es.Event, error) {
	if saga.Status != Active {
		return nil, ErrInvalidState
	}

	attempts := saga.ActionAttempts + 1
	now := time.Now()

	if attempts >= MaxActionAttempts {
		evt := es.Event{
			AggregateID: saga.AggregateID(),
			Type:        EventTWAPSagaFailed,
			Timestamp:   now,
			Data: &sagav1.TWAPSagaFailed{
				SagaId:   cmd.SagaID,
				Reason:   fmt.Sprintf("max retries exceeded for action: %s: %s", cmd.Action, cmd.Reason),
				FailedAt: timestamppb.New(now),
			},
		}
		if err := saga.Apply(evt); err != nil {
			return nil, err
		}
		return []es.Event{evt}, nil
	}

	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventTWAPSagaActionFailed,
		Timestamp:   now,
		Data: &sagav1.TWAPSagaActionFailed{
			SagaId:   cmd.SagaID,
			Action:   cmd.Action,
			Attempts: int32(attempts),
			FailedAt: timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

func NewSagaID() string {
	return uuid.New().String()
}
