package ocosaga

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const MaxActionAttempts = 3

var (
	ErrInvalidPrice    = errors.New("price must be positive")
	ErrInvalidQuantity = errors.New("quantity must be positive")
	ErrInvalidState    = errors.New("saga is not in the expected state")
)

type StartOCOSaga struct {
	SagaID          string
	AccountID       string
	Symbol          string
	ExitSide        orderbookv1.Side
	Quantity        int64
	TakeProfitPrice int64
	StopLossPrice   int64
	PositionSide    orderbookv1.PositionSide
}

func (c StartOCOSaga) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteStartOCOSaga(saga *OCOSaga, cmd StartOCOSaga) ([]es.Event, error) {
	if saga.Version() != 0 {
		return nil, ErrInvalidState
	}
	if cmd.Quantity <= 0 {
		return nil, ErrInvalidQuantity
	}
	if cmd.TakeProfitPrice <= 0 || cmd.StopLossPrice <= 0 {
		return nil, ErrInvalidPrice
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOCOSagaStarted,
		Timestamp:   now,
		Data: &orderbookv1.OCOSagaStarted{
			SagaId:          cmd.SagaID,
			AccountId:       cmd.AccountID,
			Symbol:          cmd.Symbol,
			ExitSide:        cmd.ExitSide,
			Quantity:        cmd.Quantity,
			TakeProfitPrice: cmd.TakeProfitPrice,
			StopLossPrice:   cmd.StopLossPrice,
			StartedAt:       timestamppb.New(now),
			PositionSide:    cmd.PositionSide,
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordSharesHeld struct {
	SagaID string
}

func (c RecordSharesHeld) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordSharesHeld(saga *OCOSaga, cmd RecordSharesHeld) ([]es.Event, error) {
	if saga.Status != Started {
		return nil, ErrInvalidState
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOCOSagaSharesHeld,
		Timestamp:   now,
		Data: &orderbookv1.OCOSagaSharesHeld{
			SagaId: cmd.SagaID,
			HeldAt: timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordExitPlaced struct {
	SagaID            string
	TakeProfitOrderID string
	StopLossOrderID   string
}

func (c RecordExitPlaced) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordExitPlaced(saga *OCOSaga, cmd RecordExitPlaced) ([]es.Event, error) {
	if saga.Status != SharesHeld {
		return nil, ErrInvalidState
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOCOSagaExitPlaced,
		Timestamp:   now,
		Data: &orderbookv1.OCOSagaExitPlaced{
			SagaId:            cmd.SagaID,
			TakeProfitOrderId: cmd.TakeProfitOrderID,
			StopLossOrderId:   cmd.StopLossOrderID,
			PlacedAt:          timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordFill struct {
	SagaID       string
	TradeID      string
	OrderID      string
	FillQuantity int64
	FillPrice    int64
}

func (c RecordFill) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordFill(saga *OCOSaga, cmd RecordFill) ([]es.Event, error) {
	if saga.Status != ExitPlaced {
		return nil, ErrInvalidState
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOCOSagaFillRecorded,
		Timestamp:   now,
		Data: &orderbookv1.OCOSagaFillRecorded{
			SagaId:       cmd.SagaID,
			TradeId:      cmd.TradeID,
			OrderId:      cmd.OrderID,
			FillQuantity: cmd.FillQuantity,
			FillPrice:    cmd.FillPrice,
			RecordedAt:   timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordCompleted struct {
	SagaID string
}

func (c RecordCompleted) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordCompleted(saga *OCOSaga, cmd RecordCompleted) ([]es.Event, error) {
	if saga.Status != ExitPlaced {
		return nil, ErrInvalidState
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOCOSagaCompleted,
		Timestamp:   now,
		Data: &orderbookv1.OCOSagaCompleted{
			SagaId:      cmd.SagaID,
			CompletedAt: timestamppb.New(now),
		},
	}
	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordFailed struct {
	SagaID string
	Reason string
}

func (c RecordFailed) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordFailed(saga *OCOSaga, cmd RecordFailed) ([]es.Event, error) {
	if saga.Status == Completed || saga.Status == Failed {
		return nil, ErrInvalidState
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOCOSagaFailed,
		Timestamp:   now,
		Data: &orderbookv1.OCOSagaFailed{
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

func ExecuteRecordActionFailed(saga *OCOSaga, cmd RecordActionFailed) ([]es.Event, error) {
	if saga.Status == Completed || saga.Status == Failed {
		return nil, ErrInvalidState
	}
	attempts := saga.ActionAttempts + 1
	now := time.Now()
	if attempts >= MaxActionAttempts {
		// Out of retries — bail by recording the saga as failed.
		failEvt := es.Event{
			AggregateID: saga.AggregateID(),
			Type:        EventOCOSagaFailed,
			Timestamp:   now,
			Data: &orderbookv1.OCOSagaFailed{
				SagaId:   cmd.SagaID,
				Reason:   "max retries exceeded for action: " + cmd.Action + ": " + cmd.Reason,
				FailedAt: timestamppb.New(now),
			},
		}
		if err := saga.Apply(failEvt); err != nil {
			return nil, err
		}
		return []es.Event{failEvt}, nil
	}
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOCOSagaActionFailed,
		Timestamp:   now,
		Data: &orderbookv1.OCOSagaActionFailed{
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

// NewSagaID returns a fresh OCO saga ID.
func NewSagaID() string {
	return uuid.New().String()
}
