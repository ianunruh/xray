package saga

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

var (
	ErrInvalidPrice    = errors.New("price must be positive")
	ErrInvalidQuantity = errors.New("quantity must be positive")
	ErrInvalidState    = errors.New("saga is not in the expected state")
)

type StartSaga struct {
	SagaID          string
	Symbol          string
	EntrySide       orderbookv1.Side
	EntryPrice      int64
	EntryQty        int64
	TakeProfitPrice int64
	StopLossPrice   int64
	EntryOrderID    string
}

func (c StartSaga) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteStartSaga(saga *BracketSaga, cmd StartSaga) ([]es.Event, error) {
	if saga.Version() != 0 {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        "SagaStarted",
		Timestamp:   now,
		Data: &orderbookv1.SagaStarted{
			SagaId:          cmd.SagaID,
			Symbol:          cmd.Symbol,
			EntrySide:       cmd.EntrySide,
			EntryPrice:      cmd.EntryPrice,
			EntryQuantity:   cmd.EntryQty,
			TakeProfitPrice: cmd.TakeProfitPrice,
			StopLossPrice:   cmd.StopLossPrice,
			EntryOrderId:    cmd.EntryOrderID,
			StartedAt:       timestamppb.New(now),
		},
	}

	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordEntryFilled struct {
	SagaID            string
	TakeProfitOrderID string
	StopLossOrderID   string
}

func (c RecordEntryFilled) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordEntryFilled(saga *BracketSaga, cmd RecordEntryFilled) ([]es.Event, error) {
	if saga.Status != PendingEntry {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        "EntryFilled",
		Timestamp:   now,
		Data: &orderbookv1.EntryFilled{
			SagaId:            cmd.SagaID,
			TakeProfitOrderId: cmd.TakeProfitOrderID,
			StopLossOrderId:   cmd.StopLossOrderID,
			FilledAt:          timestamppb.New(now),
		},
	}

	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordExitFilled struct {
	SagaID           string
	FilledOrderID    string
	CancelledOrderID string
}

func (c RecordExitFilled) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordExitFilled(saga *BracketSaga, cmd RecordExitFilled) ([]es.Event, error) {
	if saga.Status != PendingExit {
		return nil, ErrInvalidState
	}

	now := time.Now()
	exitEvt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        "ExitFilled",
		Timestamp:   now,
		Data: &orderbookv1.ExitFilled{
			SagaId:           cmd.SagaID,
			FilledOrderId:    cmd.FilledOrderID,
			CancelledOrderId: cmd.CancelledOrderID,
			FilledAt:         timestamppb.New(now),
		},
	}
	if err := saga.Apply(exitEvt); err != nil {
		return nil, err
	}

	completedEvt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        "SagaCompleted",
		Timestamp:   now,
		Data: &orderbookv1.SagaCompleted{
			SagaId:      cmd.SagaID,
			CompletedAt: timestamppb.New(now),
		},
	}
	if err := saga.Apply(completedEvt); err != nil {
		return nil, err
	}

	return []es.Event{exitEvt, completedEvt}, nil
}

type RecordSagaFailed struct {
	SagaID string
	Reason string
}

func (c RecordSagaFailed) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordSagaFailed(saga *BracketSaga, cmd RecordSagaFailed) ([]es.Event, error) {
	if saga.Status != PendingEntry {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        "SagaFailed",
		Timestamp:   now,
		Data: &orderbookv1.SagaFailed{
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

func NewSagaID() string {
	return uuid.New().String()
}
