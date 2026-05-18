package ordersaga

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/pkg/es"
)

const MaxActionAttempts = 3

var (
	ErrInvalidPrice    = errors.New("price must be positive")
	ErrInvalidQuantity = errors.New("quantity must be positive")
	ErrInvalidState    = errors.New("saga is not in the expected state")
)

type StartOrderSaga struct {
	SagaID         string
	AccountID      string
	Symbol         string
	Side           orderbookv1.Side
	Price          int64
	StopPrice      int64
	Quantity       int64
	DisplayQty     int64
	TrailAmount    int64
	TrailOffsetBps int32
	LimitOffset    int64
	OrderType      orderbookv1.OrderType
	TimeInForce    orderbookv1.TimeInForce
	ReplaceOrderID string
	PositionSide   orderbookv1.PositionSide
	CauseEventID   string
	Initiator      sagav1.Initiator
}

func (c StartOrderSaga) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteStartOrderSaga(saga *OrderSaga, cmd StartOrderSaga) ([]es.Event, error) {
	if saga.Version() != 0 {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOrderSagaStarted,
		Timestamp:   now,
		Data: &portfoliov1.OrderSagaStarted{
			SagaId:          cmd.SagaID,
			AccountId:       cmd.AccountID,
			Symbol:          cmd.Symbol,
			Side:            cmd.Side,
			Price:           cmd.Price,
			StopPrice:       cmd.StopPrice,
			Quantity:        cmd.Quantity,
			DisplayQuantity: cmd.DisplayQty,
			TrailAmount:     cmd.TrailAmount,
			TrailOffsetBps:  cmd.TrailOffsetBps,
			LimitOffset:     cmd.LimitOffset,
			OrderType:       cmd.OrderType,
			TimeInForce:     cmd.TimeInForce,
			StartedAt:       timestamppb.New(now),
			ReplaceOrderId:  cmd.ReplaceOrderID,
			PositionSide:    cmd.PositionSide,
			CauseEventId:    cmd.CauseEventID,
			Initiator:       cmd.Initiator,
		},
	}

	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordCashHeld struct {
	SagaID     string
	AmountHeld int64
}

func (c RecordCashHeld) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordCashHeld(saga *OrderSaga, cmd RecordCashHeld) ([]es.Event, error) {
	if saga.Status != Started {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOrderSagaCashHeld,
		Timestamp:   now,
		Data: &portfoliov1.OrderSagaCashHeld{
			SagaId:     cmd.SagaID,
			AmountHeld: cmd.AmountHeld,
			HeldAt:     timestamppb.New(now),
		},
	}

	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordCollateralHeld struct {
	SagaID     string
	AmountHeld int64
}

func (c RecordCollateralHeld) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordCollateralHeld(saga *OrderSaga, cmd RecordCollateralHeld) ([]es.Event, error) {
	if saga.Status != Started {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOrderSagaCollateralHeld,
		Timestamp:   now,
		Data: &portfoliov1.OrderSagaCollateralHeld{
			SagaId:     cmd.SagaID,
			AmountHeld: cmd.AmountHeld,
			HeldAt:     timestamppb.New(now),
		},
	}

	if err := saga.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordOrderPlaced struct {
	SagaID  string
	OrderID string
}

func (c RecordOrderPlaced) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordOrderPlaced(saga *OrderSaga, cmd RecordOrderPlaced) ([]es.Event, error) {
	if saga.Status != CashHeld && saga.Status != CollateralHeld && saga.Status != SharesHeld {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOrderSagaOrderPlaced,
		Timestamp:   now,
		Data: &portfoliov1.OrderSagaOrderPlaced{
			SagaId:   cmd.SagaID,
			OrderId:  cmd.OrderID,
			PlacedAt: timestamppb.New(now),
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
	FillQuantity int64
	FillPrice    int64
	CashSettled  int64
	FeeCharged   int64
}

func (c RecordFill) AggregateID() string {
	return AggregateID(c.SagaID)
}

func ExecuteRecordFill(saga *OrderSaga, cmd RecordFill) ([]es.Event, error) {
	if saga.Status != OrderPlaced {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOrderSagaFillRecorded,
		Timestamp:   now,
		Data: &portfoliov1.OrderSagaFillRecorded{
			SagaId:       cmd.SagaID,
			TradeId:      cmd.TradeID,
			FillQuantity: cmd.FillQuantity,
			FillPrice:    cmd.FillPrice,
			CashSettled:  cmd.CashSettled,
			FeeCharged:   cmd.FeeCharged,
			FilledAt:     timestamppb.New(now),
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

func ExecuteRecordCompleted(saga *OrderSaga, cmd RecordCompleted) ([]es.Event, error) {
	if saga.Status != OrderPlaced {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOrderSagaCompleted,
		Timestamp:   now,
		Data: &portfoliov1.OrderSagaCompleted{
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

func ExecuteRecordFailed(saga *OrderSaga, cmd RecordFailed) ([]es.Event, error) {
	if saga.Status == Completed || saga.Status == Failed || saga.Status == Uninitialized {
		return nil, ErrInvalidState
	}

	now := time.Now()
	evt := es.Event{
		AggregateID: saga.AggregateID(),
		Type:        EventOrderSagaFailed,
		Timestamp:   now,
		Data: &portfoliov1.OrderSagaFailed{
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

func ExecuteRecordActionFailed(saga *OrderSaga, cmd RecordActionFailed) ([]es.Event, error) {
	if saga.Status == Completed || saga.Status == Failed || saga.Status == Uninitialized {
		return nil, ErrInvalidState
	}

	attempts := saga.ActionAttempts + 1
	now := time.Now()

	if attempts >= MaxActionAttempts {
		evt := es.Event{
			AggregateID: saga.AggregateID(),
			Type:        EventOrderSagaFailed,
			Timestamp:   now,
			Data: &portfoliov1.OrderSagaFailed{
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
		Type:        EventOrderSagaActionFailed,
		Timestamp:   now,
		Data: &portfoliov1.OrderSagaActionFailed{
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
