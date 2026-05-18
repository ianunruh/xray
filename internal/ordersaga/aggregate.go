package ordersaga

import (
	"fmt"
	"strings"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "order-saga"

func AggregateID(sagaID string) string {
	return AggregateType + ":" + sagaID
}

const orderIDPrefix = "order-saga:"

// OrderID returns the deterministic orderbook orderID for a given saga.
// Since each saga places exactly one order, deriving the orderID from
// the sagaID lets the reactor safely retry placement after a crash.
func OrderID(sagaID string) string {
	return orderIDPrefix + sagaID
}

// sagaIDFromOrderID reverses OrderID, returning ok=false if the order
// wasn't placed by a saga (e.g. resting liquidity from another source).
func sagaIDFromOrderID(orderID string) (string, bool) {
	if !strings.HasPrefix(orderID, orderIDPrefix) {
		return "", false
	}
	return strings.TrimPrefix(orderID, orderIDPrefix), true
}

type Status int

const (
	Uninitialized Status = iota
	Started
	CashHeld
	OrderPlaced
	Completed
	Failed
	CollateralHeld
	SharesHeld
)

type OrderSaga struct {
	es.AggregateBase

	SagaID         string
	AccountID      string
	Symbol         string
	Side           orderbook.Side
	Price          int64
	StopPrice      int64
	Quantity       int64
	DisplayQty     int64
	TrailAmount    int64
	TrailOffsetBps int32
	LimitOffset    int64
	OrderType      orderbook.OrderType
	TimeInForce    orderbook.TimeInForce
	ReplaceOrderID string
	OrderID        string
	AmountHeld     int64
	FilledQty      int64
	CashSettled    int64
	FeesPaid       int64
	Status         Status
	ActionAttempts int
	PositionSide   orderbookv1.PositionSide
	CauseEventID   string
	Initiator      sagav1.Initiator
}

func NewOrderSaga(id string) *OrderSaga {
	s := &OrderSaga{}
	s.SetID(id)
	return s
}

func (s *OrderSaga) Apply(evt es.Event) error {
	switch data := evt.Data.(type) {
	case *portfoliov1.OrderSagaStarted:
		s.applyStarted(data)
	case *portfoliov1.OrderSagaCashHeld:
		s.applyCashHeld(data)
	case *portfoliov1.OrderSagaCollateralHeld:
		s.applyCollateralHeld(data)
	case *portfoliov1.OrderSagaOrderPlaced:
		s.applyOrderPlaced(data)
	case *portfoliov1.OrderSagaFillRecorded:
		s.applyFillRecorded(data)
	case *portfoliov1.OrderSagaCompleted:
		s.applyCompleted()
	case *portfoliov1.OrderSagaFailed:
		s.applyFailed()
	case *portfoliov1.OrderSagaActionFailed:
		s.applyActionFailed(data)
	default:
		return fmt.Errorf("unknown event type: %T", evt.Data)
	}
	s.IncrementVersion()
	return nil
}

func (s *OrderSaga) applyStarted(data *portfoliov1.OrderSagaStarted) {
	s.SagaID = data.SagaId
	s.AccountID = data.AccountId
	s.Symbol = data.Symbol
	s.Side = orderbook.SideFromProto(data.Side)
	s.Price = data.Price
	s.StopPrice = data.StopPrice
	s.Quantity = data.Quantity
	s.DisplayQty = data.DisplayQuantity
	s.TrailAmount = data.TrailAmount
	s.TrailOffsetBps = data.TrailOffsetBps
	s.LimitOffset = data.LimitOffset
	s.OrderType = orderbook.OrderTypeFromProto(data.OrderType)
	s.TimeInForce = orderbook.TimeInForceFromProto(data.TimeInForce)
	s.ReplaceOrderID = data.ReplaceOrderId
	s.PositionSide = data.PositionSide
	s.CauseEventID = data.CauseEventId
	s.Initiator = data.Initiator
	s.Status = Started
}

func (s *OrderSaga) applyCashHeld(data *portfoliov1.OrderSagaCashHeld) {
	s.AmountHeld = data.AmountHeld
	s.Status = CashHeld
	s.ActionAttempts = 0
}

func (s *OrderSaga) applyCollateralHeld(data *portfoliov1.OrderSagaCollateralHeld) {
	s.AmountHeld = data.AmountHeld
	s.Status = CollateralHeld
	s.ActionAttempts = 0
}

func (s *OrderSaga) applyOrderPlaced(data *portfoliov1.OrderSagaOrderPlaced) {
	s.OrderID = data.OrderId
	s.Status = OrderPlaced
	s.ActionAttempts = 0
}

func (s *OrderSaga) applyFillRecorded(data *portfoliov1.OrderSagaFillRecorded) {
	s.FilledQty += data.FillQuantity
	s.CashSettled += data.CashSettled
	s.FeesPaid += data.FeeCharged
	s.ActionAttempts = 0
}

func (s *OrderSaga) applyCompleted() {
	s.Status = Completed
}

func (s *OrderSaga) applyFailed() {
	s.Status = Failed
}

func (s *OrderSaga) applyActionFailed(data *portfoliov1.OrderSagaActionFailed) {
	s.ActionAttempts = int(data.Attempts)
}
