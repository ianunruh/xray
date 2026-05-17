package ocosaga

import (
	"fmt"
	"strings"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "oco-saga"

func AggregateID(sagaID string) string {
	return AggregateType + ":" + sagaID
}

const orderIDPrefix = "oco-saga:"

// TakeProfitOrderID and StopLossOrderID return deterministic orderbook
// orderIDs for the OCO saga's two legs. OCOGroupID returns the
// orderbook OCO group ID that links them — when one fills, the
// orderbook atomically cancels the other.
func TakeProfitOrderID(sagaID string) string {
	return orderIDPrefix + sagaID + ":tp"
}

func StopLossOrderID(sagaID string) string {
	return orderIDPrefix + sagaID + ":sl"
}

func OCOGroupID(sagaID string) string {
	return orderIDPrefix + sagaID + ":oco"
}

// sagaIDFromOrderID reverses TakeProfitOrderID / StopLossOrderID,
// returning ok=false if the orderID wasn't placed by an OCO saga.
func sagaIDFromOrderID(orderID string) (string, bool) {
	rest, ok := strings.CutPrefix(orderID, orderIDPrefix)
	if !ok {
		return "", false
	}
	for _, suffix := range []string{":tp", ":sl"} {
		if s, ok := strings.CutSuffix(rest, suffix); ok {
			return s, true
		}
	}
	return "", false
}

type Status int

const (
	Uninitialized Status = iota
	Started
	SharesHeld
	ExitPlaced
	Completed
	Failed
)

type OCOSaga struct {
	es.AggregateBase

	SagaID            string
	AccountID         string
	Symbol            string
	ExitSide          orderbook.Side
	Quantity          int64
	TakeProfitPrice   int64
	StopLossPrice     int64
	TakeProfitOrderID string
	StopLossOrderID   string
	SettledQty        int64
	Status            Status
	ActionAttempts    int
	PositionSide      orderbookv1.PositionSide
}

func NewOCOSaga(id string) *OCOSaga {
	s := &OCOSaga{}
	s.SetID(id)
	return s
}

func (s *OCOSaga) Apply(evt es.Event) error {
	switch data := evt.Data.(type) {
	case *orderbookv1.OCOSagaStarted:
		s.applyStarted(data)
	case *orderbookv1.OCOSagaSharesHeld:
		s.Status = SharesHeld
		s.ActionAttempts = 0
	case *orderbookv1.OCOSagaExitPlaced:
		s.applyExitPlaced(data)
	case *orderbookv1.OCOSagaFillRecorded:
		s.SettledQty += data.FillQuantity
		s.ActionAttempts = 0
	case *orderbookv1.OCOSagaCompleted:
		s.Status = Completed
	case *orderbookv1.OCOSagaFailed:
		s.Status = Failed
	case *orderbookv1.OCOSagaActionFailed:
		s.ActionAttempts = int(data.Attempts)
	default:
		return fmt.Errorf("unknown event type: %T", evt.Data)
	}
	s.IncrementVersion()
	return nil
}

func (s *OCOSaga) applyStarted(data *orderbookv1.OCOSagaStarted) {
	s.SagaID = data.SagaId
	s.AccountID = data.AccountId
	s.Symbol = data.Symbol
	s.ExitSide = orderbook.SideFromProto(data.ExitSide)
	s.Quantity = data.Quantity
	s.TakeProfitPrice = data.TakeProfitPrice
	s.StopLossPrice = data.StopLossPrice
	s.PositionSide = data.PositionSide
	s.Status = Started
}

func (s *OCOSaga) applyExitPlaced(data *orderbookv1.OCOSagaExitPlaced) {
	s.TakeProfitOrderID = data.TakeProfitOrderId
	s.StopLossOrderID = data.StopLossOrderId
	s.Status = ExitPlaced
	s.ActionAttempts = 0
}
