package bracket

import (
	"fmt"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "bracket-saga"

func AggregateID(sagaID string) string {
	return AggregateType + ":" + sagaID
}

// TakeProfitOrderID and StopLossOrderID return deterministic orderbook
// orderIDs for a bracket saga's exit legs. Deriving them from sagaID lets
// the reactor safely retry exit placement after a crash between PlaceOrder
// and RecordEntryFilled — the orderbook treats the retry as a duplicate.
func TakeProfitOrderID(sagaID string) string {
	return "bracket-saga:" + sagaID + ":tp"
}

func StopLossOrderID(sagaID string) string {
	return "bracket-saga:" + sagaID + ":sl"
}

type Status int

const (
	Uninitialized Status = iota
	PendingEntry
	PendingExit
	Completed
	Failed
)

type BracketSaga struct {
	es.AggregateBase

	SagaID            string
	Symbol            string
	EntrySide         orderbook.Side
	EntryPrice        int64
	EntryQty          int64
	TakeProfitPrice   int64
	StopLossPrice     int64
	EntryOrderID      string
	TakeProfitOrderID string
	StopLossOrderID   string
	Status            Status
	EntryFilledQty    int64
	ActionAttempts    int
}

func NewBracketSaga(id string) *BracketSaga {
	s := &BracketSaga{}
	s.SetID(id)
	return s
}

func (s *BracketSaga) Apply(evt es.Event) error {
	switch data := evt.Data.(type) {
	case *orderbookv1.SagaStarted:
		s.applySagaStarted(data)
	case *orderbookv1.EntryFilled:
		s.applyEntryFilled(data)
	case *orderbookv1.ExitFilled:
		s.applyExitFilled(data)
	case *orderbookv1.SagaCompleted:
		s.applySagaCompleted()
	case *orderbookv1.SagaFailed:
		s.applySagaFailed()
	case *orderbookv1.SagaActionFailed:
		s.applySagaActionFailed(data)
	default:
		return fmt.Errorf("unknown event type: %T", evt.Data)
	}
	s.IncrementVersion()
	return nil
}

func (s *BracketSaga) applySagaStarted(data *orderbookv1.SagaStarted) {
	s.SagaID = data.SagaId
	s.Symbol = data.Symbol
	s.EntrySide = orderbook.SideFromProto(data.EntrySide)
	s.EntryPrice = data.EntryPrice
	s.EntryQty = data.EntryQuantity
	s.TakeProfitPrice = data.TakeProfitPrice
	s.StopLossPrice = data.StopLossPrice
	s.EntryOrderID = data.EntryOrderId
	s.Status = PendingEntry
}

func (s *BracketSaga) applyEntryFilled(data *orderbookv1.EntryFilled) {
	s.TakeProfitOrderID = data.TakeProfitOrderId
	s.StopLossOrderID = data.StopLossOrderId
	s.Status = PendingExit
	s.ActionAttempts = 0
}

func (s *BracketSaga) applyExitFilled(_ *orderbookv1.ExitFilled) {
}

func (s *BracketSaga) applySagaCompleted() {
	s.Status = Completed
}

func (s *BracketSaga) applySagaFailed() {
	s.Status = Failed
}

func (s *BracketSaga) applySagaActionFailed(data *orderbookv1.SagaActionFailed) {
	s.ActionAttempts = int(data.Attempts)
}
