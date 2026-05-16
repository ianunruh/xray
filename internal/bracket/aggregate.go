package bracket

import (
	"fmt"
	"strings"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "bracket-saga"

func AggregateID(sagaID string) string {
	return AggregateType + ":" + sagaID
}

// EntryOrderID, TakeProfitOrderID, and StopLossOrderID return deterministic
// orderbook orderIDs for a bracket saga's legs. Deriving them from sagaID
// lets the reactor reverse-lookup which saga owns an order without
// maintaining an in-memory map, and lets PlaceOrder retries after a crash
// be treated as duplicates by the orderbook's idempotency check.
func EntryOrderID(sagaID string) string {
	return orderIDPrefix + sagaID + ":entry"
}

func TakeProfitOrderID(sagaID string) string {
	return orderIDPrefix + sagaID + ":tp"
}

func StopLossOrderID(sagaID string) string {
	return orderIDPrefix + sagaID + ":sl"
}

const orderIDPrefix = "bracket-saga:"

// sagaIDFromOrderID reverses any of the *OrderID helpers, returning
// ok=false if the orderID wasn't placed by a bracket saga (e.g. resting
// liquidity from another source). Recognizes the :entry, :tp, and :sl
// suffixes.
func sagaIDFromOrderID(orderID string) (string, bool) {
	rest, ok := strings.CutPrefix(orderID, orderIDPrefix)
	if !ok {
		return "", false
	}
	for _, suffix := range []string{":entry", ":tp", ":sl"} {
		if s, ok := strings.CutSuffix(rest, suffix); ok {
			return s, true
		}
	}
	return "", false
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
