package twapsaga

import (
	"fmt"
	"strings"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

const AggregateType = "twap-saga"

func AggregateID(sagaID string) string {
	return AggregateType + ":" + sagaID
}

const childSagaIDPrefix = "twap-slice:"

// ChildSagaID returns the deterministic OrderSaga ID for slice N of a
// TWAP. The shared prefix lets the saga projection's List() filter hide
// these from user-facing responses, and lets the reactor reverse the
// child saga ID back to (twapID, sliceIndex) when observing child
// lifecycle events.
func ChildSagaID(twapID string, sliceIndex int32) string {
	return fmt.Sprintf("%s%s:%d", childSagaIDPrefix, twapID, sliceIndex)
}

// ChildSagaIDPrefix is exported so the sagasvc projection can hide
// child TWAP slice sagas from List() responses.
func ChildSagaIDPrefix() string {
	return childSagaIDPrefix
}

// twapIDAndSliceFromChildSagaID reverses ChildSagaID; returns ok=false
// for ordersagas that weren't spawned by a TWAP.
func twapIDAndSliceFromChildSagaID(childSagaID string) (string, int32, bool) {
	rest, ok := strings.CutPrefix(childSagaID, childSagaIDPrefix)
	if !ok {
		return "", 0, false
	}
	// rest is "{twapID}:{sliceIndex}"; twapID is a UUID with no colons,
	// so split on the LAST colon to allow UUIDs containing arbitrary chars.
	idx := strings.LastIndex(rest, ":")
	if idx < 0 {
		return "", 0, false
	}
	var sliceIndex int32
	if _, err := fmt.Sscanf(rest[idx+1:], "%d", &sliceIndex); err != nil {
		return "", 0, false
	}
	return rest[:idx], sliceIndex, true
}

type Status int

const (
	Uninitialized Status = iota
	Active
	Completed
	Failed
)

// Slice records the lifecycle of one TWAP slice.
type Slice struct {
	Index            int32
	ChildSagaID      string
	LaunchedQuantity int64
	FilledQuantity   int64
	CashSettled      int64
	LaunchedAt       time.Time
	CompletedAt      time.Time
	Completed        bool
}

type TWAPSaga struct {
	es.AggregateBase

	SagaID          string
	AccountID       string
	Symbol          string
	Side            orderbook.Side
	PositionSide    orderbookv1.PositionSide
	TotalQuantity   int64
	SliceCount      int32
	SliceIntervalMs int64
	LimitPrice      int64
	StartedAt       time.Time
	Status          Status
	Slices          []Slice
	TotalFilled     int64
	TotalSettled    int64
	ActionAttempts  int
	Initiator       sagav1.Initiator
}

func NewTWAPSaga(id string) *TWAPSaga {
	s := &TWAPSaga{}
	s.SetID(id)
	return s
}

// SliceInterval returns the wall-clock gap between slice launches.
func (s *TWAPSaga) SliceInterval() time.Duration {
	return time.Duration(s.SliceIntervalMs) * time.Millisecond
}

// SlicesLaunched returns the number of slices for which TWAPSliceLaunched
// has been recorded.
func (s *TWAPSaga) SlicesLaunched() int32 {
	return int32(len(s.Slices))
}

// CurrentSlice returns the most recently launched slice (or nil if none
// have been launched yet).
func (s *TWAPSaga) CurrentSlice() *Slice {
	if len(s.Slices) == 0 {
		return nil
	}
	return &s.Slices[len(s.Slices)-1]
}

// PlannedSliceQuantity returns the qty for slice N under an even split
// of TotalQuantity over SliceCount, before any rollover adjustment.
// The final slice absorbs the remainder so the per-slice qtys sum to
// TotalQuantity exactly.
func (s *TWAPSaga) PlannedSliceQuantity(sliceIndex int32) int64 {
	if s.SliceCount <= 0 {
		return 0
	}
	base := s.TotalQuantity / int64(s.SliceCount)
	if sliceIndex == s.SliceCount-1 {
		return base + (s.TotalQuantity - base*int64(s.SliceCount))
	}
	return base
}

// NextSliceQuantity computes the size of the next slice to launch,
// rolling any prior underfill forward so the saga stays on-pace. The
// returned qty equals the planned cumulative qty for slices [0..N]
// minus what has actually filled so far.
func (s *TWAPSaga) NextSliceQuantity() int64 {
	next := s.SlicesLaunched()
	if next >= s.SliceCount {
		return 0
	}
	plannedThroughNext := int64(0)
	for i := int32(0); i <= next; i++ {
		plannedThroughNext += s.PlannedSliceQuantity(i)
	}
	qty := plannedThroughNext - s.TotalFilled
	if qty <= 0 {
		// Already filled the cumulative target; emit a zero-qty slice
		// only if it's the final one (so the saga still records a launch).
		// Otherwise skip ahead (caller decides).
		return 0
	}
	// Don't exceed total remaining.
	remaining := s.TotalQuantity - s.TotalFilled
	if qty > remaining {
		qty = remaining
	}
	return qty
}

// AllSlicesTerminal reports whether every launched slice has a
// TWAPSliceCompleted event recorded.
func (s *TWAPSaga) AllSlicesTerminal() bool {
	for i := range s.Slices {
		if !s.Slices[i].Completed {
			return false
		}
	}
	return true
}

func (s *TWAPSaga) Apply(evt es.Event) error {
	switch data := evt.Data.(type) {
	case *sagav1.TWAPSagaStarted:
		s.applyStarted(data)
	case *sagav1.TWAPSliceLaunched:
		s.applySliceLaunched(data)
	case *sagav1.TWAPSliceCompleted:
		s.applySliceCompleted(data)
	case *sagav1.TWAPSagaCompleted:
		s.applySagaCompleted()
	case *sagav1.TWAPSagaFailed:
		s.applySagaFailed()
	case *sagav1.TWAPSagaActionFailed:
		s.applyActionFailed(data)
	default:
		return fmt.Errorf("unknown event type: %T", evt.Data)
	}
	s.IncrementVersion()
	return nil
}

func (s *TWAPSaga) applyStarted(data *sagav1.TWAPSagaStarted) {
	s.SagaID = data.SagaId
	s.AccountID = data.AccountId
	s.Symbol = data.Symbol
	s.Side = orderbook.SideFromProto(data.Side)
	s.PositionSide = data.PositionSide
	s.TotalQuantity = data.TotalQuantity
	s.SliceCount = data.SliceCount
	s.SliceIntervalMs = data.SliceIntervalMs
	s.LimitPrice = data.LimitPrice
	s.StartedAt = data.StartedAt.AsTime()
	s.Initiator = data.Initiator
	s.Status = Active
}

func (s *TWAPSaga) applySliceLaunched(data *sagav1.TWAPSliceLaunched) {
	s.Slices = append(s.Slices, Slice{
		Index:            data.SliceIndex,
		ChildSagaID:      data.ChildSagaId,
		LaunchedQuantity: data.SliceQuantity,
		LaunchedAt:       data.LaunchedAt.AsTime(),
	})
	s.ActionAttempts = 0
}

func (s *TWAPSaga) applySliceCompleted(data *sagav1.TWAPSliceCompleted) {
	for i := range s.Slices {
		if s.Slices[i].Index == data.SliceIndex {
			s.Slices[i].FilledQuantity = data.FilledQuantity
			s.Slices[i].CashSettled = data.CashSettled
			s.Slices[i].CompletedAt = data.CompletedAt.AsTime()
			s.Slices[i].Completed = true
			break
		}
	}
	s.TotalFilled += data.FilledQuantity
	s.TotalSettled += data.CashSettled
	s.ActionAttempts = 0
}

func (s *TWAPSaga) applySagaCompleted() {
	s.Status = Completed
}

func (s *TWAPSaga) applySagaFailed() {
	s.Status = Failed
}

func (s *TWAPSaga) applyActionFailed(data *sagav1.TWAPSagaActionFailed) {
	s.ActionAttempts = int(data.Attempts)
}
