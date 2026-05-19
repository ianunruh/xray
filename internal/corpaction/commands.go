package corpaction

import (
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/pkg/es"
)

var (
	ErrAlreadyDeclared       = errors.New("corporate action already declared")
	ErrNotDeclared           = errors.New("corporate action not in declared state")
	ErrInvalidSplitRatio     = errors.New("split ratio must have numerator > 0 and denominator > 0")
	ErrInvalidDividendAmount = errors.New("dividend per-share must be positive")
	ErrMissingNewSymbol      = errors.New("symbol-change requires a new symbol")
	ErrMissingEffectiveDate  = errors.New("split / symbol-change requires effective_date")
	ErrMissingDividendDates  = errors.New("cash dividend requires record_date and pay_date")
	ErrSameSymbol            = errors.New("symbol-change new_symbol must differ from symbol")
	ErrAlreadySnapshotted    = errors.New("dividend record-date snapshot already taken")
)

type Declare struct {
	ActionID         string
	Symbol           string
	Type             corpactionv1.ActionType
	SplitNumerator   int32
	SplitDenominator int32
	DividendPerShare int64
	NewSymbol        string
	EffectiveDate    time.Time
	RecordDate       time.Time
	PayDate          time.Time
}

func (c Declare) AggregateID() string { return AggregateID(c.ActionID) }

// ExecuteDeclare validates the type-specific payload and emits
// CorporateActionDeclared. Type-keyed validation is here (not in the
// aggregate's applier) so replays of valid historical events succeed
// even if the validation rules change later.
func ExecuteDeclare(a *CorporateAction, cmd Declare) ([]es.Event, error) {
	if a.Status != Uninitialized {
		return nil, ErrAlreadyDeclared
	}
	switch cmd.Type {
	case corpactionv1.ActionType_ACTION_TYPE_SPLIT:
		if cmd.SplitNumerator <= 0 || cmd.SplitDenominator <= 0 {
			return nil, ErrInvalidSplitRatio
		}
		if cmd.EffectiveDate.IsZero() {
			return nil, ErrMissingEffectiveDate
		}
	case corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND:
		if cmd.DividendPerShare <= 0 {
			return nil, ErrInvalidDividendAmount
		}
		if cmd.RecordDate.IsZero() || cmd.PayDate.IsZero() {
			return nil, ErrMissingDividendDates
		}
	case corpactionv1.ActionType_ACTION_TYPE_SYMBOL_CHANGE:
		if cmd.NewSymbol == "" {
			return nil, ErrMissingNewSymbol
		}
		if cmd.NewSymbol == cmd.Symbol {
			return nil, ErrSameSymbol
		}
		if cmd.EffectiveDate.IsZero() {
			return nil, ErrMissingEffectiveDate
		}
	default:
		return nil, errors.New("unknown action type")
	}

	now := time.Now()
	data := &corpactionv1.CorporateActionDeclared{
		ActionId:         cmd.ActionID,
		Symbol:           cmd.Symbol,
		Type:             cmd.Type,
		SplitNumerator:   cmd.SplitNumerator,
		SplitDenominator: cmd.SplitDenominator,
		DividendPerShare: cmd.DividendPerShare,
		NewSymbol:        cmd.NewSymbol,
		DeclaredAt:       timestamppb.New(now),
	}
	if !cmd.EffectiveDate.IsZero() {
		data.EffectiveDate = timestamppb.New(cmd.EffectiveDate)
	}
	if !cmd.RecordDate.IsZero() {
		data.RecordDate = timestamppb.New(cmd.RecordDate)
	}
	if !cmd.PayDate.IsZero() {
		data.PayDate = timestamppb.New(cmd.PayDate)
	}
	evt := es.Event{
		AggregateID: a.AggregateID(),
		Type:        EventCorporateActionDeclared,
		Timestamp:   now,
		Data:        data,
	}
	if err := a.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordApplied struct {
	ActionID      string
	HoldersCount  int32
	OrdersCount   int32
	SagasCount    int32
}

func (c RecordApplied) AggregateID() string { return AggregateID(c.ActionID) }

// ExecuteRecordApplied marks the action as Applied. Idempotent — a
// second call is a no-op (the reactor may retry).
func ExecuteRecordApplied(a *CorporateAction, cmd RecordApplied) ([]es.Event, error) {
	if a.Status == Applied {
		return nil, nil
	}
	if a.Status != Declared {
		return nil, ErrNotDeclared
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: a.AggregateID(),
		Type:        EventCorporateActionApplied,
		Timestamp:   now,
		Data: &corpactionv1.CorporateActionApplied{
			ActionId:     cmd.ActionID,
			AppliedAt:    timestamppb.New(now),
			HoldersCount: cmd.HoldersCount,
			OrdersCount:  cmd.OrdersCount,
			SagasCount:   cmd.SagasCount,
		},
	}
	if err := a.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordFailed struct {
	ActionID string
	Reason   string
}

func (c RecordFailed) AggregateID() string { return AggregateID(c.ActionID) }

func ExecuteRecordFailed(a *CorporateAction, cmd RecordFailed) ([]es.Event, error) {
	if a.Status == Failed {
		return nil, nil
	}
	if a.Status != Declared {
		return nil, ErrNotDeclared
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: a.AggregateID(),
		Type:        EventCorporateActionFailed,
		Timestamp:   now,
		Data: &corpactionv1.CorporateActionFailed{
			ActionId: cmd.ActionID,
			Reason:   cmd.Reason,
			FailedAt: timestamppb.New(now),
		},
	}
	if err := a.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}

type RecordDividendSnapshot struct {
	ActionID      string
	Symbol        string
	HoldersCount  int32
}

func (c RecordDividendSnapshot) AggregateID() string { return AggregateID(c.ActionID) }

// ExecuteRecordDividendSnapshot records that the per-action
// record-date snapshot has been taken. Idempotent — a second call
// is a no-op so the reactor can safely re-check on every tick.
func ExecuteRecordDividendSnapshot(a *CorporateAction, cmd RecordDividendSnapshot) ([]es.Event, error) {
	if a.DividendSnapshotted {
		return nil, nil
	}
	if a.Status != Declared {
		return nil, ErrNotDeclared
	}
	now := time.Now()
	evt := es.Event{
		AggregateID: a.AggregateID(),
		Type:        EventDividendRecordSnapshotted,
		Timestamp:   now,
		Data: &corpactionv1.DividendRecordSnapshotted{
			ActionId:       cmd.ActionID,
			Symbol:         cmd.Symbol,
			HoldersCount:   cmd.HoldersCount,
			SnapshottedAt:  timestamppb.New(now),
		},
	}
	if err := a.Apply(evt); err != nil {
		return nil, err
	}
	return []es.Event{evt}, nil
}
