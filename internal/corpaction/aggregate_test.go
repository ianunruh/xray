package corpaction_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/internal/corpaction"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/memstore"
)

func newTestHandler() *es.Handler[*corpaction.CorporateAction] {
	registry := es.NewRegistry()
	corpaction.RegisterEvents(registry)
	store := memstore.New()
	return es.NewHandler(store, registry, func(id string) *corpaction.CorporateAction {
		return corpaction.NewCorporateAction(id)
	}, slog.Default())
}

func TestDeclare_Split_Forward(t *testing.T) {
	handler := newTestHandler()
	ctx := context.Background()
	effective := time.Now().Add(1 * time.Hour)

	cmd := corpaction.Declare{
		ActionID:         "action-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    effective,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, cmd)
	}))

	a, err := handler.Load(ctx, corpaction.AggregateID("action-1"))
	require.NoError(t, err)
	assert.Equal(t, corpaction.Declared, a.Status)
	assert.Equal(t, "AAPL", a.Symbol)
	assert.Equal(t, int32(2), a.SplitNumerator)
	assert.Equal(t, int32(1), a.SplitDenominator)
	assert.WithinDuration(t, effective, a.EffectiveDate, time.Second)
}

func TestDeclare_Split_Reverse(t *testing.T) {
	handler := newTestHandler()
	ctx := context.Background()

	cmd := corpaction.Declare{
		ActionID:         "action-2",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   1,
		SplitDenominator: 10,
		EffectiveDate:    time.Now().Add(1 * time.Hour),
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, cmd)
	}))

	a, err := handler.Load(ctx, corpaction.AggregateID("action-2"))
	require.NoError(t, err)
	assert.Equal(t, int32(1), a.SplitNumerator)
	assert.Equal(t, int32(10), a.SplitDenominator, "reverse split has denominator > numerator")
}

func TestDeclare_Dividend(t *testing.T) {
	handler := newTestHandler()
	ctx := context.Background()
	record := time.Now().Add(24 * time.Hour)
	pay := record.Add(48 * time.Hour)

	cmd := corpaction.Declare{
		ActionID:         "div-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 2400, // $0.24
		RecordDate:       record,
		PayDate:          pay,
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, cmd)
	}))

	a, err := handler.Load(ctx, corpaction.AggregateID("div-1"))
	require.NoError(t, err)
	assert.Equal(t, int64(2400), a.DividendPerShare)
	assert.WithinDuration(t, record, a.RecordDate, time.Second)
	assert.WithinDuration(t, pay, a.PayDate, time.Second)
}

func TestDeclare_SymbolChange(t *testing.T) {
	handler := newTestHandler()
	ctx := context.Background()

	cmd := corpaction.Declare{
		ActionID:      "rename-1",
		Symbol:        "FB",
		Type:          corpactionv1.ActionType_ACTION_TYPE_SYMBOL_CHANGE,
		NewSymbol:     "META",
		EffectiveDate: time.Now().Add(1 * time.Hour),
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, cmd)
	}))

	a, err := handler.Load(ctx, corpaction.AggregateID("rename-1"))
	require.NoError(t, err)
	assert.Equal(t, "META", a.NewSymbol)
}

func TestDeclare_Validation(t *testing.T) {
	cases := []struct {
		name string
		cmd  corpaction.Declare
		want error
	}{
		{
			name: "split missing ratio",
			cmd: corpaction.Declare{
				ActionID: "x", Symbol: "AAPL",
				Type:          corpactionv1.ActionType_ACTION_TYPE_SPLIT,
				EffectiveDate: time.Now(),
			},
			want: corpaction.ErrInvalidSplitRatio,
		},
		{
			name: "split missing effective date",
			cmd: corpaction.Declare{
				ActionID: "x", Symbol: "AAPL",
				Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
				SplitNumerator:   2,
				SplitDenominator: 1,
			},
			want: corpaction.ErrMissingEffectiveDate,
		},
		{
			name: "dividend missing amount",
			cmd: corpaction.Declare{
				ActionID: "x", Symbol: "AAPL",
				Type:       corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
				RecordDate: time.Now(),
				PayDate:    time.Now().Add(time.Hour),
			},
			want: corpaction.ErrInvalidDividendAmount,
		},
		{
			name: "dividend missing dates",
			cmd: corpaction.Declare{
				ActionID: "x", Symbol: "AAPL",
				Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
				DividendPerShare: 100,
			},
			want: corpaction.ErrMissingDividendDates,
		},
		{
			name: "rename missing new symbol",
			cmd: corpaction.Declare{
				ActionID: "x", Symbol: "AAPL",
				Type:          corpactionv1.ActionType_ACTION_TYPE_SYMBOL_CHANGE,
				EffectiveDate: time.Now(),
			},
			want: corpaction.ErrMissingNewSymbol,
		},
		{
			name: "rename same symbol",
			cmd: corpaction.Declare{
				ActionID: "x", Symbol: "AAPL",
				Type:          corpactionv1.ActionType_ACTION_TYPE_SYMBOL_CHANGE,
				NewSymbol:     "AAPL",
				EffectiveDate: time.Now(),
			},
			want: corpaction.ErrSameSymbol,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := newTestHandler()
			err := handler.Handle(context.Background(), tc.cmd, func(a *corpaction.CorporateAction) ([]es.Event, error) {
				return corpaction.ExecuteDeclare(a, tc.cmd)
			})
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

func TestStateMachine_DeclaredToApplied(t *testing.T) {
	handler := newTestHandler()
	ctx := context.Background()

	declare := corpaction.Declare{
		ActionID:         "action-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    time.Now(),
	}
	require.NoError(t, handler.Handle(ctx, declare, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, declare)
	}))

	applied := corpaction.RecordApplied{ActionID: "action-1", HoldersCount: 5, OrdersCount: 3, SagasCount: 1}
	require.NoError(t, handler.Handle(ctx, applied, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteRecordApplied(a, applied)
	}))

	a, err := handler.Load(ctx, corpaction.AggregateID("action-1"))
	require.NoError(t, err)
	assert.Equal(t, corpaction.Applied, a.Status)
	assert.Equal(t, int32(5), a.HoldersCount)
	assert.Equal(t, int32(3), a.OrdersCount)
	assert.Equal(t, int32(1), a.SagasCount)
	assert.False(t, a.AppliedAt.IsZero())
}

func TestStateMachine_AppliedIdempotent(t *testing.T) {
	handler := newTestHandler()
	ctx := context.Background()

	declare := corpaction.Declare{
		ActionID:         "action-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    time.Now(),
	}
	require.NoError(t, handler.Handle(ctx, declare, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, declare)
	}))
	applied := corpaction.RecordApplied{ActionID: "action-1"}
	require.NoError(t, handler.Handle(ctx, applied, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteRecordApplied(a, applied)
	}))

	// Second apply is a no-op (returns nil events without error).
	require.NoError(t, handler.Handle(ctx, applied, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		evts, err := corpaction.ExecuteRecordApplied(a, applied)
		require.NoError(t, err)
		assert.Empty(t, evts, "second apply must be a no-op")
		return evts, nil
	}))
}

func TestDoubleDeclareRejected(t *testing.T) {
	handler := newTestHandler()
	ctx := context.Background()

	cmd := corpaction.Declare{
		ActionID:         "action-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    time.Now(),
	}
	require.NoError(t, handler.Handle(ctx, cmd, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, cmd)
	}))
	err := handler.Handle(ctx, cmd, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, cmd)
	})
	assert.ErrorIs(t, err, corpaction.ErrAlreadyDeclared)
}

func TestDividendSnapshotIdempotent(t *testing.T) {
	handler := newTestHandler()
	ctx := context.Background()

	declare := corpaction.Declare{
		ActionID:         "div-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 1000,
		RecordDate:       time.Now(),
		PayDate:          time.Now().Add(48 * time.Hour),
	}
	require.NoError(t, handler.Handle(ctx, declare, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, declare)
	}))

	snap := corpaction.RecordDividendSnapshot{ActionID: "div-1", Symbol: "AAPL", HoldersCount: 7}
	require.NoError(t, handler.Handle(ctx, snap, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteRecordDividendSnapshot(a, snap)
	}))
	a, err := handler.Load(ctx, corpaction.AggregateID("div-1"))
	require.NoError(t, err)
	assert.True(t, a.DividendSnapshotted)

	// Second snapshot call is a no-op.
	require.NoError(t, handler.Handle(ctx, snap, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		evts, err := corpaction.ExecuteRecordDividendSnapshot(a, snap)
		require.NoError(t, err)
		assert.Empty(t, evts)
		return evts, nil
	}))
}
