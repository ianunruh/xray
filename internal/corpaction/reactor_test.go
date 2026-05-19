package corpaction_test

import (
	"context"
	"errors"
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

// fakeReader is a minimal in-memory Reader for reactor tests. The PG
// projection's SQL queries are exercised manually in dev; in unit
// tests we drive the reactor against this in-memory mirror.
type fakeReader struct {
	rows         []corpaction.ActionRow
	snapshotted  map[string]bool
	appliedIds   map[string]bool
}

func newFakeReader() *fakeReader {
	return &fakeReader{
		snapshotted: make(map[string]bool),
		appliedIds:  make(map[string]bool),
	}
}

func (f *fakeReader) add(row corpaction.ActionRow) { f.rows = append(f.rows, row) }

func (f *fakeReader) DueActions(_ context.Context, before time.Time) ([]corpaction.ActionRow, error) {
	var out []corpaction.ActionRow
	for _, r := range f.rows {
		if f.appliedIds[r.ActionID] {
			continue
		}
		trigger := r.EffectiveDate
		if r.Type == corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND {
			trigger = r.PayDate
		}
		if trigger.After(before) {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeReader) DueDividendSnapshots(_ context.Context, before time.Time) ([]corpaction.ActionRow, error) {
	var out []corpaction.ActionRow
	for _, r := range f.rows {
		if r.Type != corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND {
			continue
		}
		if f.snapshotted[r.ActionID] {
			continue
		}
		if r.RecordDate.After(before) {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeReader) List(_ context.Context, _ string, _ corpactionv1.ActionStatus, _ int32) ([]*corpactionv1.CorporateActionRecord, error) {
	return nil, nil
}
func (f *fakeReader) Get(_ context.Context, _ string) (*corpactionv1.CorporateActionRecord, error) {
	return nil, nil
}

// recordingApplier captures every fan-out call so tests can assert
// the dispatch happened.
type recordingApplier struct {
	holders         int32
	snapshotHolders int32
	failApplies     bool

	applied     []corpaction.ActionRow
	snapshotted []corpaction.ActionRow
}

func (r *recordingApplier) ApplyAction(_ context.Context, row corpaction.ActionRow) (corpaction.FanoutCounts, error) {
	if r.failApplies {
		return corpaction.FanoutCounts{}, errors.New("synthetic apply failure")
	}
	r.applied = append(r.applied, row)
	return corpaction.FanoutCounts{Holders: r.holders}, nil
}
func (r *recordingApplier) SnapshotDividendHolders(_ context.Context, row corpaction.ActionRow) (int32, error) {
	r.snapshotted = append(r.snapshotted, row)
	return r.snapshotHolders, nil
}

func newReactorEnv(_ *testing.T) (*es.Handler[*corpaction.CorporateAction], *fakeReader, *recordingApplier, *corpaction.Reactor, context.Context) {
	registry := es.NewRegistry()
	corpaction.RegisterEvents(registry)
	store := memstore.New()
	handler := es.NewHandler(store, registry, func(id string) *corpaction.CorporateAction {
		return corpaction.NewCorporateAction(id)
	}, slog.Default())
	reader := newFakeReader()
	applier := &recordingApplier{holders: 3}
	r := corpaction.New(handler, reader, applier, time.Now,
		corpaction.Config{Interval: time.Second}, slog.Default())
	return handler, reader, applier, r, context.Background()
}

func declareInStore(t *testing.T, h *es.Handler[*corpaction.CorporateAction], cmd corpaction.Declare) {
	t.Helper()
	require.NoError(t, h.Handle(context.Background(), cmd, func(a *corpaction.CorporateAction) ([]es.Event, error) {
		return corpaction.ExecuteDeclare(a, cmd)
	}))
}

func TestReactor_NoDueActions_NoOp(t *testing.T) {
	_, _, _, r, ctx := newReactorEnv(t)
	require.NoError(t, r.Tick(ctx, time.Now()))
	s := r.Status()
	assert.Equal(t, 0, s.LastTickApplied)
	assert.Equal(t, 0, s.LastTickSnapshotted)
}

func TestReactor_AppliesSplitOnEffectiveDate(t *testing.T) {
	handler, reader, applier, r, ctx := newReactorEnv(t)
	past := time.Now().Add(-time.Hour)
	declareInStore(t, handler, corpaction.Declare{
		ActionID:         "split-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    past,
	})
	reader.add(corpaction.ActionRow{
		ActionID:         "split-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    past,
	})

	require.NoError(t, r.Tick(ctx, time.Now()))

	assert.Len(t, applier.applied, 1, "applier called once")
	a, err := handler.Load(ctx, corpaction.AggregateID("split-1"))
	require.NoError(t, err)
	assert.Equal(t, corpaction.Applied, a.Status)
	assert.Equal(t, int32(3), a.HoldersCount, "holders count propagated from applier")
}

func TestReactor_SkipsFutureActions(t *testing.T) {
	handler, reader, applier, r, ctx := newReactorEnv(t)
	future := time.Now().Add(time.Hour)
	declareInStore(t, handler, corpaction.Declare{
		ActionID:         "split-2",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    future,
	})
	reader.add(corpaction.ActionRow{
		ActionID:         "split-2",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    future,
	})

	require.NoError(t, r.Tick(ctx, time.Now()))

	assert.Empty(t, applier.applied)
	a, err := handler.Load(ctx, corpaction.AggregateID("split-2"))
	require.NoError(t, err)
	assert.Equal(t, corpaction.Declared, a.Status, "future action stays declared")
}

func TestReactor_DividendSnapshotThenApply(t *testing.T) {
	handler, reader, applier, r, ctx := newReactorEnv(t)
	applier.snapshotHolders = 4
	past := time.Now().Add(-time.Hour)
	pay := past.Add(30 * time.Minute) // also in the past, but after record_date
	declareInStore(t, handler, corpaction.Declare{
		ActionID:         "div-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 2400,
		RecordDate:       past,
		PayDate:          pay,
	})
	reader.add(corpaction.ActionRow{
		ActionID:         "div-1",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 2400,
		RecordDate:       past,
		PayDate:          pay,
	})

	// First tick: snapshot taken, AND apply succeeds because the
	// snapshot lands before the apply scan in the same tick.
	require.NoError(t, r.Tick(ctx, time.Now()))

	assert.Len(t, applier.snapshotted, 1, "snapshot taken once")
	assert.Len(t, applier.applied, 1, "apply runs after snapshot lands in same tick")
	a, err := handler.Load(ctx, corpaction.AggregateID("div-1"))
	require.NoError(t, err)
	assert.True(t, a.DividendSnapshotted)
	assert.Equal(t, corpaction.Applied, a.Status)
}

func TestReactor_DividendApplyDeferredUntilSnapshot(t *testing.T) {
	// Construct a dividend where record_date is in the past but the
	// fake reader withholds the snapshot — apply must skip and the
	// action stays Declared.
	handler, reader, applier, r, ctx := newReactorEnv(t)
	past := time.Now().Add(-time.Hour)
	declareInStore(t, handler, corpaction.Declare{
		ActionID:         "div-2",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 1000,
		RecordDate:       past,
		PayDate:          past.Add(30 * time.Minute),
	})
	// Mark snapshot as already done in the fake reader so it doesn't
	// run, but DON'T record it on the aggregate. The apply path
	// loads the aggregate, sees DividendSnapshotted=false, and
	// skips.
	reader.snapshotted["div-2"] = true
	reader.add(corpaction.ActionRow{
		ActionID:         "div-2",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND,
		DividendPerShare: 1000,
		RecordDate:       past,
		PayDate:          past.Add(30 * time.Minute),
	})

	require.NoError(t, r.Tick(ctx, time.Now()))

	assert.Empty(t, applier.snapshotted, "snapshot skipped (already marked)")
	assert.Empty(t, applier.applied, "apply skipped because aggregate isn't snapshotted")
	a, err := handler.Load(ctx, corpaction.AggregateID("div-2"))
	require.NoError(t, err)
	assert.Equal(t, corpaction.Declared, a.Status, "stays declared until snapshot lands")
}

func TestReactor_ApplyFailure_StaysDeclared(t *testing.T) {
	handler, reader, applier, r, ctx := newReactorEnv(t)
	applier.failApplies = true
	past := time.Now().Add(-time.Hour)
	declareInStore(t, handler, corpaction.Declare{
		ActionID:         "split-fail",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    past,
	})
	reader.add(corpaction.ActionRow{
		ActionID:         "split-fail",
		Symbol:           "AAPL",
		Type:             corpactionv1.ActionType_ACTION_TYPE_SPLIT,
		SplitNumerator:   2,
		SplitDenominator: 1,
		EffectiveDate:    past,
	})

	err := r.Tick(ctx, time.Now())
	assert.Error(t, err, "tick surfaces aggregated error")

	a, err := handler.Load(ctx, corpaction.AggregateID("split-fail"))
	require.NoError(t, err)
	assert.Equal(t, corpaction.Declared, a.Status, "failed apply leaves action Declared for retry")
	assert.Equal(t, 1, r.Status().LastTickFailed)
}
