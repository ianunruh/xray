package corpaction

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// Clock is the indirection used so tests can drive the reactor at
// arbitrary virtual times. Production wires time.Now.
type Clock func() time.Time

// Config bundles tunables. Interval is the wakeup cadence.
type Config struct {
	Interval time.Duration
}

// ReactorStatus is a point-in-time snapshot of the reactor for
// diagnostics. LastTickAt is zero before the first tick completes.
// (Named ReactorStatus rather than Status to avoid colliding with
// the aggregate's lifecycle Status enum.)
type ReactorStatus struct {
	Interval         time.Duration
	LastTickAt       time.Time
	LastTickDuration time.Duration
	LastTickApplied  int
	LastTickSnapshotted int
	LastTickFailed   int
}

// Applier is the type-keyed fan-out hook. Phase 2 ships with a
// dispatch stub that returns "not implemented" for every type;
// phases 3-6 fill in splits, dividends, and renames behind this
// interface. Keeping it as an interface (rather than concrete
// methods on Reactor) lets tests stub the fan-out without spinning
// up portfolios / orderbooks / sagas.
type Applier interface {
	// ApplyAction runs the fan-out for one due action. Returns
	// counts to bundle into CorporateActionApplied. On error the
	// reactor leaves the action Declared so the next tick retries.
	ApplyAction(ctx context.Context, action ActionRow) (counts FanoutCounts, err error)
	// SnapshotDividendHolders writes the per-action record-date
	// snapshot. Idempotent at the projection level (PK collision
	// = ignore).
	SnapshotDividendHolders(ctx context.Context, action ActionRow) (holdersCount int32, err error)
}

// FanoutCounts is the per-action tally the reactor stamps onto
// CorporateActionApplied.
type FanoutCounts struct {
	Holders int32
	Orders  int32
	Sagas   int32
}

// Reactor drives Declared corporate actions toward Applied. Each
// tick scans the projection for due work, dispatches to the
// type-keyed Applier, and records the lifecycle event on the
// CorporateAction aggregate. Mirrors settlement.Reactor in shape so
// diagnostics surfaces it identically.
type Reactor struct {
	handler *es.Handler[*CorporateAction]
	reader  Reader
	applier Applier
	clock   Clock
	cfg     Config
	log     *slog.Logger

	mu     sync.Mutex
	status ReactorStatus
}

func New(
	handler *es.Handler[*CorporateAction],
	reader Reader,
	applier Applier,
	clock Clock,
	cfg Config,
	log *slog.Logger,
) *Reactor {
	if clock == nil {
		clock = time.Now
	}
	return &Reactor{
		handler: handler,
		reader:  reader,
		applier: applier,
		clock:   clock,
		cfg:     cfg,
		log:     log,
		status:  ReactorStatus{Interval: cfg.Interval},
	}
}

// Status returns a snapshot of the reactor's current configuration
// and last-tick stats. Safe to call from any goroutine.
func (r *Reactor) Status() ReactorStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

// Run starts a ticker loop that calls Tick at the configured
// Interval. Returns when ctx is done. Errors from individual ticks
// are logged but don't abort the loop.
func (r *Reactor) Run(ctx context.Context) {
	if r.cfg.Interval <= 0 {
		r.log.Warn("corpaction: interval <= 0, not running")
		return
	}
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Tick(ctx, r.clock()); err != nil {
				r.log.Error("corpaction: tick failed", "error", err)
			}
		}
	}
}

// Tick processes one cycle: take any due dividend snapshots, then
// apply any actions whose trigger date has passed.
func (r *Reactor) Tick(ctx context.Context, now time.Time) error {
	start := r.clock()
	var (
		snapshotted, applied, failed int
		errs                         []error
	)

	// 1. Dividend record-date snapshots first — they have to land
	//    before any same-cycle pay-date apply, in the (rare) case
	//    record_date == pay_date.
	dueSnaps, err := r.reader.DueDividendSnapshots(ctx, now)
	if err != nil {
		return fmt.Errorf("list due dividend snapshots: %w", err)
	}
	for _, action := range dueSnaps {
		if err := r.snapshotOne(ctx, action); err != nil {
			r.log.Error("corpaction: snapshot failed", "action_id", action.ActionID, "error", err)
			errs = append(errs, err)
			continue
		}
		snapshotted++
	}

	// 2. Apply any due actions (splits + renames on effective_date,
	//    dividends on pay_date).
	due, err := r.reader.DueActions(ctx, now)
	if err != nil {
		return fmt.Errorf("list due actions: %w", err)
	}
	for _, action := range due {
		ok, err := r.applyOne(ctx, action)
		if err != nil {
			r.log.Error("corpaction: apply failed", "action_id", action.ActionID, "error", err)
			errs = append(errs, err)
			failed++
			continue
		}
		if ok {
			applied++
		}
	}

	r.recordTick(start, snapshotted, applied, failed)
	return errors.Join(errs...)
}

func (r *Reactor) snapshotOne(ctx context.Context, action ActionRow) error {
	holders, err := r.applier.SnapshotDividendHolders(ctx, action)
	if err != nil {
		return err
	}
	cmd := RecordDividendSnapshot{
		ActionID:     action.ActionID,
		Symbol:       action.Symbol,
		HoldersCount: holders,
	}
	return r.handler.Handle(ctx, cmd, func(a *CorporateAction) ([]es.Event, error) {
		return ExecuteRecordDividendSnapshot(a, cmd)
	})
}

// applyOne returns ok=true when the action was successfully applied,
// ok=false when the action was skipped (e.g. dividend whose snapshot
// hasn't happened yet).
func (r *Reactor) applyOne(ctx context.Context, action ActionRow) (bool, error) {
	if action.Type == corpactionv1.ActionType_ACTION_TYPE_CASH_DIVIDEND {
		// Pay-date dispatch requires the record-date snapshot to be
		// in place — if it isn't, defer to the next tick (which will
		// take the snapshot first).
		a, err := r.handler.Load(ctx, AggregateID(action.ActionID))
		if err != nil {
			return false, fmt.Errorf("load action: %w", err)
		}
		if !a.DividendSnapshotted {
			return false, nil
		}
	}
	counts, err := r.applier.ApplyAction(ctx, action)
	if err != nil {
		return false, err
	}
	cmd := RecordApplied{
		ActionID:     action.ActionID,
		HoldersCount: counts.Holders,
		OrdersCount:  counts.Orders,
		SagasCount:   counts.Sagas,
	}
	if err := r.handler.Handle(ctx, cmd, func(a *CorporateAction) ([]es.Event, error) {
		return ExecuteRecordApplied(a, cmd)
	}); err != nil {
		return false, fmt.Errorf("record applied: %w", err)
	}
	return true, nil
}

func (r *Reactor) recordTick(start time.Time, snapshotted, applied, failed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.LastTickAt = start
	r.status.LastTickDuration = r.clock().Sub(start)
	r.status.LastTickSnapshotted = snapshotted
	r.status.LastTickApplied = applied
	r.status.LastTickFailed = failed
}

// NoopApplier is the placeholder Applier used in phase 2 — every
// fan-out is a no-op (returns zero counts, no error). Phases 3-6
// replace this with concrete applier implementations.
type NoopApplier struct{}

func (NoopApplier) ApplyAction(_ context.Context, _ ActionRow) (FanoutCounts, error) {
	return FanoutCounts{}, nil
}

func (NoopApplier) SnapshotDividendHolders(_ context.Context, _ ActionRow) (int32, error) {
	return 0, nil
}
