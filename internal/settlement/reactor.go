// Package settlement runs the T+1 settlement reactor: a background
// ticker that scans the pending-settlements projection for legs whose
// settles_at has passed and issues ClearSettlement against the
// portfolio aggregate for each one. Mirrors the feesaccruer pattern
// — same Run / Tick / Status shape so diagnostics surface it the
// same way and tests can drive single cycles.
//
// Idempotency: ClearSettlement is a no-op when the leg is already
// gone (PendingLegs lookup misses), so duplicate ticks and event
// replays cost nothing.
package settlement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

// Clock is the indirection used so tests can drive the reactor at
// arbitrary virtual times. Production wires time.Now.
type Clock func() time.Time

// Config bundles tunables. Interval is the wakeup cadence.
type Config struct {
	Interval time.Duration
}

// Reactor clears due settlement legs. Construct with New and either
// start a background ticker via Run or drive single cycles from tests
// via Tick.
type Reactor struct {
	handler *es.Handler[*portfolio.Portfolio]
	reader  portfolio.PendingSettlementsReader
	clock   Clock
	cfg     Config
	log     *slog.Logger

	mu     sync.Mutex
	status Status
}

// Status is a point-in-time snapshot for diagnostics. LastTickAt is
// zero before the first tick completes.
type Status struct {
	Interval         time.Duration
	LastTickAt       time.Time
	LastTickDuration time.Duration
	LastTickAccounts int
	LastTickCleared  int
	LastTickFailed   int
}

func New(
	handler *es.Handler[*portfolio.Portfolio],
	reader portfolio.PendingSettlementsReader,
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
		clock:   clock,
		cfg:     cfg,
		log:     log,
		status:  Status{Interval: cfg.Interval},
	}
}

// Status returns a snapshot of the reactor's current configuration
// and last-tick stats. Safe to call from any goroutine.
func (r *Reactor) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status
}

// Run starts a ticker loop that calls Tick at the configured
// Interval. Returns when ctx is done. Errors from individual ticks
// are logged but don't abort the loop — one bad account shouldn't
// stop the rest of the schedule.
func (r *Reactor) Run(ctx context.Context) {
	if r.cfg.Interval <= 0 {
		r.log.Warn("settlement: interval <= 0, not running")
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
				r.log.Error("settlement: tick failed", "error", err)
			}
		}
	}
}

// Tick processes one settlement cycle at the given wall-clock time.
// Exported so tests can step through cycles deterministically.
func (r *Reactor) Tick(ctx context.Context, now time.Time) error {
	start := r.clock()
	accounts, err := r.reader.AccountsWithDueSettlements(ctx, now)
	if err != nil {
		return fmt.Errorf("list due accounts: %w", err)
	}
	var (
		cleared int
		errs    []error
	)
	for _, accountID := range accounts {
		n, err := r.clearAccount(ctx, accountID, now)
		cleared += n
		if err != nil {
			r.log.Error("settlement: account failed", "account_id", accountID, "error", err)
			errs = append(errs, err)
		}
	}
	r.recordTick(start, len(accounts), cleared, len(errs))
	return errors.Join(errs...)
}

func (r *Reactor) clearAccount(ctx context.Context, accountID string, now time.Time) (int, error) {
	legs, err := r.reader.DueLegs(ctx, accountID, now)
	if err != nil {
		return 0, fmt.Errorf("list legs for %s: %w", accountID, err)
	}
	cleared := 0
	for _, leg := range legs {
		cmd := portfolio.ClearSettlement{
			AccountID: leg.AccountID,
			TradeID:   leg.TradeID,
			Kind:      leg.Kind,
		}
		err := r.handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
			return portfolio.ExecuteClearSettlement(p, cmd)
		})
		if err != nil {
			// One leg failing shouldn't stop the others. The projection
			// row stays, so the next tick will retry.
			r.log.Error("settlement: clear failed", "account_id", accountID, "trade_id", leg.TradeID, "kind", leg.Kind, "error", err)
			continue
		}
		cleared++
	}
	return cleared, nil
}

func (r *Reactor) recordTick(start time.Time, accounts, cleared, failed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.LastTickAt = start
	r.status.LastTickDuration = r.clock().Sub(start)
	r.status.LastTickAccounts = accounts
	r.status.LastTickCleared = cleared
	r.status.LastTickFailed = failed
}
