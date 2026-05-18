// Package feesaccruer periodically charges margin interest and short
// borrow fees against accounts with open liabilities. Runs as a
// background ticker; each tick walks the AccruableAccountsTracker,
// loads each portfolio, computes the per-cycle interest and per-symbol
// borrow fees, and issues one AccrueFees command per account.
//
// Idempotency: each portfolio carries a LastAccruedAt clock advanced
// by every accrual event. A duplicate tick with the same now value
// would compute the same amounts twice — replays don't re-charge
// because Apply moves LastAccruedAt forward and a later tick computes
// elapsed from that. (Catch-up after a missed tick is automatic for
// the same reason: a 3h gap charges 3h of interest, not 1h.)
package feesaccruer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ianunruh/xray/internal/margin"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

// Clock is the indirection used so tests can drive accrual at
// arbitrary virtual times. Production wires time.Now.
type Clock func() time.Time

// Config bundles tunables. Interval is the wakeup cadence; MinElapsed
// is the threshold below which a per-account accrual is skipped to
// avoid churn (defaults to Interval/2 when zero, so a half-missed
// tick can still fire).
type Config struct {
	Interval   time.Duration
	MinElapsed time.Duration
}

// Accruer applies financing charges. Construct with NewAccruer and
// either start a background ticker via Run or drive single cycles
// from tests via Tick.
type Accruer struct {
	handler  *es.Handler[*portfolio.Portfolio]
	accounts portfolio.AccruableAccountsTracker
	marker   portfolio.Marker
	clock    Clock
	cfg      Config
	log      *slog.Logger

	mu     sync.Mutex
	status Status
}

// Status is a point-in-time snapshot of the accruer for diagnostics.
// LastTickAt is zero before the first tick completes.
type Status struct {
	Interval         time.Duration
	MinElapsed       time.Duration
	LastTickAt       time.Time
	LastTickDuration time.Duration
	LastTickAccounts int
	LastTickFailed   int
}

func NewAccruer(
	handler *es.Handler[*portfolio.Portfolio],
	accounts portfolio.AccruableAccountsTracker,
	marker portfolio.Marker,
	clock Clock,
	cfg Config,
	log *slog.Logger,
) *Accruer {
	if clock == nil {
		clock = time.Now
	}
	if cfg.MinElapsed <= 0 {
		cfg.MinElapsed = cfg.Interval / 2
	}
	return &Accruer{
		handler:  handler,
		accounts: accounts,
		marker:   marker,
		clock:    clock,
		cfg:      cfg,
		log:      log,
		status: Status{
			Interval:   cfg.Interval,
			MinElapsed: cfg.MinElapsed,
		},
	}
}

// Status returns a snapshot of the accruer's current configuration
// and last-tick stats. Safe to call from any goroutine.
func (a *Accruer) Status() Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status
}

// Run starts a ticker loop that calls Tick at the configured
// Interval. Returns when ctx is done. Errors from individual ticks
// are logged but don't abort the loop — one bad account shouldn't
// stop the rest of the schedule.
func (a *Accruer) Run(ctx context.Context) {
	if a.cfg.Interval <= 0 {
		a.log.Warn("feesaccruer: interval <= 0, not running")
		return
	}
	t := time.NewTicker(a.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := a.Tick(ctx, a.clock()); err != nil {
				a.log.Error("feesaccruer: tick failed", "error", err)
			}
		}
	}
}

// Tick processes one accrual cycle at the given wall-clock time.
// Exported so tests can step through cycles deterministically.
func (a *Accruer) Tick(ctx context.Context, now time.Time) error {
	start := a.clock()
	ids, err := a.accounts.AccruableAccounts(ctx)
	if err != nil {
		return fmt.Errorf("list accruable accounts: %w", err)
	}
	var errs []error
	for _, accountID := range ids {
		if err := a.accrueOne(ctx, accountID, now); err != nil {
			a.log.Error("feesaccruer: account failed", "account_id", accountID, "error", err)
			errs = append(errs, err)
		}
	}
	a.recordTick(start, len(ids), len(errs))
	return errors.Join(errs...)
}

func (a *Accruer) recordTick(start time.Time, accounts, failed int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.status.LastTickAt = start
	a.status.LastTickDuration = a.clock().Sub(start)
	a.status.LastTickAccounts = accounts
	a.status.LastTickFailed = failed
}

func (a *Accruer) accrueOne(ctx context.Context, accountID string, now time.Time) error {
	p, err := a.handler.Load(ctx, portfolio.AggregateID(accountID))
	if err != nil {
		return fmt.Errorf("load portfolio %s: %w", accountID, err)
	}

	periodStart := p.LastAccruedAt
	if periodStart.IsZero() || !periodStart.Before(now) {
		// No prior clock seed (account has no events yet — impossible
		// since the tracker only includes accounts with events) or
		// the clock is already at-or-past now (test ran twice with
		// same now). Skip.
		return nil
	}
	elapsed := now.Sub(periodStart)
	if elapsed < a.cfg.MinElapsed {
		return nil
	}

	loanPrincipal := p.MarginLoan()
	interestAmount := margin.AccruedAmount(loanPrincipal, margin.MarginLoanRateBps, elapsed)

	var fees []portfolio.BorrowFeeEntry
	for symbol, short := range p.ShortPositions {
		if short.Quantity <= 0 {
			continue
		}
		mark, _, ok := a.marker.GetMarkPrice(symbol)
		if !ok {
			// No mark — can't price the borrow. Skip this symbol;
			// the next cycle will catch it once a trade prints.
			continue
		}
		notional := mark * short.Quantity
		amount := margin.AccruedAmount(notional, margin.ShortBorrowRateBps, elapsed)
		if amount <= 0 {
			continue
		}
		fees = append(fees, portfolio.BorrowFeeEntry{
			Symbol:    symbol,
			MarkPrice: mark,
			Qty:       short.Quantity,
			RateBps:   margin.ShortBorrowRateBps,
			Amount:    amount,
		})
	}

	// Truly idle account (no shorts, no loan). Skip — the aggregate
	// resets LastAccruedAt when a new liability appears, so this
	// dormant period doesn't end up retroactively billed.
	if loanPrincipal == 0 && len(p.ShortPositions) == 0 {
		return nil
	}

	cmd := portfolio.AccrueFees{
		AccountID:      accountID,
		PeriodStart:    periodStart,
		PeriodEnd:      now,
		LoanPrincipal:  loanPrincipal,
		LoanRateBps:    margin.MarginLoanRateBps,
		InterestAmount: interestAmount,
		BorrowFees:     fees,
	}
	if err := a.handler.Handle(ctx, cmd, func(p *portfolio.Portfolio) ([]es.Event, error) {
		return portfolio.ExecuteAccrueFees(p, cmd)
	}); err != nil {
		return fmt.Errorf("accrue fees %s: %w", accountID, err)
	}
	if a.log.Enabled(ctx, slog.LevelDebug) {
		a.log.Debug("feesaccruer: charged",
			"account_id", accountID,
			"interest", interestAmount,
			"fees", len(fees),
			"elapsed", elapsed)
	}
	return nil
}
