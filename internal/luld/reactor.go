package luld

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

// ReferenceReader is the subset of the LULD reference projection the
// reactor consumes. Implemented by *orderbook.LULDReferenceProjection.
type ReferenceReader interface {
	GetReference(symbol string, now time.Time) (int64, bool)
}

// Reactor orchestrates the LULD volatility moderator. It updates
// per-symbol price bands on the orderbook aggregate as the rolling
// reference moves, and drives the limit-state → continuous|halted
// transition once the limit-state grace expires (via the reconciler
// invoking EvaluateLULDExpiry).
//
// The trip itself (LULDLimitStateEntered + aggressor cancel) happens
// inside the orderbook matcher; this reactor never trips. The halt →
// reopen-auction transition is wired in step 6.
type Reactor struct {
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	reference        ReferenceReader
	active           ActiveSymbolsTracker
	tiers            Tiers
	cfg              Config
	log              *slog.Logger
}

// NewReactor builds a reactor with sensible defaults applied to zero-
// valued config fields.
func NewReactor(
	obHandler *es.Handler[*orderbook.OrderBook],
	reference ReferenceReader,
	active ActiveSymbolsTracker,
	tiers Tiers,
	cfg Config,
	log *slog.Logger,
) *Reactor {
	if cfg.LimitStateGrace == 0 {
		cfg.LimitStateGrace = orderbook.LULDLimitStateGrace
	}
	if cfg.HaltDuration == 0 {
		cfg.HaltDuration = 5 * time.Minute
	}
	return &Reactor{
		orderbookHandler: obHandler,
		reference:        reference,
		active:           active,
		tiers:            tiers,
		cfg:              cfg,
		log:              log,
	}
}

// Status snapshots the reactor for diagnostics. The active count is
// live from the tracker; the rest is static config.
type Status struct {
	LimitStateGrace time.Duration
	HaltDuration    time.Duration
	ActiveCount     int
}

func (r *Reactor) Status(ctx context.Context) Status {
	count := 0
	if r.active != nil {
		count = len(r.active.ListActiveSymbols(ctx))
	}
	return Status{
		LimitStateGrace: r.cfg.LimitStateGrace,
		HaltDuration:    r.cfg.HaltDuration,
		ActiveCount:     count,
	}
}

func (r *Reactor) HandleEvents(ctx context.Context, events []es.Event) error {
	var errs []error
	for _, evt := range events {
		if err := r.handleOne(ctx, evt); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *Reactor) handleOne(ctx context.Context, evt es.Event) error {
	ctx = es.WithCausation(ctx, evt)
	switch data := evt.Data.(type) {
	case *orderbookv1.TradeExecuted:
		// Only continuous prints contribute to the reference window,
		// and only they cause us to recompute bands.
		if data.CrossType != orderbookv1.CrossType_CROSS_TYPE_NONE {
			return nil
		}
		return r.maybeUpdateBands(ctx, data.Symbol, data.ExecutedAt.AsTime(), "reference_update")
	case *orderbookv1.AuctionUncrossed:
		// A halt-reopen cross re-anchors the bands; the reference
		// projection has cleared its window on TradingHalted, so the
		// fresh reference is the cross price.
		if data.CrossType == orderbookv1.CrossType_CROSS_TYPE_HALT_REOPEN {
			return r.setBands(ctx, data.Symbol, data.ClearingPrice, "halt_reopen")
		}
	}
	return nil
}

// maybeUpdateBands pulls the latest reference from the projection and
// stamps it onto the aggregate via SetLULDBands. The command's executor
// is no-op when the bands haven't shifted, so calling this every trade
// is cheap.
func (r *Reactor) maybeUpdateBands(ctx context.Context, symbol string, now time.Time, reason string) error {
	ref, ok := r.reference.GetReference(symbol, now)
	if !ok {
		return nil
	}
	return r.setBands(ctx, symbol, ref, reason)
}

func (r *Reactor) setBands(ctx context.Context, symbol string, reference int64, reason string) error {
	bps := r.tiers.BandBps(symbol)
	lower, upper := ComputeBands(reference, bps)
	if upper == 0 {
		return nil
	}
	cmd := orderbook.SetLULDBands{
		Symbol:         symbol,
		ReferencePrice: reference,
		UpperBand:      upper,
		LowerBand:      lower,
		BandBps:        bps,
		Reason:         reason,
	}
	if err := r.orderbookHandler.Handle(ctx, cmd, func(b *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteSetLULDBands(b, cmd)
	}); err != nil {
		return fmt.Errorf("set LULD bands for %s: %w", symbol, err)
	}
	return nil
}

// EvaluateLULDExpiry is called by the reconciler for each symbol in
// the active set. It looks at the symbol's current orderbook phase
// and, if the relevant timer has passed, drives the next transition:
//
//   - PhaseLimitState past LULDHaltDeadline: either ResumeFromLULDLimitState
//     (when the top of book has returned inside the bands) or
//     EscalateLULDToHalt (otherwise).
//   - PhaseHalted past LULDReopenAt: opens a reopening auction.
//     Wired in step 6; currently logs a warning and returns.
//
// Idempotent: no-op when a timer hasn't expired, when the phase
// doesn't match the watch-set kind, or when another writer has
// already advanced the state.
func (r *Reactor) EvaluateLULDExpiry(ctx context.Context, symbol string, now time.Time) error {
	book, err := r.orderbookHandler.Load(ctx, orderbook.AggregateID(symbol))
	if err != nil {
		return fmt.Errorf("load orderbook %s: %w", symbol, err)
	}
	switch book.Phase {
	case orderbook.PhaseLimitState:
		if book.LULDHaltDeadline.IsZero() || now.Before(book.LULDHaltDeadline) {
			return nil
		}
		if book.LULDSpreadInBand() {
			cmd := orderbook.ResumeFromLULDLimitState{Symbol: symbol}
			if err := r.orderbookHandler.Handle(ctx, cmd, func(b *orderbook.OrderBook) ([]es.Event, error) {
				return orderbook.ExecuteResumeFromLULDLimitState(b, cmd)
			}); err != nil {
				return fmt.Errorf("resume from limit state %s: %w", symbol, err)
			}
			r.log.Info("luld: resumed from limit state", "symbol", symbol)
			return nil
		}
		cmd := orderbook.EscalateLULDToHalt{
			Symbol:   symbol,
			ReopenAt: now.Add(r.cfg.HaltDuration),
		}
		if err := r.orderbookHandler.Handle(ctx, cmd, func(b *orderbook.OrderBook) ([]es.Event, error) {
			return orderbook.ExecuteEscalateLULDToHalt(b, cmd)
		}); err != nil {
			return fmt.Errorf("escalate to halt %s: %w", symbol, err)
		}
		r.log.Warn("luld: escalated to halt",
			"symbol", symbol,
			"halt_duration", r.cfg.HaltDuration,
			"reopen_at", cmd.ReopenAt)
		return nil

	case orderbook.PhaseHalted:
		// The halt → reopen-auction transition lands in step 6 (needs
		// ExecuteOpenAuction to accept PhaseHalted as a source). For
		// now, just log if we're past the reopen deadline so the
		// reconciler doesn't sit silent.
		if !book.LULDReopenAt.IsZero() && now.After(book.LULDReopenAt) {
			r.log.Warn("luld: halt reopen pending wiring", "symbol", symbol, "reopen_at", book.LULDReopenAt)
		}
		return nil
	}
	return nil
}
