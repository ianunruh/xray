// Package reconciler periodically reconciles each saga reactor against
// its source-of-truth aggregates. Two failure modes are addressed:
//
//   - Stuck state machines (L1): if an action emitted SagaActionFailed
//     and the followup retry never fired (e.g., process restart), the
//     saga sits in an intermediate state. Periodic retry walks every
//     active saga and re-dispatches based on current state.
//
//   - Lost trade settlements (L2): if a TradeExecuted was processed by
//     the reactor but the settle command failed and the event was acked
//     anyway, the trade is in projection_trades but missing from
//     Portfolio.SettledTrades. Cross-reference and replay.
//
// Everything here is idempotent on top of the per-saga and per-trade
// idempotency the reactors already rely on, so running multiple
// reconcilers concurrently is safe.
package reconciler

import (
	"context"
	"log/slog"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/margincall"
	"github.com/ianunruh/xray/internal/ocosaga"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/internal/sagasvc"
	"github.com/ianunruh/xray/internal/twapsaga"
	"github.com/ianunruh/xray/pkg/es"
)

// TradeLookup queries the trade projection for trades involving a given
// order ID, used to find unsettled fills.
type TradeLookup interface {
	TradesByOrderID(ctx context.Context, orderID string) ([]*orderbookv1.TradeExecuted, error)
}

// SagaLookup enumerates sagas matching status filters; the unified
// projection from sagasvc satisfies this.
type SagaLookup interface {
	List(ctx context.Context, accountID, symbol string, kind sagav1.SagaKind, status sagav1.SagaStatus) ([]*sagasvc.SagaRow, error)
}

type Reconciler struct {
	interval         time.Duration
	sagaLookup       SagaLookup
	tradeLookup      TradeLookup
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	orderSagaReactor *ordersaga.Reactor
	bracketReactor   *bracket.Reactor
	ocoSagaReactor   *ocosaga.Reactor
	twapReactor      *twapsaga.Reactor
	marginReactor    *margincall.Reactor
	activeCalls      portfolio.ActiveMarginCallsTracker
	now              func() time.Time
	log              *slog.Logger
}

func New(
	interval time.Duration,
	sagaLookup SagaLookup,
	tradeLookup TradeLookup,
	portfolioHandler *es.Handler[*portfolio.Portfolio],
	orderSagaReactor *ordersaga.Reactor,
	bracketReactor *bracket.Reactor,
	ocoSagaReactor *ocosaga.Reactor,
	twapReactor *twapsaga.Reactor,
	marginReactor *margincall.Reactor,
	activeCalls portfolio.ActiveMarginCallsTracker,
	log *slog.Logger,
) *Reconciler {
	return &Reconciler{
		interval:         interval,
		sagaLookup:       sagaLookup,
		tradeLookup:      tradeLookup,
		portfolioHandler: portfolioHandler,
		orderSagaReactor: orderSagaReactor,
		bracketReactor:   bracketReactor,
		ocoSagaReactor:   ocoSagaReactor,
		twapReactor:      twapReactor,
		marginReactor:    marginReactor,
		activeCalls:      activeCalls,
		now:              time.Now,
		log:              log,
	}
}

// Run loops until ctx is cancelled, calling ReconcileOnce on each tick.
// Errors are logged and don't abort the loop.
func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.ReconcileOnce(ctx); err != nil {
				r.log.Error("reconciler pass failed", "error", err)
			}
		}
	}
}

// ReconcileOnce makes a single sweep: reconcile every active saga,
// then evaluate every open margin call for grace expiry. Errors are
// logged per-item so one bad saga doesn't block the rest of the pass.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	sagas, err := r.sagaLookup.List(ctx, "", "",
		sagav1.SagaKind_SAGA_KIND_UNSPECIFIED,
		sagav1.SagaStatus_SAGA_STATUS_ACTIVE)
	if err != nil {
		return err
	}
	for _, s := range sagas {
		if err := r.reconcileSaga(ctx, s); err != nil {
			r.log.Warn("reconcile saga failed",
				"saga_id", s.SagaID, "kind", s.Kind, "error", err)
		}
	}
	r.reconcileMarginCalls(ctx)
	return nil
}

func (r *Reconciler) reconcileMarginCalls(ctx context.Context) {
	if r.marginReactor == nil || r.activeCalls == nil {
		return
	}
	now := r.now()
	for _, call := range r.activeCalls.ListOpenCalls(ctx) {
		if err := r.marginReactor.EvaluateGraceExpiry(ctx, call.AccountID, now); err != nil {
			r.log.Warn("reconcile margin call failed",
				"account_id", call.AccountID, "call_id", call.CallID, "error", err)
		}
	}
}

func (r *Reconciler) reconcileSaga(ctx context.Context, s *sagasvc.SagaRow) error {
	// Fresh correlation per reconciliation pass. The original chain that
	// kicked off this saga is lost (we don't persist correlations on
	// saga rows yet), so any events the reconciler emits start a new chain
	// tagged by saga ID in logs.
	//
	// Debug-level on purpose: this fires once per active saga per tick
	// even when the reactor's retry is a no-op (e.g., a limit order
	// resting on the book). The reactor itself logs at INFO whenever it
	// actually transitions state, so DEBUG here keeps the heartbeat
	// available without spamming a quiet system.
	ctx, correlationID := es.NewCorrelation(ctx)
	r.log.Debug("reconcile saga", "saga_id", s.SagaID, "kind", s.Kind, "correlation_id", correlationID)
	switch s.Kind {
	case sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER:
		return r.reconcileOrderSaga(ctx, s)
	case sagav1.SagaKind_SAGA_KIND_BRACKET:
		return r.reconcileBracket(ctx, s)
	case sagav1.SagaKind_SAGA_KIND_OCO:
		return r.reconcileOCO(ctx, s)
	case sagav1.SagaKind_SAGA_KIND_TWAP:
		return r.reconcileTWAP(ctx, s)
	}
	return nil
}

func (r *Reconciler) reconcileTWAP(ctx context.Context, s *sagasvc.SagaRow) error {
	// TWAP slice scheduling is the primary purpose of the reconciler
	// tick for this kind: if a slice was due during a window when no
	// child event fired (e.g., process restart), this catches it up.
	// Children settle their own trades via the per-slice ordersaga
	// reactor, so no L2 trade replay is needed at this layer.
	return r.twapReactor.Reconcile(ctx, s.SagaID)
}

func (r *Reconciler) reconcileOCO(ctx context.Context, s *sagasvc.SagaRow) error {
	for _, orderID := range []string{
		ocosaga.TakeProfitOrderID(s.SagaID),
		ocosaga.StopLossOrderID(s.SagaID),
	} {
		if err := r.replayMissingTrades(ctx, s, orderID, r.ocoSagaReactor.ReplayTrade); err != nil {
			return err
		}
	}
	return r.ocoSagaReactor.Reconcile(ctx, s.SagaID)
}

func (r *Reconciler) reconcileOrderSaga(ctx context.Context, s *sagasvc.SagaRow) error {
	// L2 first: replay any unsettled trades before driving the state
	// machine. The state machine's "complete?" check looks at FilledQty
	// which only advances after a successful RecordFill, so settling
	// stale trades unblocks completion.
	orderID := ordersaga.OrderID(s.SagaID)
	if err := r.replayMissingTrades(ctx, s, orderID, r.orderSagaReactor.ReplayTrade); err != nil {
		return err
	}
	// L1: drive the state machine forward.
	return r.orderSagaReactor.Reconcile(ctx, s.SagaID)
}

func (r *Reconciler) reconcileBracket(ctx context.Context, s *sagasvc.SagaRow) error {
	// Brackets don't settle trades themselves anymore — the exit OCO
	// saga does. So no L2 (trade replay) at the bracket level; the
	// child OCO saga gets reconciled separately via SAGA_KIND_OCO.
	// L1: drive the bracket state machine forward.
	return r.bracketReactor.Reconcile(ctx, s.SagaID)
}

// replayMissingTrades pulls every trade against the given order ID,
// checks each against Portfolio.SettledTrades, and pushes any unsettled
// ones back through the reactor's trade handler.
func (r *Reconciler) replayMissingTrades(
	ctx context.Context,
	s *sagasvc.SagaRow,
	orderID string,
	replay func(context.Context, *orderbookv1.TradeExecuted) error,
) error {
	trades, err := r.tradeLookup.TradesByOrderID(ctx, orderID)
	if err != nil {
		return err
	}
	if len(trades) == 0 {
		return nil
	}
	p, err := r.portfolioHandler.Load(ctx, portfolio.AggregateID(s.AccountID))
	if err != nil {
		return err
	}
	for _, t := range trades {
		if p.HasSettled(s.SagaID, t.TradeId) {
			continue
		}
		r.log.Info("reconciler: replaying unsettled trade",
			"saga_id", s.SagaID, "trade_id", t.TradeId, "order_id", orderID)
		if err := replay(ctx, t); err != nil {
			r.log.Warn("reconciler: replay trade failed",
				"saga_id", s.SagaID, "trade_id", t.TradeId, "error", err)
		}
	}
	return nil
}
