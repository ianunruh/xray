package mm

import (
	"context"
	"log/slog"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/gen/saga/v1/sagav1connect"
	"github.com/ianunruh/xray/internal/pricesource"
	"github.com/ianunruh/xray/internal/trader"
)

type Engine struct {
	cfg      SymbolConfig
	strategy Strategy
	prices   pricesource.PriceSource
	tracker  *trader.OrderTracker
	pfClient portfoliov1connect.PortfolioServiceClient
	phase    *trader.PhaseWatcher
	log      *slog.Logger

	lastRefPrice int64
}

func NewEngine(
	cfg SymbolConfig,
	strategy Strategy,
	prices pricesource.PriceSource,
	obClient orderbookv1connect.OrderBookServiceClient,
	pfClient portfoliov1connect.PortfolioServiceClient,
	sagaClient sagav1connect.SagaServiceClient,
	log *slog.Logger,
) *Engine {
	engineLog := log.With("symbol", cfg.Symbol, "account", cfg.AccountID)
	return &Engine{
		cfg:      cfg,
		strategy: strategy,
		prices:   prices,
		tracker:  trader.NewOrderTracker(cfg.Symbol, obClient, sagaClient, engineLog),
		pfClient: pfClient,
		phase:    trader.NewPhaseWatcher(cfg.Symbol),
		log:      engineLog,
	}
}

func (e *Engine) Run(ctx context.Context) error {
	trader.Bootstrap(ctx, trader.BootstrapConfig{
		AccountID:      e.cfg.AccountID,
		Symbol:         e.cfg.Symbol,
		InitialDeposit: e.cfg.InitialDeposit,
		InitialShares:  e.cfg.InitialShares,
	}, e.prices, e.pfClient, e.log)

	e.tracker.CleanupOrphans(ctx, e.cfg.AccountID)

	fillCh := make(chan *orderbookv1.Trade, 64)
	go trader.StreamTrades(ctx, e.tracker.ObClient, e.cfg.Symbol, fillCh, e.log)
	go e.phase.Watch(ctx, e.tracker.ObClient, 5*time.Second, e.log)

	e.requote(ctx)

	requoteTicker := time.NewTicker(e.cfg.RequoteInterval)
	defer requoteTicker.Stop()

	priceCheckTicker := time.NewTicker(1 * time.Second)
	defer priceCheckTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.tracker.Shutdown()
			return ctx.Err()

		case <-requoteTicker.C:
			if ctx.Err() != nil {
				continue
			}
			e.requote(ctx)

		case <-priceCheckTicker.C:
			if ctx.Err() != nil {
				continue
			}
			e.checkPriceMove(ctx)

		case trade, ok := <-fillCh:
			if !ok {
				fillCh = make(chan *orderbookv1.Trade, 64)
				go trader.StreamTrades(ctx, e.tracker.ObClient, e.cfg.Symbol, fillCh, e.log)
				continue
			}
			if ctx.Err() != nil {
				continue
			}
			e.handleFill(ctx, trade)

		case res := <-e.tracker.ResolveCh:
			e.tracker.HandleResolve(res)
		}
	}
}

func (e *Engine) requote(ctx context.Context) {
	// The MM is a continuous-matching strategy. During an auction the
	// matching engine doesn't cross, so leaving stale quotes outstanding
	// (and re-quoting against an indicative price) wastes RPCs and
	// confuses the imbalance picture. After CLOSED we have nothing to do
	// until the next session opens.
	if !e.phase.IsContinuous() {
		e.log.Debug("market phase is not CONTINUOUS, skipping requote", "phase", e.phase.Phase().String())
		return
	}
	snap, ok := e.prices.GetPrice(e.cfg.Symbol)
	if !ok {
		e.log.Warn("no reference price available, skipping requote")
		return
	}
	if time.Since(snap.FetchedAt) > 5*time.Minute {
		e.log.Warn("reference price is stale, skipping requote", "fetched_at", snap.FetchedAt)
		return
	}

	portfolio := trader.GetPortfolio(ctx, e.pfClient, e.cfg.AccountID, e.log)
	position := trader.GetPosition(portfolio, e.cfg.Symbol)

	inv := InventoryState{
		Position:    position,
		MaxPosition: e.cfg.MaxPosition,
	}
	levels := e.strategy.ComputeQuotes(snap.Price, inv)

	e.lastRefPrice = snap.Price

	e.log.Info("requoting",
		"ref_price", snap.Price,
		"position", position,
		"cash_available", portfolio.CashBalance-portfolio.CashHeld,
		"levels", len(levels))

	var newBids, newAsks []QuoteLevel
	for _, level := range levels {
		if level.Side == orderbookv1.Side_SIDE_BUY {
			newBids = append(newBids, level)
		} else {
			newAsks = append(newAsks, level)
		}
	}

	oldBids := e.tracker.OrdersBySide(orderbookv1.Side_SIDE_BUY)
	oldAsks := e.tracker.OrdersBySide(orderbookv1.Side_SIDE_SELL)

	e.replaceOrders(ctx, oldBids, newBids)
	e.replaceOrders(ctx, oldAsks, newAsks)
}

func (e *Engine) replaceOrders(ctx context.Context, oldSagaIDs []string, newLevels []QuoteLevel) {
	// First, pair up old orders whose (price, qty) already matches a new
	// level — leave those resting untouched. Only the remainder need an
	// RPC.
	keptOld := make(map[string]bool, len(oldSagaIDs))
	keptNew := make(map[int]bool, len(newLevels))
	for i, level := range newLevels {
		for _, sagaID := range oldSagaIDs {
			if keptOld[sagaID] {
				continue
			}
			tracked, ok := e.tracker.Orders[sagaID]
			if !ok {
				continue
			}
			if tracked.Price == level.Price && tracked.Qty == level.Quantity {
				keptOld[sagaID] = true
				keptNew[i] = true
				break
			}
		}
	}

	remainingOld := make([]string, 0, len(oldSagaIDs))
	for _, sagaID := range oldSagaIDs {
		if !keptOld[sagaID] {
			remainingOld = append(remainingOld, sagaID)
		}
	}
	remainingNew := make([]QuoteLevel, 0, len(newLevels))
	for i, level := range newLevels {
		if !keptNew[i] {
			remainingNew = append(remainingNew, level)
		}
	}

	matched := min(len(remainingOld), len(remainingNew))

	for i := 0; i < matched; i++ {
		e.tracker.ReplaceOrder(ctx, e.cfg.AccountID, remainingOld[i], remainingNew[i].Side, remainingNew[i].Price, remainingNew[i].Quantity)
	}

	for i := matched; i < len(remainingOld); i++ {
		e.tracker.CancelTracked(ctx, remainingOld[i])
	}

	for i := matched; i < len(remainingNew); i++ {
		e.tracker.PlaceOrder(ctx, e.cfg.AccountID, remainingNew[i].Side, remainingNew[i].Price, remainingNew[i].Quantity)
	}
}

func (e *Engine) handleFill(ctx context.Context, trade *orderbookv1.Trade) {
	if e.tracker.RecognizeFill(trade) {
		e.requote(ctx)
	}
}

func (e *Engine) checkPriceMove(ctx context.Context) {
	if e.lastRefPrice == 0 || e.cfg.PriceMoveThreshold == 0 {
		return
	}
	snap, ok := e.prices.GetPrice(e.cfg.Symbol)
	if !ok {
		return
	}
	delta := snap.Price - e.lastRefPrice
	if delta < 0 {
		delta = -delta
	}
	if delta >= e.cfg.PriceMoveThreshold {
		e.log.Info("reference price moved beyond threshold, requoting",
			"old_price", e.lastRefPrice,
			"new_price", snap.Price,
			"delta", delta)
		e.requote(ctx)
	}
}
