package mm

import (
	"context"
	"log/slog"
	"time"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/internal/pricesource"
	"github.com/ianunruh/xray/internal/trader"
)

type Engine struct {
	cfg      SymbolConfig
	strategy Strategy
	prices   pricesource.PriceSource
	tracker  *trader.OrderTracker
	pfClient portfoliov1connect.PortfolioServiceClient
	log      *slog.Logger

	lastRefPrice int64
}

func NewEngine(
	cfg SymbolConfig,
	strategy Strategy,
	prices pricesource.PriceSource,
	obClient orderbookv1connect.OrderBookServiceClient,
	pfClient portfoliov1connect.PortfolioServiceClient,
	log *slog.Logger,
) *Engine {
	engineLog := log.With("symbol", cfg.Symbol, "account", cfg.AccountID)
	return &Engine{
		cfg:      cfg,
		strategy: strategy,
		prices:   prices,
		tracker:  trader.NewOrderTracker(cfg.Symbol, obClient, pfClient, engineLog),
		pfClient: pfClient,
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

	e.requote(ctx)

	requoteTicker := time.NewTicker(e.cfg.RequoteInterval)
	defer requoteTicker.Stop()

	priceCheckTicker := time.NewTicker(1 * time.Second)
	defer priceCheckTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.tracker.DrainResolves()
			e.log.Info("shutting down, cancelling orders", "tracked_orders", len(e.tracker.Orders))
			e.tracker.CancelAll(context.Background())
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
	snap, ok := e.prices.GetPrice(e.cfg.Symbol)
	if !ok {
		e.log.Warn("no reference price available, skipping requote")
		return
	}
	if time.Since(snap.FetchedAt) > 5*time.Minute {
		e.log.Warn("reference price is stale, skipping requote", "fetched_at", snap.FetchedAt)
		return
	}

	e.tracker.CancelAll(ctx)

	portfolio := trader.GetPortfolio(ctx, e.pfClient, e.cfg.AccountID, e.log)
	position := trader.GetPosition(portfolio, e.cfg.Symbol)

	inv := InventoryState{
		Position:    position,
		MaxPosition: e.cfg.MaxPosition,
	}
	levels := e.strategy.ComputeQuotes(snap.Price, inv)

	if len(levels) == 0 {
		e.log.Info("no quotes to place")
		return
	}

	e.lastRefPrice = snap.Price

	e.log.Info("placing quotes",
		"ref_price", snap.Price,
		"position", position,
		"cash_available", portfolio.CashBalance-portfolio.CashHeld,
		"levels", len(levels))

	for _, level := range levels {
		e.tracker.PlaceOrder(ctx, e.cfg.AccountID, level.Side, level.Price, level.Quantity)
	}
}

func (e *Engine) handleFill(ctx context.Context, trade *orderbookv1.Trade) {
	if !e.tracker.IsOwnTrade(trade) {
		return
	}

	e.log.Info("fill detected",
		"trade_id", trade.TradeId,
		"price", trade.Price,
		"quantity", trade.Quantity)

	e.requote(ctx)
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
