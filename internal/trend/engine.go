package trend

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
	strategy *Strategy
	state    *EMAState
	prices   pricesource.PriceSource
	tracker  *trader.OrderTracker
	pfClient portfoliov1connect.PortfolioServiceClient
	log      *slog.Logger
}

func NewEngine(
	cfg SymbolConfig,
	strategy *Strategy,
	prices pricesource.PriceSource,
	obClient orderbookv1connect.OrderBookServiceClient,
	pfClient portfoliov1connect.PortfolioServiceClient,
	sagaClient sagav1connect.SagaServiceClient,
	log *slog.Logger,
) *Engine {
	log = log.With("symbol", cfg.Symbol, "account", cfg.AccountID)
	return &Engine{
		cfg:      cfg,
		strategy: strategy,
		state:    &EMAState{},
		prices:   prices,
		tracker:  trader.NewOrderTracker(cfg.Symbol, obClient, sagaClient, log),
		pfClient: pfClient,
		log:      log,
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

	tradeCh := make(chan *orderbookv1.Trade, 64)
	go trader.StreamTrades(ctx, e.tracker.ObClient, e.cfg.Symbol, tradeCh, e.log)

	expireTicker := time.NewTicker(5 * time.Second)
	defer expireTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.tracker.DrainResolves()
			e.log.Info("shutting down, cancelling orders", "tracked_orders", len(e.tracker.Orders))
			e.tracker.CancelAll(context.Background())
			return ctx.Err()

		case t, ok := <-tradeCh:
			if !ok {
				tradeCh = make(chan *orderbookv1.Trade, 64)
				go trader.StreamTrades(ctx, e.tracker.ObClient, e.cfg.Symbol, tradeCh, e.log)
				continue
			}
			if ctx.Err() != nil {
				continue
			}
			e.handleTrade(ctx, t)

		case res := <-e.tracker.ResolveCh:
			e.tracker.HandleResolve(res)

		case <-expireTicker.C:
			if ctx.Err() != nil {
				continue
			}
			e.tracker.ExpireStaleOrders(ctx, e.cfg.OrderTimeout)
		}
	}
}

func (e *Engine) handleTrade(ctx context.Context, trade *orderbookv1.Trade) {
	if e.tracker.IsOwnTrade(trade) {
		e.log.Info("fill detected",
			"trade_id", trade.TradeId,
			"price", trade.Price,
			"quantity", trade.Quantity)
		e.tracker.RemoveFilledOrder(trade)
	}

	signal := e.strategy.Update(e.state, trade.Price)

	if !e.state.Primed {
		e.log.Debug("warming up EMAs",
			"trade_count", e.state.TradeCount,
			"remaining", e.strategy.SlowPeriod-e.state.TradeCount)
		return
	}

	if signal == SignalNone {
		return
	}

	e.log.Info("signal detected",
		"signal", signalName(signal),
		"fast_ema", e.state.FastEMA,
		"slow_ema", e.state.SlowEMA)

	e.actOnSignal(ctx, signal, trade.Price)
}

func (e *Engine) actOnSignal(ctx context.Context, signal Signal, currentPrice int64) {
	e.tracker.CancelAll(ctx)

	portfolio := trader.GetPortfolio(ctx, e.pfClient, e.cfg.AccountID, e.log)
	position := trader.GetPosition(portfolio, e.cfg.Symbol)

	var targetPosition int64
	switch signal {
	case SignalBuy:
		targetPosition = e.cfg.MaxPosition
	case SignalSell:
		targetPosition = 0
	}

	delta := targetPosition - position
	if delta == 0 {
		e.log.Info("already at target position", "position", position)
		return
	}

	side := orderbookv1.Side_SIDE_BUY
	if delta < 0 {
		side = orderbookv1.Side_SIDE_SELL
		delta = -delta
	}

	qty := delta
	if qty > e.cfg.Quantity {
		qty = e.cfg.Quantity
	}

	price := currentPrice
	if side == orderbookv1.Side_SIDE_BUY {
		price += e.cfg.PriceOffset
	} else {
		price -= e.cfg.PriceOffset
		if price <= 0 {
			price = 1
		}
	}

	e.log.Info("placing order",
		"signal", signalName(signal),
		"side", side,
		"price", price,
		"quantity", qty,
		"position", position,
		"target", targetPosition,
		"cash_available", portfolio.CashBalance-portfolio.CashHeld)

	e.tracker.PlaceOrder(ctx, e.cfg.AccountID, side, price, qty)
}

func signalName(s Signal) string {
	switch s {
	case SignalBuy:
		return "buy"
	case SignalSell:
		return "sell"
	default:
		return "none"
	}
}
