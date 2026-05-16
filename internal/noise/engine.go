package noise

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/internal/pricesource"
	"github.com/ianunruh/xray/internal/trader"
)

type Engine struct {
	cfg      SymbolConfig
	prices   pricesource.PriceSource
	pfClient portfoliov1connect.PortfolioServiceClient
	log      *slog.Logger
}

func NewEngine(
	cfg SymbolConfig,
	prices pricesource.PriceSource,
	pfClient portfoliov1connect.PortfolioServiceClient,
	log *slog.Logger,
) *Engine {
	return &Engine{
		cfg:      cfg,
		prices:   prices,
		pfClient: pfClient,
		log:      log.With("symbol", cfg.Symbol, "account", cfg.AccountID),
	}
}

func (e *Engine) Run(ctx context.Context) error {
	trader.Bootstrap(ctx, trader.BootstrapConfig{
		AccountID:      e.cfg.AccountID,
		Symbol:         e.cfg.Symbol,
		InitialDeposit: e.cfg.InitialDeposit,
		InitialShares:  e.cfg.InitialShares,
	}, e.prices, e.pfClient, e.log)

	ticker := time.NewTicker(e.cfg.OrderInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			e.placeRandomOrder(ctx)
		}
	}
}

func (e *Engine) placeRandomOrder(ctx context.Context) {
	snap, ok := e.prices.GetPrice(e.cfg.Symbol)
	if !ok {
		e.log.Warn("no reference price available")
		return
	}
	if time.Since(snap.FetchedAt) > 5*time.Minute {
		e.log.Warn("reference price is stale")
		return
	}

	portfolio := trader.GetPortfolio(ctx, e.pfClient, e.cfg.AccountID, e.log)
	position := trader.GetPosition(portfolio, e.cfg.Symbol)

	side := orderbookv1.Side_SIDE_BUY
	if rand.Float64() >= e.cfg.BuyBias {
		side = orderbookv1.Side_SIDE_SELL
	}

	if side == orderbookv1.Side_SIDE_BUY && position >= e.cfg.MaxPosition {
		side = orderbookv1.Side_SIDE_SELL
	} else if side == orderbookv1.Side_SIDE_SELL && position <= -e.cfg.MaxPosition {
		side = orderbookv1.Side_SIDE_BUY
	}

	if (side == orderbookv1.Side_SIDE_BUY && position >= e.cfg.MaxPosition) ||
		(side == orderbookv1.Side_SIDE_SELL && position <= -e.cfg.MaxPosition) {
		return
	}

	qty := e.cfg.MinQuantity
	if e.cfg.MaxQuantity > e.cfg.MinQuantity {
		qty += rand.Int64N(e.cfg.MaxQuantity - e.cfg.MinQuantity + 1)
	}

	orderType := orderbookv1.OrderType_ORDER_TYPE_LIMIT
	tif := orderbookv1.TimeInForce_TIME_IN_FORCE_GTC
	price := snap.Price

	if rand.Float64() < e.cfg.MarketOrderPct {
		orderType = orderbookv1.OrderType_ORDER_TYPE_MARKET
		tif = orderbookv1.TimeInForce_TIME_IN_FORCE_IOC
		price = 0
	} else if e.cfg.PriceJitter > 0 {
		jitter := rand.Int64N(e.cfg.PriceJitter*2+1) - e.cfg.PriceJitter
		price = snap.Price + jitter
		if price <= 0 {
			price = 1
		}
	}

	if e.wouldSelfTrade(portfolio.PendingOrders, side, price, orderType) {
		e.log.Debug("skipping order to avoid self-trade",
			"side", side, "price", price, "order_type", orderType)
		return
	}

	resp, err := e.pfClient.PlaceOrder(ctx, connect.NewRequest(&portfoliov1.PortfolioPlaceOrderRequest{
		AccountId:   e.cfg.AccountID,
		Symbol:      e.cfg.Symbol,
		Side:        side,
		Price:       price,
		Quantity:    qty,
		OrderType:   orderType,
		TimeInForce: tif,
	}))
	if err != nil {
		e.log.Error("failed to place order", "error", err)
		return
	}

	e.log.Info("placed order",
		"saga_id", resp.Msg.SagaId,
		"side", side,
		"price", price,
		"quantity", qty,
		"order_type", orderType,
		"position", position,
		"cash_available", portfolio.CashBalance-portfolio.CashHeld)
}

func (e *Engine) wouldSelfTrade(
	pending []*portfoliov1.PendingOrder,
	side orderbookv1.Side,
	price int64,
	orderType orderbookv1.OrderType,
) bool {
	for _, po := range pending {
		if po.Status == portfoliov1.OrderStatus_ORDER_STATUS_COMPLETED || po.Status == portfoliov1.OrderStatus_ORDER_STATUS_FAILED {
			continue
		}
		if po.Symbol != e.cfg.Symbol {
			continue
		}
		if po.Side == side {
			continue
		}
		if orderType == orderbookv1.OrderType_ORDER_TYPE_MARKET {
			return true
		}
		if side == orderbookv1.Side_SIDE_BUY && price >= po.Price {
			return true
		}
		if side == orderbookv1.Side_SIDE_SELL && price <= po.Price {
			return true
		}
	}
	return false
}

