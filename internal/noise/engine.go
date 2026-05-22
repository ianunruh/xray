package noise

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/gen/saga/v1/sagav1connect"
	"github.com/ianunruh/xray/internal/pricesource"
	"github.com/ianunruh/xray/internal/trader"
)

type Engine struct {
	cfg      SymbolConfig
	prices   pricesource.PriceSource
	pfClient portfoliov1connect.PortfolioServiceClient
	tracker  *trader.OrderTracker
	phase    *trader.PhaseWatcher
	log      *slog.Logger
}

func NewEngine(
	cfg SymbolConfig,
	prices pricesource.PriceSource,
	pfClient portfoliov1connect.PortfolioServiceClient,
	sagaClient sagav1connect.SagaServiceClient,
	obClient orderbookv1connect.OrderBookServiceClient,
	log *slog.Logger,
) *Engine {
	log = log.With("symbol", cfg.Symbol, "account", cfg.AccountID)
	return &Engine{
		cfg:      cfg,
		prices:   prices,
		pfClient: pfClient,
		tracker:  trader.NewOrderTracker(cfg.Symbol, obClient, sagaClient, log),
		phase:    trader.NewPhaseWatcher(cfg.Symbol),
		log:      log,
	}
}

func (e *Engine) Run(ctx context.Context) error {
	trader.Bootstrap(ctx, trader.BootstrapConfig{
		AccountID:           e.cfg.AccountID,
		Symbol:              e.cfg.Symbol,
		InitialDeposit:      e.cfg.InitialDeposit,
		InitialShares:       e.cfg.InitialShares,
		RandomInitialShares: e.cfg.RandomInitialShares,
	}, e.prices, e.pfClient, e.log)

	e.tracker.CleanupOrphans(ctx, e.cfg.AccountID)

	fillCh := make(chan *orderbookv1.Trade, 64)
	go trader.StreamTrades(ctx, e.tracker.ObClient, e.cfg.Symbol, fillCh, e.log)
	go e.phase.Watch(ctx, e.tracker.ObClient, 5*time.Second, e.log)

	orderTicker := time.NewTicker(e.cfg.OrderInterval)
	defer orderTicker.Stop()

	expireTicker := time.NewTicker(5 * time.Second)
	defer expireTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.tracker.Shutdown()
			return ctx.Err()

		case <-orderTicker.C:
			e.placeRandomOrder(ctx)

		case trade, ok := <-fillCh:
			if !ok {
				fillCh = make(chan *orderbookv1.Trade, 64)
				go trader.StreamTrades(ctx, e.tracker.ObClient, e.cfg.Symbol, fillCh, e.log)
				continue
			}
			if ctx.Err() != nil {
				continue
			}
			e.handleFill(trade)

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

// handleFill drops a tracked order from the book once it trades, so the
// tracker reflects only live resting orders (and ExpireStaleOrders doesn't
// waste a cancel RPC on an order that's already gone).
func (e *Engine) handleFill(trade *orderbookv1.Trade) {
	if e.tracker.RecognizeFill(trade) {
		e.tracker.RemoveFilledOrder(trade)
	}
}

func (e *Engine) placeRandomOrder(ctx context.Context) {
	// CLOSED: don't trade. AUCTION/CLOSING_AUCTION: keep posting GTC
	// limit orders (they rest and contribute to the indicative book),
	// but don't bother with market or IOC orders that would get rejected.
	if e.phase.IsClosed() {
		e.log.Debug("market closed, skipping order")
		return
	}

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

	// During an auction, force GTC limit (market/IOC orders would be
	// rejected). CLOSING_AUCTION rejects regular orders entirely — skip.
	if e.phase.IsAuction() && e.phase.Phase() == orderbookv1.MarketPhase_MARKET_PHASE_CLOSING_AUCTION {
		e.log.Debug("closing auction active, skipping order")
		return
	}
	rollMarket := !e.phase.IsAuction() && rand.Float64() < e.cfg.MarketOrderPct
	if rollMarket {
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

	if !canAfford(portfolio, e.cfg.Symbol, side, orderType, price, qty, snap.Price) {
		flipped := flipSide(side)
		overLimit := (flipped == orderbookv1.Side_SIDE_BUY && position >= e.cfg.MaxPosition) ||
			(flipped == orderbookv1.Side_SIDE_SELL && position <= -e.cfg.MaxPosition)
		if overLimit || !canAfford(portfolio, e.cfg.Symbol, flipped, orderType, price, qty, snap.Price) {
			e.log.Debug("skipping order: insufficient resources on both sides",
				"side", side, "price", price, "quantity", qty, "order_type", orderType,
				"cash_available", portfolio.CashBalance)
			return
		}
		e.log.Debug("flipping side for affordability",
			"from", side, "to", flipped, "order_type", orderType)
		side = flipped
	}

	if e.wouldSelfTrade(portfolio.PendingOrders, side, price, orderType) {
		e.log.Debug("skipping order to avoid self-trade",
			"side", side, "price", price, "order_type", orderType)
		return
	}

	// Market/IOC orders execute immediately and never rest in the book, so
	// there's nothing to time out — place them directly without tracking.
	if orderType == orderbookv1.OrderType_ORDER_TYPE_MARKET {
		resp, err := e.tracker.SagaClient.Place(ctx, connect.NewRequest(&sagav1.PlaceSagaRequest{
			AccountId: e.cfg.AccountID,
			Plan: &sagav1.PlaceSagaRequest_SingleOrder{
				SingleOrder: &sagav1.SingleOrderPlan{
					Symbol:      e.cfg.Symbol,
					Side:        side,
					Price:       price,
					Quantity:    qty,
					OrderType:   orderType,
					TimeInForce: tif,
				},
			},
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
			"cash_available", portfolio.CashBalance)
		return
	}

	// GTC limit orders rest in the book; track them so ExpireStaleOrders
	// can cancel any that linger past OrderTimeout.
	e.log.Info("placing order",
		"side", side,
		"price", price,
		"quantity", qty,
		"order_type", orderType,
		"position", position,
		"cash_available", portfolio.CashBalance)
	e.tracker.PlaceOrder(ctx, e.cfg.AccountID, side, price, qty)
}

// marketBuyAffordabilityPadBps pads the reference-price estimate of a market
// buy's cash requirement. The saga reactor walks the actual ask book and pads
// 1.05×; the noise trader has no book visibility, so it uses a larger 1.10×
// pad to absorb both the depth-walk shortfall and slippage.
const marketBuyAffordabilityPadBps = 11000

func flipSide(s orderbookv1.Side) orderbookv1.Side {
	if s == orderbookv1.Side_SIDE_BUY {
		return orderbookv1.Side_SIDE_SELL
	}
	return orderbookv1.Side_SIDE_BUY
}

func canAfford(
	portfolio *portfoliov1.GetPortfolioResponse,
	symbol string,
	side orderbookv1.Side,
	orderType orderbookv1.OrderType,
	price, qty, refPrice int64,
) bool {
	if side == orderbookv1.Side_SIDE_SELL {
		var available int64
		for _, h := range portfolio.Holdings {
			if h.Symbol == symbol {
				available = h.Quantity - h.SharesHeld
				break
			}
		}
		return available >= qty
	}
	var required int64
	if orderType == orderbookv1.OrderType_ORDER_TYPE_MARKET {
		required = (refPrice * qty * marketBuyAffordabilityPadBps) / 10000
	} else {
		required = price * qty
	}
	return portfolio.CashBalance >= required
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

