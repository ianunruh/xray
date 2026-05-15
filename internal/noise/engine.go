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
	e.bootstrap(ctx)

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

func (e *Engine) bootstrap(ctx context.Context) {
	resp, err := e.pfClient.GetPortfolio(ctx, connect.NewRequest(&portfoliov1.GetPortfolioRequest{
		AccountId: e.cfg.AccountID,
	}))
	if err != nil {
		e.log.Error("failed to get portfolio for bootstrap", "error", err)
		return
	}

	if resp.Msg.CashBalance == 0 && e.cfg.InitialDeposit > 0 {
		_, err := e.pfClient.Deposit(ctx, connect.NewRequest(&portfoliov1.DepositRequest{
			AccountId: e.cfg.AccountID,
			Amount:    e.cfg.InitialDeposit,
		}))
		if err != nil {
			e.log.Error("failed to deposit initial cash", "error", err)
		} else {
			e.log.Info("deposited initial cash", "amount", e.cfg.InitialDeposit)
		}
	}

	hasHolding := false
	for _, h := range resp.Msg.Holdings {
		if h.Symbol == e.cfg.Symbol && h.Quantity > 0 {
			hasHolding = true
			break
		}
	}
	if !hasHolding && e.cfg.InitialShares > 0 {
		refPrice := int64(0)
		if snap, ok := e.prices.GetPrice(e.cfg.Symbol); ok {
			refPrice = snap.Price
		}
		_, err := e.pfClient.CreditShares(ctx, connect.NewRequest(&portfoliov1.CreditSharesRequest{
			AccountId:    e.cfg.AccountID,
			Symbol:       e.cfg.Symbol,
			Quantity:     e.cfg.InitialShares,
			CostPerShare: refPrice,
		}))
		if err != nil {
			e.log.Error("failed to credit initial shares", "error", err)
		} else {
			e.log.Info("credited initial shares", "quantity", e.cfg.InitialShares)
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

	side := orderbookv1.Side_SIDE_BUY
	if rand.Float64() >= e.cfg.BuyBias {
		side = orderbookv1.Side_SIDE_SELL
	}

	position := e.getPosition(ctx)
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
		"order_type", orderType)
}

func (e *Engine) getPosition(ctx context.Context) int64 {
	resp, err := e.pfClient.GetPortfolio(ctx, connect.NewRequest(&portfoliov1.GetPortfolioRequest{
		AccountId: e.cfg.AccountID,
	}))
	if err != nil {
		e.log.Error("failed to get portfolio", "error", err)
		return 0
	}
	for _, h := range resp.Msg.Holdings {
		if h.Symbol == e.cfg.Symbol {
			return h.Quantity
		}
	}
	return 0
}
