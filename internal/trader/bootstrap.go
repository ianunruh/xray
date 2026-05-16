package trader

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"connectrpc.com/connect"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/internal/pricesource"
)

type BootstrapConfig struct {
	AccountID           string
	Symbol              string
	InitialDeposit      int64
	InitialShares       int64
	RandomInitialShares bool
}

const bootstrapPriceWait = 30 * time.Second

func Bootstrap(
	ctx context.Context,
	cfg BootstrapConfig,
	prices pricesource.PriceSource,
	pfClient portfoliov1connect.PortfolioServiceClient,
	log *slog.Logger,
) {
	resp, err := pfClient.GetPortfolio(ctx, connect.NewRequest(&portfoliov1.GetPortfolioRequest{
		AccountId: cfg.AccountID,
	}))
	if err != nil {
		log.Error("failed to get portfolio for bootstrap", "error", err)
		return
	}

	if resp.Msg.CashBalance == 0 && cfg.InitialDeposit > 0 {
		_, err := pfClient.Deposit(ctx, connect.NewRequest(&portfoliov1.DepositRequest{
			AccountId: cfg.AccountID,
			Amount:    cfg.InitialDeposit,
		}))
		if err != nil {
			log.Error("failed to deposit initial cash", "error", err)
		} else {
			log.Info("deposited initial cash", "amount", cfg.InitialDeposit)
		}
	}

	hasHolding := false
	for _, h := range resp.Msg.Holdings {
		if h.Symbol == cfg.Symbol && h.Quantity > 0 {
			hasHolding = true
			break
		}
	}
	if hasHolding || cfg.InitialShares <= 0 {
		return
	}

	qty := cfg.InitialShares
	if cfg.RandomInitialShares {
		qty = rand.Int64N(cfg.InitialShares + 1)
	}
	if qty <= 0 {
		log.Info("skipping initial share credit", "quantity", qty)
		return
	}

	refPrice, ok := waitForPrice(ctx, prices, cfg.Symbol, bootstrapPriceWait)
	if !ok {
		log.Error("no reference price available, skipping initial share credit",
			"symbol", cfg.Symbol, "quantity", qty)
		return
	}

	if _, err := pfClient.CreditShares(ctx, connect.NewRequest(&portfoliov1.CreditSharesRequest{
		AccountId:    cfg.AccountID,
		Symbol:       cfg.Symbol,
		Quantity:     qty,
		CostPerShare: refPrice,
	})); err != nil {
		log.Error("failed to credit initial shares", "error", err)
	} else {
		log.Info("credited initial shares", "quantity", qty, "cost_per_share", refPrice)
	}
}

func waitForPrice(ctx context.Context, prices pricesource.PriceSource, symbol string, timeout time.Duration) (int64, bool) {
	if snap, ok := prices.GetPrice(symbol); ok && snap.Price > 0 {
		return snap.Price, true
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, false
		case <-deadline.C:
			return 0, false
		case <-tick.C:
			if snap, ok := prices.GetPrice(symbol); ok && snap.Price > 0 {
				return snap.Price, true
			}
		}
	}
}
