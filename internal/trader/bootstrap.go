package trader

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/internal/pricesource"
)

type BootstrapConfig struct {
	AccountID      string
	Symbol         string
	InitialDeposit int64
	InitialShares  int64
}

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
	if !hasHolding && cfg.InitialShares > 0 {
		refPrice := int64(0)
		if snap, ok := prices.GetPrice(cfg.Symbol); ok {
			refPrice = snap.Price
		}
		_, err := pfClient.CreditShares(ctx, connect.NewRequest(&portfoliov1.CreditSharesRequest{
			AccountId:    cfg.AccountID,
			Symbol:       cfg.Symbol,
			Quantity:     cfg.InitialShares,
			CostPerShare: refPrice,
		}))
		if err != nil {
			log.Error("failed to credit initial shares", "error", err)
		} else {
			log.Info("credited initial shares", "quantity", cfg.InitialShares)
		}
	}
}
