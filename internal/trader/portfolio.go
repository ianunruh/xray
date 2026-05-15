package trader

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
)

func GetPortfolio(
	ctx context.Context,
	pfClient portfoliov1connect.PortfolioServiceClient,
	accountID string,
	log *slog.Logger,
) *portfoliov1.GetPortfolioResponse {
	resp, err := pfClient.GetPortfolio(ctx, connect.NewRequest(&portfoliov1.GetPortfolioRequest{
		AccountId: accountID,
	}))
	if err != nil {
		log.Error("failed to get portfolio", "error", err)
		return &portfoliov1.GetPortfolioResponse{}
	}
	return resp.Msg
}

func GetPosition(portfolio *portfoliov1.GetPortfolioResponse, symbol string) int64 {
	for _, h := range portfolio.Holdings {
		if h.Symbol == symbol {
			return h.Quantity
		}
	}
	return 0
}
