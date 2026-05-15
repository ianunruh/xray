package trader

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
)

func StreamTrades(
	ctx context.Context,
	obClient orderbookv1connect.OrderBookServiceClient,
	symbol string,
	ch chan<- *orderbookv1.Trade,
	log *slog.Logger,
) {
	for ctx.Err() == nil {
		stream, err := obClient.StreamTrades(ctx, connect.NewRequest(&orderbookv1.StreamTradesRequest{
			Symbol: symbol,
		}))
		if err != nil {
			log.Error("failed to open trade stream", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		for stream.Receive() {
			ch <- stream.Msg()
		}
		if err := stream.Err(); err != nil && ctx.Err() == nil {
			log.Error("trade stream error", "error", err)
		}
		stream.Close()

		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}
