package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/internal/mm"
	"github.com/ianunruh/xray/internal/trader"
)

func main() {
	configPath := flag.String("config", "mm.yaml", "Path to config file")
	flag.Parse()

	cfg, err := mm.LoadConfig(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: trader.ParseLogLevel(cfg.LogLevel),
	}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpClient := &http.Client{}
	obClient := orderbookv1connect.NewOrderBookServiceClient(httpClient, cfg.ServerURL)
	pfClient := portfoliov1connect.NewPortfolioServiceClient(httpClient, cfg.ServerURL)

	symbols := make([]string, len(cfg.Symbols))
	for i, s := range cfg.Symbols {
		symbols[i] = s.Symbol
	}

	priceSource := trader.SetupPriceSource(trader.PriceSourceConfig{
		PriceSource:  cfg.PriceSource,
		PolygonKey:   cfg.PolygonKey,
		Polygon:      cfg.Polygon,
		StaticPrices: cfg.StaticPrices,
	}, symbols, log)

	go func() {
		if err := priceSource.Start(ctx); err != nil && ctx.Err() == nil {
			log.Error("price source stopped unexpectedly", "error", err)
		}
	}()

	var wg sync.WaitGroup
	for _, symCfg := range cfg.Symbols {
		strategy := mm.NewSpreadStrategy(symCfg)
		engine := mm.NewEngine(symCfg, strategy, priceSource, obClient, pfClient, log)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := engine.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("engine stopped unexpectedly", "symbol", symCfg.Symbol, "error", err)
			}
		}()
	}

	log.Info("market maker started",
		"symbols", strings.Join(symbols, ","),
		"server", cfg.ServerURL,
		"price_source", cfg.PriceSource)

	wg.Wait()
	log.Info("market maker shutdown complete")
}

