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
	"github.com/ianunruh/xray/internal/trader"
	"github.com/ianunruh/xray/internal/trend"
)

func main() {
	configPath := flag.String("config", "trend.yaml", "Path to config file")
	flag.Parse()

	cfg, err := trend.LoadConfig(*configPath)
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

	seen := make(map[string]bool)
	var symbols []string
	for _, s := range cfg.Symbols {
		if !seen[s.Symbol] {
			seen[s.Symbol] = true
			symbols = append(symbols, s.Symbol)
		}
	}

	prices := trader.SetupPriceSource(trader.PriceSourceConfig{
		PriceSource:  cfg.PriceSource,
		PolygonKey:   cfg.PolygonKey,
		Polygon:      cfg.Polygon,
		StaticPrices: cfg.StaticPrices,
	}, symbols, log)

	go func() {
		if err := prices.Start(ctx); err != nil && ctx.Err() == nil {
			log.Error("price source stopped unexpectedly", "error", err)
		}
	}()

	var wg sync.WaitGroup
	for _, symCfg := range cfg.Symbols {
		strategy := trend.NewStrategy(symCfg)
		engine := trend.NewEngine(symCfg, strategy, prices, obClient, pfClient, log)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := engine.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("engine stopped unexpectedly", "symbol", symCfg.Symbol, "error", err)
			}
		}()
	}

	log.Info("trend follower started",
		"symbols", strings.Join(symbols, ","),
		"server", cfg.ServerURL,
		"price_source", cfg.PriceSource)

	wg.Wait()
	log.Info("trend follower shutdown complete")
}
