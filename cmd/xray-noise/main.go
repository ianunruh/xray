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

	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/gen/saga/v1/sagav1connect"
	"github.com/ianunruh/xray/internal/noise"
	"github.com/ianunruh/xray/internal/trader"
)

func main() {
	configPath := flag.String("config", "noise.yaml", "Path to config file")
	flag.Parse()

	cfg, err := noise.LoadConfig(*configPath)
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
	pfClient := portfoliov1connect.NewPortfolioServiceClient(httpClient, cfg.ServerURL)
	sagaClient := sagav1connect.NewSagaServiceClient(httpClient, cfg.ServerURL)

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
		engine := noise.NewEngine(symCfg, prices, pfClient, sagaClient, log)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := engine.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("engine stopped unexpectedly", "symbol", symCfg.Symbol, "error", err)
			}
		}()
	}

	log.Info("noise trader started",
		"symbols", strings.Join(symbols, ","),
		"server", cfg.ServerURL,
		"price_source", cfg.PriceSource)

	wg.Wait()
	log.Info("noise trader shutdown complete")
}

