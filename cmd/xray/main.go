package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ianunruh/xray/gen/diagnostics/v1/diagnosticsv1connect"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/diagnostics"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/natsstore"
	"github.com/ianunruh/xray/pkg/es/pgstore"
	"github.com/ianunruh/xray/web"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://xray:xray@localhost:5432/xray?sslmode=disable"
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8080"
	}

	logLevelStr := os.Getenv("LOG_LEVEL")
	if logLevelStr == "" {
		logLevelStr = "info"
	}
	logLevel := parseLogLevel(logLevelStr)

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Error("failed to parse database config", "error", err)
		os.Exit(1)
	}
	poolConfig.ConnConfig.Tracer = pgstore.NewQueryTracer(log)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := pgstore.New(pool)
	if err := store.Migrate(ctx); err != nil {
		log.Error("failed to migrate", "error", err)
		os.Exit(1)
	}

	registry := es.NewRegistry()
	orderbook.RegisterEvents(registry)
	bracket.RegisterEvents(registry)
	portfolio.RegisterEvents(registry)
	ordersaga.RegisterEvents(registry)

	// Connect to NATS.
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Error("failed to connect to NATS", "url", natsURL, "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("failed to create JetStream context", "error", err)
		os.Exit(1)
	}

	if _, err := natsstore.EnsureStream(ctx, js); err != nil {
		log.Error("failed to ensure NATS stream", "error", err)
		os.Exit(1)
	}

	if err := natsstore.Backfill(ctx, store, js, log); err != nil {
		log.Error("failed to backfill NATS stream", "error", err)
		os.Exit(1)
	}

	publisher := natsstore.NewPublisher(js, log)

	// Create command handlers.
	obHandler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, log).WithSnapshots(store).WithPublisher(publisher)

	bracketHandler := es.NewHandler(store, registry, func(id string) *bracket.BracketSaga {
		return bracket.NewBracketSaga(id)
	}, log).WithPublisher(publisher)

	portfolioHandler := es.NewHandler(store, registry, func(id string) *portfolio.Portfolio {
		return portfolio.NewPortfolio(id)
	}, log).WithPublisher(publisher)

	orderSagaHandler := es.NewHandler(store, registry, func(id string) *ordersaga.OrderSaga {
		return ordersaga.NewOrderSaga(id)
	}, log).WithPublisher(publisher)

	// Create projections.
	tradeProjection := orderbook.NewPgTradeProjection(pool)
	orderProjection := orderbook.NewPgOrderProjection(pool)
	portfolioProjection := portfolio.NewPgPortfolioProjection(pool)
	pnlProjection := portfolio.NewPgPnLProjection(pool)
	depthProjection := orderbook.NewDepthProjection()
	candleProjection := orderbook.NewCandleProjection()
	broker := orderbook.NewBroker()
	portfolioBroker := portfolio.NewPortfolioBroker()
	bracketReactor := bracket.NewReactor(bracketHandler, obHandler, log)
	orderSagaReactor := ordersaga.NewReactor(orderSagaHandler, portfolioHandler, obHandler, log)

	// One consumer per persistent projection so each one's cursor advances
	// independently. Ephemeral projections (in-memory) share a single
	// consumer whose JetStream cursor is reset on every boot, so their
	// state rebuilds from the start of the stream.
	consumers := []*natsstore.ProjectionConsumer{
		natsstore.NewProjectionConsumer(js, registry, log, "ephemeral").
			WithEphemeral(depthProjection, candleProjection, broker, portfolioBroker),
		natsstore.NewProjectionConsumer(js, registry, log, "trade-projection").
			WithPersistent(store, tradeProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "order-projection").
			WithPersistent(store, orderProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "portfolio-projection").
			WithPersistent(store, portfolioProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "pnl-projection").
			WithPersistent(store, pnlProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "saga-reactor").
			WithPersistent(store, orderSagaReactor),
		natsstore.NewProjectionConsumer(js, registry, log, "bracket-reactor").
			WithPersistent(store, bracketReactor),
	}
	for _, c := range consumers {
		if err := c.Start(ctx); err != nil {
			log.Error("failed to start projection consumer", "error", err)
			os.Exit(1)
		}
	}
	broker.SetReady()
	portfolioBroker.SetReady()

	srv := orderbook.NewServer(obHandler, log, tradeProjection, orderProjection, orderProjection, depthProjection, candleProjection, broker)
	bracketSrv := bracket.NewServer(bracketHandler, obHandler, log)
	portfolioSrv := portfolio.NewServer(portfolioHandler, portfolioProjection, pnlProjection, ordersaga.NewPlaceOrderFunc(orderSagaHandler), ordersaga.NewReplaceOrderFunc(orderSagaHandler), ordersaga.NewGetOrderStatusFunc(orderSagaHandler), portfolioBroker, log)
	diagnosticsSrv := diagnostics.NewServer(store, registry, log)

	mux := http.NewServeMux()
	path, h := orderbookv1connect.NewOrderBookServiceHandler(srv)
	mux.Handle(path, h)
	bracketPath, bracketH := orderbookv1connect.NewSagaServiceHandler(bracketSrv)
	mux.Handle(bracketPath, bracketH)
	portfolioPath, portfolioH := portfoliov1connect.NewPortfolioServiceHandler(portfolioSrv)
	mux.Handle(portfolioPath, portfolioH)
	diagnosticsPath, diagnosticsH := diagnosticsv1connect.NewDiagnosticsServiceHandler(diagnosticsSrv)
	mux.Handle(diagnosticsPath, diagnosticsH)
	mux.Handle("/", web.Handler())

	httpServer := &http.Server{
		Addr:      listenAddr,
		Handler:   mux,
		Protocols: &http.Protocols{},
	}
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetHTTP2(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	log.Info("listening", "addr", listenAddr)

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("listen failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	if err := httpServer.Shutdown(context.Background()); err != nil {
		log.Error("shutdown failed", "error", err)
		os.Exit(1)
	}

	log.Info("shutdown complete")
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		fmt.Fprintf(os.Stderr, "unknown log level: %s\n", s)
		os.Exit(1)
		return slog.LevelInfo
	}
}
