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
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/natsstore"
	"github.com/ianunruh/xray/pkg/es/pgstore"
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
	registry.Register("OrderPlaced", func() proto.Message { return new(orderbookv1.OrderPlaced) })
	registry.Register("TradeExecuted", func() proto.Message { return new(orderbookv1.TradeExecuted) })
	registry.Register("OrderCancelled", func() proto.Message { return new(orderbookv1.OrderCancelled) })

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

	// Create projections.
	tradeProjection := orderbook.NewPgTradeProjection(pool)
	orderProjection := orderbook.NewPgOrderProjection(pool)
	depthProjection := orderbook.NewDepthProjection()
	broker := orderbook.NewBroker()

	// Start NATS consumer: replays from stream, then consumes live events.
	// Ephemeral projections (in-memory) always replay from the beginning.
	// Persistent projections (Pg-backed) resume from the last checkpoint.
	consumer := natsstore.NewProjectionConsumer(js, registry, log).
		WithEphemeral(depthProjection, broker).
		WithPersistent(store, tradeProjection, orderProjection)
	if err := consumer.Start(ctx); err != nil {
		log.Error("failed to start projection consumer", "error", err)
		os.Exit(1)
	}
	broker.SetReady()

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, log).WithSnapshots(store).WithPublisher(publisher)

	srv := orderbook.NewServer(handler, log, tradeProjection, orderProjection, depthProjection, broker)

	mux := http.NewServeMux()
	path, h := orderbookv1connect.NewOrderBookServiceHandler(srv)
	mux.Handle(path, h)
	mux.Handle("/", orderbook.WebHandler())

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
