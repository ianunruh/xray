package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/pgstore"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://xray:xray@localhost:5432/xray?sslmode=disable"
	}

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":8080"
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx := context.Background()

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

	handler := es.NewHandler(store, registry, func(id string) *orderbook.OrderBook {
		return orderbook.NewOrderBook(id)
	}, log).WithSnapshots(store)

	srv := orderbook.NewServer(handler, log)

	mux := http.NewServeMux()
	path, h := orderbookv1connect.NewOrderBookServiceHandler(srv)
	mux.Handle(path, h)

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
		Protocols: &http.Protocols{},
	}
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetHTTP2(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	log.Info("listening", "addr", listenAddr)
	if err := httpServer.ListenAndServe(); err != nil {
		log.Error("listen failed", "error", err)
		os.Exit(1)
	}
}
