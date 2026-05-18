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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ianunruh/xray/gen/diagnostics/v1/diagnosticsv1connect"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/gen/saga/v1/sagav1connect"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/diagnostics"
	"github.com/ianunruh/xray/internal/feesaccruer"
	"github.com/ianunruh/xray/internal/margincall"
	"github.com/ianunruh/xray/internal/ocosaga"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/internal/reconciler"
	"github.com/ianunruh/xray/internal/sagasvc"
	"github.com/ianunruh/xray/internal/twapsaga"
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
	ocosaga.RegisterEvents(registry)
	twapsaga.RegisterEvents(registry)

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

	ocoSagaHandler := es.NewHandler(store, registry, func(id string) *ocosaga.OCOSaga {
		return ocosaga.NewOCOSaga(id)
	}, log).WithPublisher(publisher)

	twapHandler := es.NewHandler(store, registry, func(id string) *twapsaga.TWAPSaga {
		return twapsaga.NewTWAPSaga(id)
	}, log).WithPublisher(publisher)

	// Create projections.
	tradeProjection := orderbook.NewPgTradeProjection(pool)
	orderProjection := orderbook.NewPgOrderProjection(pool)
	dailyCloseProjection := orderbook.NewPgDailyCloseProjection(pool)
	portfolioProjection := portfolio.NewPgPortfolioProjection(pool)
	pnlProjection := portfolio.NewPgPnLProjection(pool)
	sagaProjection := sagasvc.NewPgProjection(pool)
	depthProjection := orderbook.NewDepthProjection()
	candleProjection := orderbook.NewCandleProjection()
	markProjection := orderbook.NewMarkProjection()
	shortsProjection := portfolio.NewPgShortsBySymbolProjection(pool)
	longsProjection := portfolio.NewPgLongsBySymbolProjection(pool)
	activeUserSagasProjection := portfolio.NewPgActiveUserSagasProjection(pool)
	activeCallsProjection := portfolio.NewInMemoryActiveMarginCalls()
	marginCallsProjection := portfolio.NewPgMarginCallsProjection(pool)
	accruableAccounts := portfolio.NewInMemoryAccruableAccounts()
	broker := orderbook.NewBroker()
	portfolioBroker := portfolio.NewPortfolioBroker()
	bracketReactor := bracket.NewReactor(bracketHandler, orderSagaHandler, ocoSagaHandler, obHandler, log)
	orderSagaReactor := ordersaga.NewReactor(orderSagaHandler, portfolioHandler, obHandler, markProjection, log)
	ocoSagaReactor := ocosaga.NewReactor(ocoSagaHandler, portfolioHandler, obHandler, log)
	twapReactor := twapsaga.NewReactor(twapHandler, orderSagaHandler, log)
	marginReactor := margincall.NewReactor(portfolioHandler, orderSagaHandler, obHandler, shortsProjection, longsProjection, activeUserSagasProjection, markProjection,
		margincall.Config{Grace: 30 * time.Second}, log)

	// One consumer per persistent projection so each one's cursor advances
	// independently. Ephemeral projections (in-memory) share a single
	// consumer whose JetStream cursor is reset on every boot, so their
	// state rebuilds from the start of the stream.
	consumers := []*natsstore.ProjectionConsumer{
		natsstore.NewProjectionConsumer(js, registry, log, "ephemeral").
			WithEphemeral(depthProjection, candleProjection, markProjection, activeCallsProjection, accruableAccounts, broker),
		// shortsProjection MUST precede marginReactor here: the
		// reactor queries the shorts table the projection writes,
		// and the consumer dispatches projections in slice order
		// against a shared checkpoint. Co-located so a TradeExecuted
		// never reaches the reactor before its prior ShortOpened has
		// been committed to PG.
		natsstore.NewProjectionConsumer(js, registry, log, "margin-call").
			WithPersistent(store, shortsProjection, longsProjection, activeUserSagasProjection, marginCallsProjection, marginReactor),
		natsstore.NewProjectionConsumer(js, registry, log, "trade-projection").
			WithPersistent(store, tradeProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "order-projection").
			WithPersistent(store, orderProjection),
		// portfolioBroker MUST follow portfolioProjection in this slice:
		// the broker wakes streaming subscribers, and the server's
		// re-fetch must see the projection's just-committed UPDATE.
		// Co-located in one consumer guarantees that ordering within
		// every batch.
		natsstore.NewProjectionConsumer(js, registry, log, "portfolio-projection").
			WithPersistent(store, portfolioProjection, portfolioBroker),
		natsstore.NewProjectionConsumer(js, registry, log, "pnl-projection").
			WithPersistent(store, pnlProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "saga-reactor").
			WithPersistent(store, orderSagaReactor),
		natsstore.NewProjectionConsumer(js, registry, log, "bracket-reactor").
			WithPersistent(store, bracketReactor),
		natsstore.NewProjectionConsumer(js, registry, log, "oco-reactor").
			WithPersistent(store, ocoSagaReactor),
		natsstore.NewProjectionConsumer(js, registry, log, "twap-reactor").
			WithPersistent(store, twapReactor),
		natsstore.NewProjectionConsumer(js, registry, log, "saga-projection").
			WithPersistent(store, sagaProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "daily-close-projection").
			WithPersistent(store, dailyCloseProjection),
	}
	// Bootstrap broker's saga→account routing map BEFORE consumers
	// start. The consumer resumes from checkpoint so OrderSagaStarted
	// for in-flight sagas won't replay; without this snapshot, their
	// later lifecycle events couldn't route to a subscriber.
	if activeSagas, err := activeUserSagasProjection.AllActiveSagas(ctx); err != nil {
		log.Error("failed to bootstrap portfolio broker saga map", "error", err)
		os.Exit(1)
	} else {
		portfolioBroker.BootstrapSagas(activeSagas)
	}

	for _, c := range consumers {
		if err := c.Start(ctx); err != nil {
			log.Error("failed to start projection consumer", "error", err)
			os.Exit(1)
		}
	}
	broker.SetReady()
	portfolioBroker.SetReady()

	rec := reconciler.New(30*time.Second, sagaProjection, tradeProjection, portfolioHandler, orderSagaReactor, bracketReactor, ocoSagaReactor, twapReactor, marginReactor, activeCallsProjection, log)
	go rec.Run(ctx)

	accruer := feesaccruer.NewAccruer(portfolioHandler, accruableAccounts, markProjection, time.Now,
		feesaccruer.Config{Interval: time.Hour}, log)
	go accruer.Run(ctx)

	srv := orderbook.NewServer(obHandler, log, tradeProjection, orderProjection, orderProjection, depthProjection, candleProjection, dailyCloseProjection, broker)
	portfolioSrv := portfolio.NewServer(portfolioHandler, obHandler, portfolioProjection, pnlProjection, markProjection, marginCallsProjection, portfolioBroker, log)
	sagaSrv := sagasvc.NewServer(orderSagaHandler, bracketHandler, ocoSagaHandler, twapHandler, obHandler, portfolioHandler, twapReactor, markProjection, sagaProjection, log)
	diagnosticsSrv := diagnostics.NewServer(store, registry, log)

	mux := http.NewServeMux()
	path, h := orderbookv1connect.NewOrderBookServiceHandler(srv)
	mux.Handle(path, h)
	portfolioPath, portfolioH := portfoliov1connect.NewPortfolioServiceHandler(portfolioSrv)
	mux.Handle(portfolioPath, portfolioH)
	sagaPath, sagaH := sagav1connect.NewSagaServiceHandler(sagaSrv)
	mux.Handle(sagaPath, sagaH)
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
