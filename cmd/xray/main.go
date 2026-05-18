package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
	"github.com/ianunruh/xray/gen/trader/v1/traderv1connect"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/diagnostics"
	"github.com/ianunruh/xray/internal/feesaccruer"
	"github.com/ianunruh/xray/internal/margincall"
	"github.com/ianunruh/xray/internal/ocosaga"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/internal/pricesource"
	"github.com/ianunruh/xray/internal/reconciler"
	"github.com/ianunruh/xray/internal/sagasvc"
	"github.com/ianunruh/xray/internal/tradermgr"
	"github.com/ianunruh/xray/internal/twapsaga"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/natsstore"
	"github.com/ianunruh/xray/pkg/es/pgstore"
	"github.com/ianunruh/xray/pkg/es/snapshotter"
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
	}, log).WithSnapshots(store).WithPublisher(publisher)

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
	feesProjection := portfolio.NewPgFeesProjection(pool)
	accruableAccounts := portfolio.NewInMemoryAccruableAccounts()
	broker := orderbook.NewBroker()
	portfolioBroker := portfolio.NewPortfolioBroker()
	bracketReactor := bracket.NewReactor(bracketHandler, orderSagaHandler, ocoSagaHandler, obHandler, log)
	orderSagaReactor := ordersaga.NewReactor(orderSagaHandler, portfolioHandler, obHandler, markProjection, log)
	ocoSagaReactor := ocosaga.NewReactor(ocoSagaHandler, portfolioHandler, obHandler, log)
	twapReactor := twapsaga.NewReactor(twapHandler, orderSagaHandler, log)
	marginReactor := margincall.NewReactor(portfolioHandler, orderSagaHandler, obHandler, shortsProjection, longsProjection, activeUserSagasProjection, markProjection, activeCallsProjection,
		margincall.Config{Grace: 30 * time.Second}, log)

	snap := newSnapshotter(store, registry, log).WithMaxIdle(30 * time.Minute)

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
			WithPersistent(store, shortsProjection, longsProjection, activeUserSagasProjection, marginCallsProjection, marginReactor).
			WithReactor(),
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
			WithPersistent(store, orderSagaReactor).
			WithReactor(),
		natsstore.NewProjectionConsumer(js, registry, log, "bracket-reactor").
			WithPersistent(store, bracketReactor).
			WithReactor(),
		natsstore.NewProjectionConsumer(js, registry, log, "oco-reactor").
			WithPersistent(store, ocoSagaReactor).
			WithReactor(),
		natsstore.NewProjectionConsumer(js, registry, log, "twap-reactor").
			WithPersistent(store, twapReactor).
			WithReactor(),
		natsstore.NewProjectionConsumer(js, registry, log, "saga-projection").
			WithPersistent(store, sagaProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "daily-close-projection").
			WithPersistent(store, dailyCloseProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "fees-history").
			WithPersistent(store, feesProjection),
		natsstore.NewProjectionConsumer(js, registry, log, "snapshotter").
			WithPersistent(store, snap),
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

	// ProjectionManager mediates projection introspection and rebuilds.
	// Hosted by the diagnostics service so the web UI can drive it.
	projectionManager := natsstore.NewProjectionManager(js, log)
	for _, c := range consumers {
		projectionManager.Add(c)
	}

	rec := reconciler.New(30*time.Second, sagaProjection, tradeProjection, portfolioHandler, orderSagaReactor, bracketReactor, ocoSagaReactor, twapReactor, marginReactor, activeCallsProjection, log)
	go rec.Run(ctx)

	accruer := feesaccruer.NewAccruer(portfolioHandler, accruableAccounts, markProjection, time.Now,
		feesaccruer.Config{Interval: time.Hour}, log)
	go accruer.Run(ctx)

	go runSnapshotSweeper(ctx, snap, 5*time.Minute)

	// In-process trader manager. Persists trader configs in PG and
	// supervises enabled traders as goroutines. Reuses the same
	// connect clients the standalone CLIs use (over loopback HTTP)
	// so engine semantics match exactly.
	traderStore := tradermgr.NewStore(pool)
	priceSrc, err := setupPriceSource(log)
	if err != nil {
		log.Error("failed to set up trader price source", "error", err)
		os.Exit(1)
	}
	go func() {
		if err := priceSrc.Start(ctx); err != nil && ctx.Err() == nil {
			log.Error("trader price source stopped unexpectedly", "error", err)
		}
	}()
	traderMgr := tradermgr.NewManager(traderStore, priceSrc, traderServerURL(listenAddr), log)

	srv := orderbook.NewServer(obHandler, log, tradeProjection, orderProjection, orderProjection, depthProjection, candleProjection, dailyCloseProjection, broker)
	portfolioSrv := portfolio.NewServer(portfolioHandler, obHandler, portfolioProjection, pnlProjection, markProjection, marginCallsProjection, feesProjection, shortsProjection, portfolioBroker, log)
	sagaSrv := sagasvc.NewServer(orderSagaHandler, bracketHandler, ocoSagaHandler, twapHandler, obHandler, portfolioHandler, twapReactor, markProjection, sagaProjection, log)
	diagnosticsSrv := diagnostics.NewServer(store, registry, projectionManager, accruer, rec, marginReactor, log)
	traderSrv := tradermgr.NewServer(traderMgr)

	mux := http.NewServeMux()
	path, h := orderbookv1connect.NewOrderBookServiceHandler(srv)
	mux.Handle(path, h)
	portfolioPath, portfolioH := portfoliov1connect.NewPortfolioServiceHandler(portfolioSrv)
	mux.Handle(portfolioPath, portfolioH)
	sagaPath, sagaH := sagav1connect.NewSagaServiceHandler(sagaSrv)
	mux.Handle(sagaPath, sagaH)
	diagnosticsPath, diagnosticsH := diagnosticsv1connect.NewDiagnosticsServiceHandler(diagnosticsSrv)
	mux.Handle(diagnosticsPath, diagnosticsH)
	traderPath, traderH := traderv1connect.NewTraderServiceHandler(traderSrv)
	mux.Handle(traderPath, traderH)

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

	// Auto-start enabled traders. Runs after the listener is up because
	// each trader's connect client dials this same server over loopback.
	go func() {
		// Tiny delay so the listener is actually accepting before the
		// first trader makes an RPC. Avoids spurious "connection
		// refused" warnings in the engine bootstrap path.
		time.Sleep(100 * time.Millisecond)
		if err := traderMgr.AutoStart(ctx); err != nil {
			log.Error("trader auto-start failed", "error", err)
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

// newSnapshotter constructs the async snapshotter and registers every
// aggregate type whose factory produces a Snapshotable.
func newSnapshotter(store *pgstore.Store, registry *es.Registry, log *slog.Logger) *snapshotter.Snapshotter {
	s := snapshotter.New(store, store, registry, log)
	s.Register("orderbook", func(id string) es.Aggregate {
		return orderbook.NewOrderBook(id)
	})
	s.Register(portfolio.AggregateType, func(id string) es.Aggregate {
		return portfolio.NewPortfolio(id)
	})
	return s
}

// runSnapshotSweeper periodically asks the snapshotter to evict idle
// aggregates from its in-memory map. Stops when ctx is done.
func runSnapshotSweeper(ctx context.Context, s *snapshotter.Snapshotter, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Sweep()
		}
	}
}

// setupPriceSource builds the shared price source the in-process trader
// manager hands to every engine. Env vars mirror the per-CLI YAML config:
// PRICE_SOURCE selects the implementation (polygon or static); POLYGON_*
// override the polling configuration; STATIC_PRICES is a comma-separated
// SYMBOL=PRICE list (integer cents-shifted-4 prices, matching the wire format).
func setupPriceSource(log *slog.Logger) (pricesource.PriceSource, error) {
	src := strings.ToLower(strings.TrimSpace(os.Getenv("PRICE_SOURCE")))
	if src == "" {
		if os.Getenv("POLYGON_API_KEY") != "" {
			src = "polygon"
		} else {
			src = "static"
		}
	}

	switch src {
	case "static":
		return pricesource.NewStaticPriceSource(parseStaticPrices(os.Getenv("STATIC_PRICES"))), nil
	case "polygon":
		apiKey := os.Getenv("POLYGON_API_KEY")
		if apiKey == "" {
			return nil, fmt.Errorf("PRICE_SOURCE=polygon requires POLYGON_API_KEY")
		}
		cfg := pricesource.PolygonConfig{
			BaseURL:      envOr("POLYGON_BASE_URL", "https://api.polygon.io"),
			PollInterval: parseDurationOr("POLYGON_POLL_INTERVAL", 30*time.Second),
		}
		return pricesource.NewPolygonPriceSource(cfg, apiKey, nil, log), nil
	default:
		return nil, fmt.Errorf("unknown PRICE_SOURCE: %q", src)
	}
}

// traderServerURL converts the listen address into a URL the in-process
// trader clients can dial over loopback. Bind-host wildcards collapse to
// localhost; explicit hosts pass through.
func traderServerURL(listenAddr string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "http://localhost" + listenAddr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}

func parseStaticPrices(s string) map[string]int64 {
	out := map[string]int64{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			continue
		}
		out[strings.TrimSpace(k)] = n
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseDurationOr(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
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
