package mm

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/internal/pricesource"
)

type trackedOrder struct {
	sagaID  string
	orderID string
	side    orderbookv1.Side
	price   int64
	qty     int64
}

type resolveResult struct {
	sagaID  string
	orderID string
	failed  bool
}

type Engine struct {
	cfg      SymbolConfig
	strategy Strategy
	prices   pricesource.PriceSource
	obClient orderbookv1connect.OrderBookServiceClient
	pfClient portfoliov1connect.PortfolioServiceClient
	log      *slog.Logger

	orders       map[string]*trackedOrder
	orderIDIndex map[string]string
	lastRefPrice int64
	resolveCh    chan resolveResult
}

func NewEngine(
	cfg SymbolConfig,
	strategy Strategy,
	prices pricesource.PriceSource,
	obClient orderbookv1connect.OrderBookServiceClient,
	pfClient portfoliov1connect.PortfolioServiceClient,
	log *slog.Logger,
) *Engine {
	return &Engine{
		cfg:          cfg,
		strategy:     strategy,
		prices:       prices,
		obClient:     obClient,
		pfClient:     pfClient,
		log:          log.With("symbol", cfg.Symbol, "account", cfg.AccountID),
		orders:       make(map[string]*trackedOrder),
		orderIDIndex: make(map[string]string),
		resolveCh:    make(chan resolveResult, 64),
	}
}

func (e *Engine) Run(ctx context.Context) error {
	e.bootstrap(ctx)
	e.cleanupOrphans(ctx)

	fillCh := make(chan *orderbookv1.Trade, 64)
	go e.streamTrades(ctx, fillCh)

	e.requote(ctx)

	requoteTicker := time.NewTicker(e.cfg.RequoteInterval)
	defer requoteTicker.Stop()

	priceCheckTicker := time.NewTicker(1 * time.Second)
	defer priceCheckTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.drainResolves()
			e.log.Info("shutting down, cancelling orders", "tracked_orders", len(e.orders))
			e.cancelAllOrders(context.Background())
			return ctx.Err()

		case <-requoteTicker.C:
			if ctx.Err() != nil {
				continue
			}
			e.requote(ctx)

		case <-priceCheckTicker.C:
			if ctx.Err() != nil {
				continue
			}
			e.checkPriceMove(ctx)

		case trade, ok := <-fillCh:
			if !ok {
				fillCh = make(chan *orderbookv1.Trade, 64)
				go e.streamTrades(ctx, fillCh)
				continue
			}
			if ctx.Err() != nil {
				continue
			}
			e.handleFill(ctx, trade)

		case res := <-e.resolveCh:
			e.handleResolve(res)
		}
	}
}

func (e *Engine) bootstrap(ctx context.Context) {
	resp, err := e.pfClient.GetPortfolio(ctx, connect.NewRequest(&portfoliov1.GetPortfolioRequest{
		AccountId: e.cfg.AccountID,
	}))
	if err != nil {
		e.log.Error("failed to get portfolio for bootstrap", "error", err)
		return
	}

	if resp.Msg.CashBalance == 0 && e.cfg.InitialDeposit > 0 {
		_, err := e.pfClient.Deposit(ctx, connect.NewRequest(&portfoliov1.DepositRequest{
			AccountId: e.cfg.AccountID,
			Amount:    e.cfg.InitialDeposit,
		}))
		if err != nil {
			e.log.Error("failed to deposit initial cash", "error", err)
		} else {
			e.log.Info("deposited initial cash", "amount", e.cfg.InitialDeposit)
		}
	}

	hasHolding := false
	for _, h := range resp.Msg.Holdings {
		if h.Symbol == e.cfg.Symbol && h.Quantity > 0 {
			hasHolding = true
			break
		}
	}
	if !hasHolding && e.cfg.InitialShares > 0 {
		refPrice := int64(0)
		if snap, ok := e.prices.GetPrice(e.cfg.Symbol); ok {
			refPrice = snap.Price
		}
		_, err := e.pfClient.CreditShares(ctx, connect.NewRequest(&portfoliov1.CreditSharesRequest{
			AccountId:    e.cfg.AccountID,
			Symbol:       e.cfg.Symbol,
			Quantity:     e.cfg.InitialShares,
			CostPerShare: refPrice,
		}))
		if err != nil {
			e.log.Error("failed to credit initial shares", "error", err)
		} else {
			e.log.Info("credited initial shares", "quantity", e.cfg.InitialShares)
		}
	}
}

func (e *Engine) cleanupOrphans(ctx context.Context) {
	resp, err := e.pfClient.GetPortfolio(ctx, connect.NewRequest(&portfoliov1.GetPortfolioRequest{
		AccountId: e.cfg.AccountID,
	}))
	if err != nil {
		e.log.Error("failed to get portfolio for orphan cleanup", "error", err)
		return
	}

	for _, po := range resp.Msg.PendingOrders {
		if po.Symbol != e.cfg.Symbol {
			continue
		}
		if po.Status != portfoliov1.PendingOrderStatus_PENDING_ORDER_STATUS_ORDER_PLACED {
			continue
		}

		statusResp, err := e.pfClient.GetOrderStatus(ctx, connect.NewRequest(&portfoliov1.GetOrderStatusRequest{
			SagaId: po.SagaId,
		}))
		if err != nil {
			e.log.Error("failed to get orphan order status", "saga_id", po.SagaId, "error", err)
			continue
		}
		if statusResp.Msg.OrderId == "" {
			continue
		}

		_, err = e.obClient.CancelOrder(ctx, connect.NewRequest(&orderbookv1.CancelOrderRequest{
			Symbol:  e.cfg.Symbol,
			OrderId: statusResp.Msg.OrderId,
		}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			e.log.Error("failed to cancel orphan", "order_id", statusResp.Msg.OrderId, "error", err)
		} else {
			e.log.Info("cancelled orphan order", "order_id", statusResp.Msg.OrderId)
		}
	}
}

func (e *Engine) requote(ctx context.Context) {
	snap, ok := e.prices.GetPrice(e.cfg.Symbol)
	if !ok {
		e.log.Warn("no reference price available, skipping requote")
		return
	}
	if time.Since(snap.FetchedAt) > 5*time.Minute {
		e.log.Warn("reference price is stale, skipping requote", "fetched_at", snap.FetchedAt)
		return
	}

	e.cancelAllOrders(ctx)

	portfolio := e.getPortfolio(ctx)

	var position int64
	for _, h := range portfolio.Holdings {
		if h.Symbol == e.cfg.Symbol {
			position = h.Quantity
			break
		}
	}

	inv := InventoryState{
		Position:    position,
		MaxPosition: e.cfg.MaxPosition,
	}
	levels := e.strategy.ComputeQuotes(snap.Price, inv)

	if len(levels) == 0 {
		e.log.Info("no quotes to place")
		return
	}

	e.lastRefPrice = snap.Price

	e.log.Info("placing quotes",
		"ref_price", snap.Price,
		"position", position,
		"cash_available", portfolio.CashBalance-portfolio.CashHeld,
		"levels", len(levels))

	for _, level := range levels {
		e.placeOrder(ctx, level)
	}
}

func (e *Engine) drainResolves() {
	for {
		select {
		case res := <-e.resolveCh:
			e.handleResolve(res)
		default:
			return
		}
	}
}

func (e *Engine) cancelAllOrders(ctx context.Context) {
	for sagaID, tracked := range e.orders {
		orderID := tracked.orderID
		if orderID == "" {
			resp, err := e.pfClient.GetOrderStatus(ctx, connect.NewRequest(&portfoliov1.GetOrderStatusRequest{
				SagaId: sagaID,
			}))
			if err == nil && resp.Msg.OrderId != "" {
				orderID = resp.Msg.OrderId
			} else {
				e.log.Warn("could not resolve order_id for cancel", "saga_id", sagaID, "error", err)
			}
		}
		if orderID != "" {
			_, err := e.obClient.CancelOrder(ctx, connect.NewRequest(&orderbookv1.CancelOrderRequest{
				Symbol:  e.cfg.Symbol,
				OrderId: orderID,
			}))
			if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
				e.log.Error("failed to cancel order", "order_id", orderID, "error", err)
			} else {
				e.log.Info("cancelled order", "order_id", orderID)
			}
		}
		delete(e.orderIDIndex, tracked.orderID)
		delete(e.orders, sagaID)
	}
}

func (e *Engine) getPortfolio(ctx context.Context) *portfoliov1.GetPortfolioResponse {
	resp, err := e.pfClient.GetPortfolio(ctx, connect.NewRequest(&portfoliov1.GetPortfolioRequest{
		AccountId: e.cfg.AccountID,
	}))
	if err != nil {
		e.log.Error("failed to get portfolio", "error", err)
		return &portfoliov1.GetPortfolioResponse{}
	}
	return resp.Msg
}

func (e *Engine) placeOrder(ctx context.Context, level QuoteLevel) {
	resp, err := e.pfClient.PlaceOrder(ctx, connect.NewRequest(&portfoliov1.PortfolioPlaceOrderRequest{
		AccountId:   e.cfg.AccountID,
		Symbol:      e.cfg.Symbol,
		Side:        level.Side,
		Price:       level.Price,
		Quantity:    level.Quantity,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}))
	if err != nil {
		e.log.Error("failed to place order",
			"side", level.Side,
			"price", level.Price,
			"quantity", level.Quantity,
			"error", err)
		return
	}

	sagaID := resp.Msg.SagaId
	tracked := &trackedOrder{
		sagaID: sagaID,
		side:   level.Side,
		price:  level.Price,
		qty:    level.Quantity,
	}
	e.orders[sagaID] = tracked

	go e.resolveOrderID(ctx, sagaID)
}

func (e *Engine) resolveOrderID(ctx context.Context, sagaID string) {
	backoff := 100 * time.Millisecond
	maxBackoff := 1 * time.Second

	for range 20 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		resp, err := e.pfClient.GetOrderStatus(ctx, connect.NewRequest(&portfoliov1.GetOrderStatusRequest{
			SagaId: sagaID,
		}))
		if err != nil {
			e.log.Debug("polling order status", "saga_id", sagaID, "error", err)
			continue
		}

		switch resp.Msg.Status {
		case portfoliov1.OrderStatus_ORDER_STATUS_ORDER_PLACED,
			portfoliov1.OrderStatus_ORDER_STATUS_COMPLETED:
			if resp.Msg.OrderId != "" {
				e.resolveCh <- resolveResult{sagaID: sagaID, orderID: resp.Msg.OrderId}
				return
			}
		case portfoliov1.OrderStatus_ORDER_STATUS_FAILED:
			e.resolveCh <- resolveResult{sagaID: sagaID, failed: true}
			return
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	e.log.Error("gave up resolving order_id", "saga_id", sagaID)
	e.resolveCh <- resolveResult{sagaID: sagaID, failed: true}
}

func (e *Engine) handleResolve(res resolveResult) {
	tracked, ok := e.orders[res.sagaID]
	if !ok {
		return
	}
	if res.failed {
		delete(e.orders, res.sagaID)
		return
	}
	tracked.orderID = res.orderID
	e.orderIDIndex[res.orderID] = res.sagaID
	e.log.Debug("resolved order_id", "saga_id", res.sagaID, "order_id", res.orderID)
}

func (e *Engine) handleFill(ctx context.Context, trade *orderbookv1.Trade) {
	_, buyMatch := e.orderIDIndex[trade.BuyOrderId]
	_, sellMatch := e.orderIDIndex[trade.SellOrderId]
	if !buyMatch && !sellMatch {
		return
	}

	e.log.Info("fill detected",
		"trade_id", trade.TradeId,
		"price", trade.Price,
		"quantity", trade.Quantity)

	e.requote(ctx)
}

func (e *Engine) checkPriceMove(ctx context.Context) {
	if e.lastRefPrice == 0 || e.cfg.PriceMoveThreshold == 0 {
		return
	}
	snap, ok := e.prices.GetPrice(e.cfg.Symbol)
	if !ok {
		return
	}
	delta := snap.Price - e.lastRefPrice
	if delta < 0 {
		delta = -delta
	}
	if delta >= e.cfg.PriceMoveThreshold {
		e.log.Info("reference price moved beyond threshold, requoting",
			"old_price", e.lastRefPrice,
			"new_price", snap.Price,
			"delta", delta)
		e.requote(ctx)
	}
}

func (e *Engine) streamTrades(ctx context.Context, ch chan<- *orderbookv1.Trade) {
	for ctx.Err() == nil {
		stream, err := e.obClient.StreamTrades(ctx, connect.NewRequest(&orderbookv1.StreamTradesRequest{
			Symbol: e.cfg.Symbol,
		}))
		if err != nil {
			e.log.Error("failed to open trade stream", "error", err)
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
			e.log.Error("trade stream error", "error", err)
		}
		stream.Close()

		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}
