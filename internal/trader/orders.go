package trader

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
)

type TrackedOrder struct {
	SagaID  string
	OrderID string
	Side    orderbookv1.Side
	Price   int64
	Qty     int64
	PlacedAt time.Time
}

type ResolveResult struct {
	SagaID  string
	OrderID string
	Failed  bool
}

type OrderTracker struct {
	Symbol   string
	ObClient orderbookv1connect.OrderBookServiceClient
	PfClient portfoliov1connect.PortfolioServiceClient
	Log      *slog.Logger

	Orders       map[string]*TrackedOrder
	OrderIDIndex map[string]string
	ResolveCh    chan ResolveResult
}

func NewOrderTracker(
	symbol string,
	obClient orderbookv1connect.OrderBookServiceClient,
	pfClient portfoliov1connect.PortfolioServiceClient,
	log *slog.Logger,
) *OrderTracker {
	return &OrderTracker{
		Symbol:       symbol,
		ObClient:     obClient,
		PfClient:     pfClient,
		Log:          log,
		Orders:       make(map[string]*TrackedOrder),
		OrderIDIndex: make(map[string]string),
		ResolveCh:    make(chan ResolveResult, 64),
	}
}

func (t *OrderTracker) PlaceOrder(
	ctx context.Context,
	accountID string,
	side orderbookv1.Side,
	price, qty int64,
) {
	resp, err := t.PfClient.PlaceOrder(ctx, connect.NewRequest(&portfoliov1.PortfolioPlaceOrderRequest{
		AccountId:   accountID,
		Symbol:      t.Symbol,
		Side:        side,
		Price:       price,
		Quantity:    qty,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}))
	if err != nil {
		t.Log.Error("failed to place order",
			"side", side,
			"price", price,
			"quantity", qty,
			"error", err)
		return
	}

	sagaID := resp.Msg.SagaId
	tracked := &TrackedOrder{
		SagaID:   sagaID,
		Side:     side,
		Price:    price,
		Qty:      qty,
		PlacedAt: time.Now(),
	}
	t.Orders[sagaID] = tracked

	go t.resolveOrderID(ctx, sagaID)
}

func (t *OrderTracker) HandleResolve(res ResolveResult) {
	tracked, ok := t.Orders[res.SagaID]
	if !ok {
		return
	}
	if res.Failed {
		delete(t.Orders, res.SagaID)
		return
	}
	tracked.OrderID = res.OrderID
	t.OrderIDIndex[res.OrderID] = res.SagaID
	t.Log.Debug("resolved order_id", "saga_id", res.SagaID, "order_id", res.OrderID)
}

func (t *OrderTracker) IsOwnTrade(trade *orderbookv1.Trade) bool {
	_, buyMatch := t.OrderIDIndex[trade.BuyOrderId]
	_, sellMatch := t.OrderIDIndex[trade.SellOrderId]
	return buyMatch || sellMatch
}

func (t *OrderTracker) RemoveFilledOrder(trade *orderbookv1.Trade) {
	for _, orderID := range []string{trade.BuyOrderId, trade.SellOrderId} {
		if sagaID, ok := t.OrderIDIndex[orderID]; ok {
			delete(t.OrderIDIndex, orderID)
			delete(t.Orders, sagaID)
		}
	}
}

func (t *OrderTracker) CancelTracked(ctx context.Context, sagaID string) {
	tracked, ok := t.Orders[sagaID]
	if !ok {
		return
	}
	orderID := tracked.OrderID
	if orderID == "" {
		resp, err := t.PfClient.GetOrderStatus(ctx, connect.NewRequest(&portfoliov1.GetOrderStatusRequest{
			SagaId: sagaID,
		}))
		if err == nil && resp.Msg.OrderId != "" {
			orderID = resp.Msg.OrderId
		} else {
			t.Log.Warn("could not resolve order_id for cancel", "saga_id", sagaID, "error", err)
		}
	}
	if orderID != "" {
		_, err := t.ObClient.CancelOrder(ctx, connect.NewRequest(&orderbookv1.CancelOrderRequest{
			Symbol:  t.Symbol,
			OrderId: orderID,
		}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			t.Log.Error("failed to cancel order", "order_id", orderID, "error", err)
		} else {
			t.Log.Info("cancelled order", "order_id", orderID)
		}
	}
	delete(t.OrderIDIndex, tracked.OrderID)
	delete(t.Orders, sagaID)
}

func (t *OrderTracker) CancelAll(ctx context.Context) {
	sagaIDs := make([]string, 0, len(t.Orders))
	for sagaID := range t.Orders {
		sagaIDs = append(sagaIDs, sagaID)
	}
	for _, sagaID := range sagaIDs {
		t.CancelTracked(ctx, sagaID)
	}
}

func (t *OrderTracker) ReplaceOrder(
	ctx context.Context,
	accountID string,
	oldSagaID string,
	side orderbookv1.Side,
	price, qty int64,
) {
	oldTracked, ok := t.Orders[oldSagaID]
	if !ok {
		t.PlaceOrder(ctx, accountID, side, price, qty)
		return
	}

	oldOrderID := oldTracked.OrderID
	if oldOrderID == "" {
		t.CancelTracked(ctx, oldSagaID)
		t.PlaceOrder(ctx, accountID, side, price, qty)
		return
	}

	resp, err := t.PfClient.ReplaceOrder(ctx, connect.NewRequest(&portfoliov1.PortfolioReplaceOrderRequest{
		AccountId:   accountID,
		Symbol:      t.Symbol,
		OldOrderId:  oldOrderID,
		Side:        side,
		Price:       price,
		Quantity:    qty,
		OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
	}))
	if err != nil {
		t.Log.Error("failed to replace order, falling back to cancel+place",
			"old_order_id", oldOrderID,
			"error", err)
		t.CancelTracked(ctx, oldSagaID)
		t.PlaceOrder(ctx, accountID, side, price, qty)
		return
	}

	delete(t.OrderIDIndex, oldTracked.OrderID)
	delete(t.Orders, oldSagaID)

	sagaID := resp.Msg.SagaId
	t.Orders[sagaID] = &TrackedOrder{
		SagaID:   sagaID,
		Side:     side,
		Price:    price,
		Qty:      qty,
		PlacedAt: time.Now(),
	}

	go t.resolveOrderID(ctx, sagaID)
}

func (t *OrderTracker) OrdersBySide(side orderbookv1.Side) []string {
	var sagaIDs []string
	for sagaID, tracked := range t.Orders {
		if tracked.Side == side {
			sagaIDs = append(sagaIDs, sagaID)
		}
	}
	return sagaIDs
}

func (t *OrderTracker) CleanupOrphans(ctx context.Context, accountID string) {
	resp, err := t.PfClient.GetPortfolio(ctx, connect.NewRequest(&portfoliov1.GetPortfolioRequest{
		AccountId: accountID,
	}))
	if err != nil {
		t.Log.Error("failed to get portfolio for orphan cleanup", "error", err)
		return
	}

	for _, po := range resp.Msg.PendingOrders {
		if po.Symbol != t.Symbol {
			continue
		}
		if po.Status != portfoliov1.PendingOrderStatus_PENDING_ORDER_STATUS_ORDER_PLACED {
			continue
		}

		statusResp, err := t.PfClient.GetOrderStatus(ctx, connect.NewRequest(&portfoliov1.GetOrderStatusRequest{
			SagaId: po.SagaId,
		}))
		if err != nil {
			t.Log.Error("failed to get orphan order status", "saga_id", po.SagaId, "error", err)
			continue
		}
		if statusResp.Msg.OrderId == "" {
			continue
		}

		_, err = t.ObClient.CancelOrder(ctx, connect.NewRequest(&orderbookv1.CancelOrderRequest{
			Symbol:  t.Symbol,
			OrderId: statusResp.Msg.OrderId,
		}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			t.Log.Error("failed to cancel orphan", "order_id", statusResp.Msg.OrderId, "error", err)
		} else {
			t.Log.Info("cancelled orphan order", "order_id", statusResp.Msg.OrderId)
		}
	}
}

func (t *OrderTracker) ExpireStaleOrders(ctx context.Context, timeout time.Duration) {
	for sagaID, tracked := range t.Orders {
		if time.Since(tracked.PlacedAt) < timeout {
			continue
		}
		orderID := tracked.OrderID
		if orderID == "" {
			continue
		}
		_, err := t.ObClient.CancelOrder(ctx, connect.NewRequest(&orderbookv1.CancelOrderRequest{
			Symbol:  t.Symbol,
			OrderId: orderID,
		}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			t.Log.Error("failed to expire order", "order_id", orderID, "error", err)
			continue
		}
		t.Log.Info("expired stale order", "order_id", orderID, "age", time.Since(tracked.PlacedAt))
		delete(t.OrderIDIndex, orderID)
		delete(t.Orders, sagaID)
	}
}

func (t *OrderTracker) DrainResolves() {
	for {
		select {
		case res := <-t.ResolveCh:
			t.HandleResolve(res)
		default:
			return
		}
	}
}

func (t *OrderTracker) resolveOrderID(ctx context.Context, sagaID string) {
	backoff := 100 * time.Millisecond
	maxBackoff := 1 * time.Second

	for range 20 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		resp, err := t.PfClient.GetOrderStatus(ctx, connect.NewRequest(&portfoliov1.GetOrderStatusRequest{
			SagaId: sagaID,
		}))
		if err != nil {
			t.Log.Debug("polling order status", "saga_id", sagaID, "error", err)
			continue
		}

		switch resp.Msg.Status {
		case portfoliov1.OrderStatus_ORDER_STATUS_ORDER_PLACED,
			portfoliov1.OrderStatus_ORDER_STATUS_COMPLETED:
			if resp.Msg.OrderId != "" {
				t.ResolveCh <- ResolveResult{SagaID: sagaID, OrderID: resp.Msg.OrderId}
				return
			}
		case portfoliov1.OrderStatus_ORDER_STATUS_FAILED:
			t.ResolveCh <- ResolveResult{SagaID: sagaID, Failed: true}
			return
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	t.Log.Error("gave up resolving order_id", "saga_id", sagaID)
	t.ResolveCh <- ResolveResult{SagaID: sagaID, Failed: true}
}
