package trader

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/gen/saga/v1/sagav1connect"
)

type TrackedOrder struct {
	SagaID   string
	OrderID  string
	Side     orderbookv1.Side
	Price    int64
	Qty      int64
	PlacedAt time.Time
}

type ResolveResult struct {
	SagaID  string
	OrderID string
	Failed  bool
}

type OrderTracker struct {
	Symbol     string
	ObClient   orderbookv1connect.OrderBookServiceClient
	SagaClient sagav1connect.SagaServiceClient
	Log        *slog.Logger

	Orders       map[string]*TrackedOrder
	OrderIDIndex map[string]string
	ResolveCh    chan ResolveResult
}

func NewOrderTracker(
	symbol string,
	obClient orderbookv1connect.OrderBookServiceClient,
	sagaClient sagav1connect.SagaServiceClient,
	log *slog.Logger,
) *OrderTracker {
	return &OrderTracker{
		Symbol:       symbol,
		ObClient:     obClient,
		SagaClient:   sagaClient,
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
	resp, err := t.SagaClient.Place(ctx, connect.NewRequest(&sagav1.PlaceSagaRequest{
		AccountId: accountID,
		Plan: &sagav1.PlaceSagaRequest_SingleOrder{
			SingleOrder: &sagav1.SingleOrderPlan{
				Symbol:      t.Symbol,
				Side:        side,
				Price:       price,
				Quantity:    qty,
				OrderType:   orderbookv1.OrderType_ORDER_TYPE_LIMIT,
				TimeInForce: orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
			},
		},
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

// RecognizeFill reports whether trade involves one of our tracked orders,
// logging it as a fill when it does. Callers decide how to react (e.g. drop
// the filled order, requote, or re-evaluate a signal).
func (t *OrderTracker) RecognizeFill(trade *orderbookv1.Trade) bool {
	if !t.IsOwnTrade(trade) {
		return false
	}
	t.Log.Info("fill detected",
		"trade_id", trade.TradeId,
		"price", trade.Price,
		"quantity", trade.Quantity)
	return true
}

func (t *OrderTracker) CancelTracked(ctx context.Context, sagaID string) {
	tracked, ok := t.Orders[sagaID]
	if !ok {
		return
	}
	// SagaService.Cancel handles "find the order id" and "send the cancel"
	// in one call — no need to fall back to GetOrderStatus + CancelOrder.
	_, err := t.SagaClient.Cancel(ctx, connect.NewRequest(&sagav1.CancelSagaRequest{
		SagaId: sagaID,
	}))
	if err != nil && connect.CodeOf(err) != connect.CodeNotFound && connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Log.Error("failed to cancel saga", "saga_id", sagaID, "error", err)
	} else {
		t.Log.Info("cancelled saga", "saga_id", sagaID, "order_id", tracked.OrderID)
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

// Shutdown drains any pending order-ID resolutions and cancels every order
// still resting in the book. Engines call this from their ctx.Done() branch.
// It uses a background context so the cancels still go through after the
// engine's own context has been cancelled.
func (t *OrderTracker) Shutdown() {
	t.DrainResolves()
	t.Log.Info("shutting down, cancelling orders", "tracked_orders", len(t.Orders))
	t.CancelAll(context.Background())
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

	resp, err := t.SagaClient.Place(ctx, connect.NewRequest(&sagav1.PlaceSagaRequest{
		AccountId: accountID,
		Plan: &sagav1.PlaceSagaRequest_SingleOrder{
			SingleOrder: &sagav1.SingleOrderPlan{
				Symbol:         t.Symbol,
				Side:           side,
				Price:          price,
				Quantity:       qty,
				OrderType:      orderbookv1.OrderType_ORDER_TYPE_LIMIT,
				TimeInForce:    orderbookv1.TimeInForce_TIME_IN_FORCE_GTC,
				ReplaceOrderId: oldOrderID,
			},
		},
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
	resp, err := t.SagaClient.List(ctx, connect.NewRequest(&sagav1.ListSagasRequest{
		AccountId: accountID,
		Symbol:    t.Symbol,
		Kind:      sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER,
		Status:    sagav1.SagaStatus_SAGA_STATUS_ACTIVE,
	}))
	if err != nil {
		t.Log.Error("failed to list active sagas for orphan cleanup", "error", err)
		return
	}

	for _, saga := range resp.Msg.Sagas {
		details := saga.GetSingleOrder()
		if details == nil || details.Phase != sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_ORDER_PLACED {
			continue
		}
		if _, err := t.SagaClient.Cancel(ctx, connect.NewRequest(&sagav1.CancelSagaRequest{
			SagaId: saga.SagaId,
		})); err != nil && connect.CodeOf(err) != connect.CodeNotFound && connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Log.Error("failed to cancel orphan", "saga_id", saga.SagaId, "error", err)
		} else {
			t.Log.Info("cancelled orphan saga", "saga_id", saga.SagaId, "order_id", details.OrderId)
		}
	}
}

func (t *OrderTracker) ExpireStaleOrders(ctx context.Context, timeout time.Duration) {
	for sagaID, tracked := range t.Orders {
		if time.Since(tracked.PlacedAt) < timeout {
			continue
		}
		if tracked.OrderID == "" {
			continue
		}
		_, err := t.SagaClient.Cancel(ctx, connect.NewRequest(&sagav1.CancelSagaRequest{
			SagaId: sagaID,
		}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound && connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Log.Error("failed to expire saga", "saga_id", sagaID, "error", err)
			continue
		}
		t.Log.Info("expired stale order", "saga_id", sagaID, "order_id", tracked.OrderID, "age", time.Since(tracked.PlacedAt))
		delete(t.OrderIDIndex, tracked.OrderID)
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

		resp, err := t.SagaClient.Get(ctx, connect.NewRequest(&sagav1.GetSagaRequest{
			SagaId: sagaID,
		}))
		if err != nil {
			t.Log.Debug("polling saga", "saga_id", sagaID, "error", err)
			continue
		}

		details := resp.Msg.GetSingleOrder()
		switch resp.Msg.Status {
		case sagav1.SagaStatus_SAGA_STATUS_ACTIVE:
			if details != nil && details.Phase == sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_ORDER_PLACED && details.OrderId != "" {
				t.ResolveCh <- ResolveResult{SagaID: sagaID, OrderID: details.OrderId}
				return
			}
		case sagav1.SagaStatus_SAGA_STATUS_COMPLETED:
			if details != nil && details.OrderId != "" {
				t.ResolveCh <- ResolveResult{SagaID: sagaID, OrderID: details.OrderId}
				return
			}
		case sagav1.SagaStatus_SAGA_STATUS_FAILED:
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
