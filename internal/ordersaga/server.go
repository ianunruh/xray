package ordersaga

import (
	"context"
	"fmt"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

func NewPlaceOrderFunc(handler *es.Handler[*OrderSaga]) portfolio.PlaceOrderFunc {
	return func(ctx context.Context, req *portfoliov1.PortfolioPlaceOrderRequest) (string, error) {
		sagaID := NewSagaID()
		cmd := StartOrderSaga{
			SagaID:      sagaID,
			AccountID:   req.AccountId,
			Symbol:      req.Symbol,
			Side:        req.Side,
			Price:       req.Price,
			Quantity:    req.Quantity,
			OrderType:   req.OrderType,
			TimeInForce: req.TimeInForce,
		}
		err := handler.Handle(ctx, cmd, func(saga *OrderSaga) ([]es.Event, error) {
			return ExecuteStartOrderSaga(saga, cmd)
		})
		if err != nil {
			return "", err
		}
		return sagaID, nil
	}
}

func NewGetOrderStatusFunc(handler *es.Handler[*OrderSaga]) portfolio.GetOrderStatusFunc {
	return func(ctx context.Context, sagaID string) (*portfoliov1.GetOrderStatusResponse, error) {
		saga, err := handler.Load(ctx, AggregateID(sagaID))
		if err != nil {
			return nil, fmt.Errorf("load order saga: %w", err)
		}
		if saga.Version() == 0 {
			return nil, fmt.Errorf("order saga not found: %s", sagaID)
		}

		return &portfoliov1.GetOrderStatusResponse{
			SagaId:         saga.SagaID,
			AccountId:      saga.AccountID,
			Symbol:         saga.Symbol,
			Side:           orderbook.SideToProto(saga.Side),
			Price:          saga.Price,
			Quantity:       saga.Quantity,
			OrderType:      orderbook.OrderTypeToProto(saga.OrderType),
			TimeInForce:    orderbook.TimeInForceToProto(saga.TimeInForce),
			Status:         statusToProto(saga.Status),
			FilledQuantity: saga.FilledQty,
			AmountHeld:     saga.AmountHeld,
			CashSettled:    saga.CashSettled,
			OrderId:        saga.OrderID,
		}, nil
	}
}

func statusToProto(s Status) portfoliov1.OrderStatus {
	switch s {
	case Started:
		return portfoliov1.OrderStatus_ORDER_STATUS_STARTED
	case CashHeld:
		return portfoliov1.OrderStatus_ORDER_STATUS_CASH_HELD
	case OrderPlaced:
		return portfoliov1.OrderStatus_ORDER_STATUS_ORDER_PLACED
	case Completed:
		return portfoliov1.OrderStatus_ORDER_STATUS_COMPLETED
	case Failed:
		return portfoliov1.OrderStatus_ORDER_STATUS_FAILED
	default:
		return portfoliov1.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}
