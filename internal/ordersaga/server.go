package ordersaga

import (
	"context"

	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
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
