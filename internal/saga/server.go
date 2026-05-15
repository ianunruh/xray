package saga

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

type Server struct {
	orderbookv1connect.UnimplementedSagaServiceHandler

	sagaHandler      *es.Handler[*BracketSaga]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	log              *slog.Logger
}

func NewServer(sagaHandler *es.Handler[*BracketSaga], orderbookHandler *es.Handler[*orderbook.OrderBook], log *slog.Logger) *Server {
	return &Server{
		sagaHandler:      sagaHandler,
		orderbookHandler: orderbookHandler,
		log:              log,
	}
}

func (s *Server) PlaceBracketOrder(ctx context.Context, req *connect.Request[orderbookv1.PlaceBracketOrderRequest]) (*connect.Response[orderbookv1.PlaceBracketOrderResponse], error) {
	msg := req.Msg

	if msg.Price <= 0 || msg.TakeProfitPrice <= 0 || msg.StopLossPrice <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrInvalidPrice)
	}
	if msg.Quantity <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrInvalidQuantity)
	}

	sagaID := NewSagaID()
	entryOrderID := uuid.New().String()

	// Create the saga first so the reactor has the order mapping before any
	// trades can occur.
	startCmd := StartSaga{
		SagaID:          sagaID,
		Symbol:          msg.Symbol,
		EntrySide:       msg.Side,
		EntryPrice:      msg.Price,
		EntryQty:        msg.Quantity,
		TakeProfitPrice: msg.TakeProfitPrice,
		StopLossPrice:   msg.StopLossPrice,
		EntryOrderID:    entryOrderID,
	}

	err := s.sagaHandler.Handle(ctx, startCmd, func(saga *BracketSaga) ([]es.Event, error) {
		return ExecuteStartSaga(saga, startCmd)
	})
	if err != nil {
		s.log.Error("PlaceBracketOrder: saga creation failed", "saga_id", sagaID, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Place the entry order on the order book.
	placeCmd := orderbook.PlaceOrder{
		Symbol:      msg.Symbol,
		Side:        orderbook.SideFromProto(msg.Side),
		Price:       msg.Price,
		Quantity:    msg.Quantity,
		OrderType:   orderbook.Limit,
		TimeInForce: orderbook.GTC,
		OrderID:     entryOrderID,
	}

	err = s.orderbookHandler.Handle(ctx, placeCmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecutePlaceOrder(book, placeCmd)
	})
	if err != nil {
		s.log.Warn("PlaceBracketOrder: entry order failed, failing saga", "saga_id", sagaID, "symbol", msg.Symbol, "error", err)
		failCmd := RecordSagaFailed{SagaID: sagaID, Reason: "entry order placement failed"}
		if failErr := s.sagaHandler.Handle(ctx, failCmd, func(saga *BracketSaga) ([]es.Event, error) {
			return ExecuteRecordSagaFailed(saga, failCmd)
		}); failErr != nil {
			s.log.Error("PlaceBracketOrder: failed to record saga failure", "saga_id", sagaID, "error", failErr)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.log.Info("PlaceBracketOrder",
		"saga_id", sagaID,
		"symbol", msg.Symbol,
		"entry_order_id", entryOrderID,
		"side", msg.Side,
		"price", msg.Price,
		"quantity", msg.Quantity,
		"take_profit", msg.TakeProfitPrice,
		"stop_loss", msg.StopLossPrice)

	resp := &orderbookv1.PlaceBracketOrderResponse{
		SagaId:       sagaID,
		EntryOrderId: entryOrderID,
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) GetSaga(ctx context.Context, req *connect.Request[orderbookv1.GetSagaRequest]) (*connect.Response[orderbookv1.GetSagaResponse], error) {
	saga, err := s.sagaHandler.Load(ctx, AggregateID(req.Msg.SagaId))
	if err != nil {
		s.log.Error("GetSaga failed", "saga_id", req.Msg.SagaId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if saga.Version() == 0 {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}

	resp := &orderbookv1.GetSagaResponse{
		SagaId:            saga.SagaID,
		Type:              orderbookv1.SagaType_SAGA_TYPE_BRACKET_ORDER,
		Status:            sagaStatusToProto(saga.Status),
		Symbol:            saga.Symbol,
		EntrySide:         orderbook.SideToProto(saga.EntrySide),
		EntryPrice:        saga.EntryPrice,
		EntryQuantity:     saga.EntryQty,
		TakeProfitPrice:   saga.TakeProfitPrice,
		StopLossPrice:     saga.StopLossPrice,
		EntryOrderId:      saga.EntryOrderID,
		TakeProfitOrderId: saga.TakeProfitOrderID,
		StopLossOrderId:   saga.StopLossOrderID,
	}

	s.log.Info("GetSaga", "saga_id", req.Msg.SagaId, "status", resp.Status)
	return connect.NewResponse(resp), nil
}

func sagaStatusToProto(s Status) orderbookv1.SagaStatus {
	switch s {
	case PendingEntry:
		return orderbookv1.SagaStatus_SAGA_STATUS_PENDING_ENTRY
	case PendingExit:
		return orderbookv1.SagaStatus_SAGA_STATUS_PENDING_EXIT
	case Completed:
		return orderbookv1.SagaStatus_SAGA_STATUS_COMPLETED
	case Failed:
		return orderbookv1.SagaStatus_SAGA_STATUS_FAILED
	case Uninitialized:
		return orderbookv1.SagaStatus_SAGA_STATUS_UNSPECIFIED
	default:
		return orderbookv1.SagaStatus_SAGA_STATUS_UNSPECIFIED
	}
}
