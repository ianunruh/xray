package sagasvc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/gen/saga/v1/sagav1connect"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/pkg/es"
)

// Server implements the unified SagaService. It dispatches Place to the
// right internal aggregate, looks up kind via the projection for Get / List /
// Cancel, and projects each aggregate's state into the unified response.
type Server struct {
	sagav1connect.UnimplementedSagaServiceHandler

	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	bracketHandler   *es.Handler[*bracket.BracketSaga]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	projection       *PgProjection
	log              *slog.Logger
}

func NewServer(
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga],
	bracketHandler *es.Handler[*bracket.BracketSaga],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	projection *PgProjection,
	log *slog.Logger,
) *Server {
	return &Server{
		orderSagaHandler: orderSagaHandler,
		bracketHandler:   bracketHandler,
		orderbookHandler: orderbookHandler,
		projection:       projection,
		log:              log,
	}
}

func (s *Server) Place(ctx context.Context, req *connect.Request[sagav1.PlaceSagaRequest]) (*connect.Response[sagav1.PlaceSagaResponse], error) {
	msg := req.Msg
	if msg.AccountId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("account_id required"))
	}

	switch plan := msg.Plan.(type) {
	case *sagav1.PlaceSagaRequest_SingleOrder:
		sagaID, err := s.placeSingleOrder(ctx, msg.AccountId, plan.SingleOrder)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&sagav1.PlaceSagaResponse{SagaId: sagaID}), nil
	case *sagav1.PlaceSagaRequest_Bracket:
		sagaID, err := s.placeBracket(ctx, msg.AccountId, plan.Bracket)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&sagav1.PlaceSagaResponse{SagaId: sagaID}), nil
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("plan required"))
	}
}

func (s *Server) placeSingleOrder(ctx context.Context, accountID string, plan *sagav1.SingleOrderPlan) (string, error) {
	sagaID := ordersaga.NewSagaID()
	cmd := ordersaga.StartOrderSaga{
		SagaID:         sagaID,
		AccountID:      accountID,
		Symbol:         plan.Symbol,
		Side:           plan.Side,
		Price:          plan.Price,
		Quantity:       plan.Quantity,
		OrderType:      plan.OrderType,
		TimeInForce:    plan.TimeInForce,
		ReplaceOrderID: plan.ReplaceOrderId,
	}
	err := s.orderSagaHandler.Handle(ctx, cmd, func(saga *ordersaga.OrderSaga) ([]es.Event, error) {
		return ordersaga.ExecuteStartOrderSaga(saga, cmd)
	})
	if err != nil {
		s.log.Error("Place(single_order) failed", "saga_id", sagaID, "error", err)
		return "", connect.NewError(connect.CodeInternal, err)
	}
	return sagaID, nil
}

func (s *Server) placeBracket(ctx context.Context, accountID string, plan *sagav1.BracketPlan) (string, error) {
	if plan.EntryPrice <= 0 || plan.TakeProfitPrice <= 0 || plan.StopLossPrice <= 0 {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("entry_price, take_profit_price, stop_loss_price must be positive"))
	}
	if plan.EntryQuantity <= 0 {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("entry_quantity must be positive"))
	}

	sagaID := bracket.NewSagaID()
	startCmd := bracket.StartSaga{
		SagaID:          sagaID,
		AccountID:       accountID,
		Symbol:          plan.Symbol,
		EntrySide:       plan.EntrySide,
		EntryPrice:      plan.EntryPrice,
		EntryQty:        plan.EntryQuantity,
		TakeProfitPrice: plan.TakeProfitPrice,
		StopLossPrice:   plan.StopLossPrice,
	}
	if err := s.bracketHandler.Handle(ctx, startCmd, func(b *bracket.BracketSaga) ([]es.Event, error) {
		return bracket.ExecuteStartSaga(b, startCmd)
	}); err != nil {
		s.log.Error("Place(bracket) saga creation failed", "saga_id", sagaID, "error", err)
		return "", connect.NewError(connect.CodeInternal, err)
	}

	// The bracket reactor spawns the entry ordersaga once it observes
	// SagaStarted; nothing to do here beyond creating the saga aggregate.
	return sagaID, nil
}

func (s *Server) Get(ctx context.Context, req *connect.Request[sagav1.GetSagaRequest]) (*connect.Response[sagav1.GetSagaResponse], error) {
	row, err := s.projection.Get(ctx, req.Msg.SagaId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if row == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("saga %s not found", req.Msg.SagaId))
	}

	resp, err := s.buildGetResponse(ctx, row)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) List(ctx context.Context, req *connect.Request[sagav1.ListSagasRequest]) (*connect.Response[sagav1.ListSagasResponse], error) {
	rows, err := s.projection.List(ctx, req.Msg.AccountId, req.Msg.Symbol, req.Msg.Kind, req.Msg.Status)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &sagav1.ListSagasResponse{}
	for _, row := range rows {
		entry, err := s.buildGetResponse(ctx, row)
		if err != nil {
			s.log.Warn("List: failed to build saga response", "saga_id", row.SagaID, "error", err)
			continue
		}
		resp.Sagas = append(resp.Sagas, entry)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) Cancel(ctx context.Context, req *connect.Request[sagav1.CancelSagaRequest]) (*connect.Response[sagav1.CancelSagaResponse], error) {
	row, err := s.projection.Get(ctx, req.Msg.SagaId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if row == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("saga %s not found", req.Msg.SagaId))
	}
	if row.Status != sagav1.SagaStatus_SAGA_STATUS_ACTIVE {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("saga %s is %s, cannot cancel", row.SagaID, row.Status))
	}

	switch row.Kind {
	case sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER:
		if err := s.cancelSingleOrder(ctx, row); err != nil {
			return nil, err
		}
	case sagav1.SagaKind_SAGA_KIND_BRACKET:
		if err := s.cancelBracket(ctx, row); err != nil {
			return nil, err
		}
	default:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unknown saga kind: %d", row.Kind))
	}
	return connect.NewResponse(&sagav1.CancelSagaResponse{}), nil
}

func (s *Server) cancelSingleOrder(ctx context.Context, row *SagaRow) error {
	saga, err := s.orderSagaHandler.Load(ctx, ordersaga.AggregateID(row.SagaID))
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("load saga: %w", err))
	}
	if saga.OrderID == "" {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("saga %s has not placed an order yet", row.SagaID))
	}
	return s.cancelOrderbookOrder(ctx, row.Symbol, saga.OrderID)
}

func (s *Server) cancelBracket(ctx context.Context, row *SagaRow) error {
	b, err := s.bracketHandler.Load(ctx, bracket.AggregateID(row.SagaID))
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("load bracket: %w", err))
	}

	// PendingEntry: cancel the entry order. The entry is an ordersaga;
	// cancelling its orderbook order cascades into the entry ordersaga's
	// failure path, which cascades into the bracket.
	// PendingExit: cancel both exit legs AND record the bracket as
	// failed so the reactor can release the share hold.
	switch b.Status {
	case bracket.PendingEntry:
		entryOrderID := ordersaga.OrderID(bracket.EntryOrderSagaID(row.SagaID))
		if b.EntryOrderID != "" {
			entryOrderID = b.EntryOrderID
		}
		return s.cancelOrderbookOrder(ctx, row.Symbol, entryOrderID)
	case bracket.PendingExit:
		if err := s.cancelOrderbookOrder(ctx, row.Symbol, b.TakeProfitOrderID); err != nil {
			return err
		}
		if err := s.cancelOrderbookOrder(ctx, row.Symbol, b.StopLossOrderID); err != nil {
			return err
		}
		failCmd := bracket.RecordSagaFailed{SagaID: row.SagaID, Reason: "cancelled by user"}
		if err := s.bracketHandler.Handle(ctx, failCmd, func(b *bracket.BracketSaga) ([]es.Event, error) {
			return bracket.ExecuteRecordSagaFailed(b, failCmd)
		}); err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("record saga failed: %w", err))
		}
		return nil
	default:
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("bracket %s is in unexpected state for cancel", row.SagaID))
	}
}

func (s *Server) cancelOrderbookOrder(ctx context.Context, symbol, orderID string) error {
	cmd := orderbook.CancelOrder{Symbol: symbol, OrderID: orderID}
	err := s.orderbookHandler.Handle(ctx, cmd, func(book *orderbook.OrderBook) ([]es.Event, error) {
		return orderbook.ExecuteCancelOrder(book, cmd)
	})
	if err != nil {
		// Already-gone is a benign race — treat as success.
		if errors.Is(err, orderbook.ErrOrderNotFound) || errors.Is(err, orderbook.ErrNoRemainingQty) {
			return nil
		}
		return connect.NewError(connect.CodeInternal, fmt.Errorf("cancel %s: %w", orderID, err))
	}
	return nil
}

func (s *Server) buildGetResponse(ctx context.Context, row *SagaRow) (*sagav1.GetSagaResponse, error) {
	resp := &sagav1.GetSagaResponse{
		SagaId:     row.SagaID,
		Kind:       row.Kind,
		Status:     row.Status,
		AccountId:  row.AccountID,
		Symbol:     row.Symbol,
		StartedAt:  timestamppb.New(row.StartedAt),
		FailReason: row.FailReason,
	}
	if row.EndedAt != nil {
		resp.EndedAt = timestamppb.New(*row.EndedAt)
	}

	switch row.Kind {
	case sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER:
		details, err := s.singleOrderDetails(ctx, row.SagaID)
		if err != nil {
			return nil, err
		}
		resp.Details = &sagav1.GetSagaResponse_SingleOrder{SingleOrder: details}
	case sagav1.SagaKind_SAGA_KIND_BRACKET:
		details, err := s.bracketDetails(ctx, row.SagaID)
		if err != nil {
			return nil, err
		}
		resp.Details = &sagav1.GetSagaResponse_Bracket{Bracket: details}
	}
	return resp, nil
}

func (s *Server) singleOrderDetails(ctx context.Context, sagaID string) (*sagav1.SingleOrderDetails, error) {
	saga, err := s.orderSagaHandler.Load(ctx, ordersaga.AggregateID(sagaID))
	if err != nil {
		return nil, fmt.Errorf("load order saga: %w", err)
	}
	return &sagav1.SingleOrderDetails{
		Phase:          orderSagaPhase(saga.Status),
		Side:           orderbook.SideToProto(saga.Side),
		Price:          saga.Price,
		Quantity:       saga.Quantity,
		OrderType:      orderbook.OrderTypeToProto(saga.OrderType),
		TimeInForce:    orderbook.TimeInForceToProto(saga.TimeInForce),
		FilledQuantity: saga.FilledQty,
		AmountHeld:     saga.AmountHeld,
		CashSettled:    saga.CashSettled,
		OrderId:        saga.OrderID,
	}, nil
}

func (s *Server) bracketDetails(ctx context.Context, sagaID string) (*sagav1.BracketDetails, error) {
	b, err := s.bracketHandler.Load(ctx, bracket.AggregateID(sagaID))
	if err != nil {
		return nil, fmt.Errorf("load bracket saga: %w", err)
	}
	entryOrderID := b.EntryOrderID
	if entryOrderID == "" {
		// New-style bracket: entry is placed by an ordersaga whose
		// orderID is derived from the bracket's sagaID.
		entryOrderID = ordersaga.OrderID(sagaID)
	}
	return &sagav1.BracketDetails{
		Phase:             bracketPhase(b.Status),
		EntrySide:         orderbook.SideToProto(b.EntrySide),
		EntryPrice:        b.EntryPrice,
		EntryQuantity:     b.EntryQty,
		TakeProfitPrice:   b.TakeProfitPrice,
		StopLossPrice:     b.StopLossPrice,
		EntryOrderId:      entryOrderID,
		TakeProfitOrderId: b.TakeProfitOrderID,
		StopLossOrderId:   b.StopLossOrderID,
	}, nil
}

func orderSagaPhase(s ordersaga.Status) sagav1.SingleOrderPhase {
	switch s {
	case ordersaga.Started:
		return sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_STARTED
	case ordersaga.CashHeld:
		return sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_CASH_HELD
	case ordersaga.OrderPlaced:
		return sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_ORDER_PLACED
	default:
		return sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_UNSPECIFIED
	}
}

func bracketPhase(s bracket.Status) sagav1.BracketPhase {
	switch s {
	case bracket.PendingEntry:
		return sagav1.BracketPhase_BRACKET_PHASE_PENDING_ENTRY
	case bracket.PendingExit:
		return sagav1.BracketPhase_BRACKET_PHASE_PENDING_EXIT
	default:
		return sagav1.BracketPhase_BRACKET_PHASE_UNSPECIFIED
	}
}
