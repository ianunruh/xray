package sagasvc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	sagav1 "github.com/ianunruh/xray/gen/saga/v1"
	"github.com/ianunruh/xray/gen/saga/v1/sagav1connect"
	"github.com/ianunruh/xray/internal/bracket"
	"github.com/ianunruh/xray/internal/ocosaga"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/internal/ordersaga"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/pkg/es"
)

// Server implements the unified SagaService. It dispatches Place to the
// right internal aggregate, looks up kind via the projection for Get / List /
// Cancel, and projects each aggregate's state into the unified response.
type Server struct {
	sagav1connect.UnimplementedSagaServiceHandler

	orderSagaHandler *es.Handler[*ordersaga.OrderSaga]
	bracketHandler   *es.Handler[*bracket.BracketSaga]
	ocoSagaHandler   *es.Handler[*ocosaga.OCOSaga]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	projection       *PgProjection
	log              *slog.Logger
}

func NewServer(
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga],
	bracketHandler *es.Handler[*bracket.BracketSaga],
	ocoSagaHandler *es.Handler[*ocosaga.OCOSaga],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	portfolioHandler *es.Handler[*portfolio.Portfolio],
	projection *PgProjection,
	log *slog.Logger,
) *Server {
	return &Server{
		orderSagaHandler: orderSagaHandler,
		bracketHandler:   bracketHandler,
		ocoSagaHandler:   ocoSagaHandler,
		orderbookHandler: orderbookHandler,
		portfolioHandler: portfolioHandler,
		projection:       projection,
		log:              log,
	}
}

func (s *Server) Place(ctx context.Context, req *connect.Request[sagav1.PlaceSagaRequest]) (*connect.Response[sagav1.PlaceSagaResponse], error) {
	msg := req.Msg
	if msg.AccountId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("account_id required"))
	}

	ctx, correlationID := es.NewCorrelation(ctx)
	s.log.Info("Place", "account_id", msg.AccountId, "correlation_id", correlationID)

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
	case *sagav1.PlaceSagaRequest_Oco:
		sagaID, err := s.placeOCO(ctx, msg.AccountId, plan.Oco)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&sagav1.PlaceSagaResponse{SagaId: sagaID}), nil
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("plan required"))
	}
}

func (s *Server) placeOCO(ctx context.Context, accountID string, plan *sagav1.OCOPlan) (string, error) {
	if plan.Quantity <= 0 {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("quantity must be positive"))
	}
	if plan.TakeProfitPrice <= 0 || plan.StopLossPrice <= 0 {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("take_profit_price and stop_loss_price must be positive"))
	}
	sagaID := ocosaga.NewSagaID()
	cmd := ocosaga.StartOCOSaga{
		SagaID:          sagaID,
		AccountID:       accountID,
		Symbol:          plan.Symbol,
		ExitSide:        plan.ExitSide,
		Quantity:        plan.Quantity,
		TakeProfitPrice: plan.TakeProfitPrice,
		StopLossPrice:   plan.StopLossPrice,
		PositionSide:    plan.PositionSide,
	}
	if err := s.ocoSagaHandler.Handle(ctx, cmd, func(saga *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteStartOCOSaga(saga, cmd)
	}); err != nil {
		s.log.Error("Place(oco) saga creation failed", "saga_id", sagaID, "error", err)
		return "", connect.NewError(connect.CodeInternal, err)
	}
	return sagaID, nil
}

// guardMarginCall blocks user-initiated orders that would grow
// exposure (BUY+LONG or SELL+SHORT) while the account has an active
// margin call. Reducing-exposure orders pass through — the user
// should still be able to sell longs or cover shorts to fix the
// breach themselves. Liquidation sagas don't hit this path; they
// don't go through Place.
func (s *Server) guardMarginCall(ctx context.Context, accountID string, side orderbookv1.Side, ps orderbookv1.PositionSide) error {
	if !portfolio.IsExposureAdding(side, ps) {
		return nil
	}
	p, err := s.portfolioHandler.Load(ctx, portfolio.AggregateID(accountID))
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("load portfolio: %w", err))
	}
	if p.ActiveMarginCall == nil {
		return nil
	}
	return connect.NewError(connect.CodeFailedPrecondition,
		errors.New("account is in margin call; only reducing-exposure orders allowed (sell long, cover short)"))
}

func (s *Server) placeSingleOrder(ctx context.Context, accountID string, plan *sagav1.SingleOrderPlan) (string, error) {
	if err := s.guardMarginCall(ctx, accountID, plan.Side, plan.PositionSide); err != nil {
		return "", err
	}
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
		PositionSide:   plan.PositionSide,
		Initiator:      sagav1.Initiator_INITIATOR_USER,
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
	if err := s.guardMarginCall(ctx, accountID, plan.EntrySide, plan.PositionSide); err != nil {
		return "", err
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
		PositionSide:    plan.PositionSide,
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

	ctx, correlationID := es.NewCorrelation(ctx)
	s.log.Info("Cancel", "saga_id", row.SagaID, "kind", row.Kind, "correlation_id", correlationID)

	switch row.Kind {
	case sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER:
		if err := s.cancelSingleOrder(ctx, row); err != nil {
			return nil, err
		}
	case sagav1.SagaKind_SAGA_KIND_BRACKET:
		if err := s.cancelBracket(ctx, row); err != nil {
			return nil, err
		}
	case sagav1.SagaKind_SAGA_KIND_OCO:
		if err := s.cancelOCO(ctx, row); err != nil {
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
	// failure path, which cascades into the bracket via
	// onEntryOrderSagaFailed.
	// PendingExit: cancel the child OCO saga, which cancels TP+SL,
	// releases the share hold, and emits OCOSagaFailed — the bracket
	// reactor observes that and marks the bracket as Failed.
	switch b.Status {
	case bracket.PendingEntry:
		entryOrderID := ordersaga.OrderID(bracket.EntryOrderSagaID(row.SagaID))
		if b.EntryOrderID != "" {
			entryOrderID = b.EntryOrderID
		}
		return s.cancelOrderbookOrder(ctx, row.Symbol, entryOrderID)
	case bracket.PendingExit:
		return s.cancelOCOByID(ctx, bracket.ExitOCOSagaID(row.SagaID), row.Symbol)
	default:
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("bracket %s is in unexpected state for cancel", row.SagaID))
	}
}

func (s *Server) cancelOCO(ctx context.Context, row *SagaRow) error {
	return s.cancelOCOByID(ctx, row.SagaID, row.Symbol)
}

// cancelOCOByID cancels any OCO saga by ID — used directly by top-level
// OCO cancellation and indirectly by bracket cancellation (which
// targets the child OCO).
func (s *Server) cancelOCOByID(ctx context.Context, sagaID, symbol string) error {
	o, err := s.ocoSagaHandler.Load(ctx, ocosaga.AggregateID(sagaID))
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("load oco saga: %w", err))
	}
	// Best-effort cancel both legs (they may not be placed yet if we're
	// still in Started/SharesHeld). Idempotency in the orderbook makes
	// already-gone errors benign.
	if o.TakeProfitOrderID != "" {
		if err := s.cancelOrderbookOrder(ctx, symbol, o.TakeProfitOrderID); err != nil {
			return err
		}
	}
	if o.StopLossOrderID != "" {
		if err := s.cancelOrderbookOrder(ctx, symbol, o.StopLossOrderID); err != nil {
			return err
		}
	}
	failCmd := ocosaga.RecordFailed{SagaID: sagaID, Reason: "cancelled by user"}
	if err := s.ocoSagaHandler.Handle(ctx, failCmd, func(saga *ocosaga.OCOSaga) ([]es.Event, error) {
		return ocosaga.ExecuteRecordFailed(saga, failCmd)
	}); err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("record oco saga failed: %w", err))
	}
	return nil
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
	case sagav1.SagaKind_SAGA_KIND_OCO:
		details, err := s.ocoDetails(ctx, row.SagaID)
		if err != nil {
			return nil, err
		}
		resp.Details = &sagav1.GetSagaResponse_Oco{Oco: details}
	}
	return resp, nil
}

func (s *Server) ocoDetails(ctx context.Context, sagaID string) (*sagav1.OCODetails, error) {
	o, err := s.ocoSagaHandler.Load(ctx, ocosaga.AggregateID(sagaID))
	if err != nil {
		return nil, fmt.Errorf("load oco saga: %w", err)
	}
	return &sagav1.OCODetails{
		Phase:             ocoPhase(o.Status),
		ExitSide:          orderbook.SideToProto(o.ExitSide),
		Quantity:          o.Quantity,
		TakeProfitPrice:   o.TakeProfitPrice,
		StopLossPrice:     o.StopLossPrice,
		TakeProfitOrderId: o.TakeProfitOrderID,
		StopLossOrderId:   o.StopLossOrderID,
		SettledQuantity:   o.SettledQty,
		PositionSide:      o.PositionSide,
	}, nil
}

func ocoPhase(s ocosaga.Status) sagav1.OCOPhase {
	switch s {
	case ocosaga.Started:
		return sagav1.OCOPhase_OCO_PHASE_STARTED
	case ocosaga.SharesHeld:
		return sagav1.OCOPhase_OCO_PHASE_SHARES_HELD
	case ocosaga.ExitPlaced:
		return sagav1.OCOPhase_OCO_PHASE_EXIT_PLACED
	default:
		return sagav1.OCOPhase_OCO_PHASE_UNSPECIFIED
	}
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
		PositionSide:   saga.PositionSide,
		Initiator:      saga.Initiator,
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
		PositionSide:      b.PositionSide,
	}, nil
}

func orderSagaPhase(s ordersaga.Status) sagav1.SingleOrderPhase {
	switch s {
	case ordersaga.Started:
		return sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_STARTED
	case ordersaga.CashHeld:
		return sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_CASH_HELD
	case ordersaga.CollateralHeld:
		return sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_COLLATERAL_HELD
	case ordersaga.SharesHeld:
		return sagav1.SingleOrderPhase_SINGLE_ORDER_PHASE_SHARES_HELD
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
