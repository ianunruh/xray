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
	"github.com/ianunruh/xray/internal/twapsaga"
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
	twapHandler      *es.Handler[*twapsaga.TWAPSaga]
	orderbookHandler *es.Handler[*orderbook.OrderBook]
	portfolioHandler *es.Handler[*portfolio.Portfolio]
	twapReactor      *twapsaga.Reactor
	marker           portfolio.Marker
	projection       *PgProjection
	log              *slog.Logger
}

func NewServer(
	orderSagaHandler *es.Handler[*ordersaga.OrderSaga],
	bracketHandler *es.Handler[*bracket.BracketSaga],
	ocoSagaHandler *es.Handler[*ocosaga.OCOSaga],
	twapHandler *es.Handler[*twapsaga.TWAPSaga],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	portfolioHandler *es.Handler[*portfolio.Portfolio],
	twapReactor *twapsaga.Reactor,
	marker portfolio.Marker,
	projection *PgProjection,
	log *slog.Logger,
) *Server {
	return &Server{
		orderSagaHandler: orderSagaHandler,
		bracketHandler:   bracketHandler,
		ocoSagaHandler:   ocoSagaHandler,
		twapHandler:      twapHandler,
		orderbookHandler: orderbookHandler,
		portfolioHandler: portfolioHandler,
		twapReactor:      twapReactor,
		marker:           marker,
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
	case *sagav1.PlaceSagaRequest_Twap:
		sagaID, err := s.placeTWAP(ctx, msg.AccountId, plan.Twap)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&sagav1.PlaceSagaResponse{SagaId: sagaID}), nil
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("plan required"))
	}
}

func (s *Server) placeTWAP(ctx context.Context, accountID string, plan *sagav1.TWAPPlan) (string, error) {
	if plan.TotalQuantity <= 0 {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("total_quantity must be positive"))
	}
	if plan.SliceCount <= 0 {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("slice_count must be positive"))
	}
	if plan.LimitPrice <= 0 {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("limit_price must be positive"))
	}
	if int64(plan.SliceCount) > plan.TotalQuantity {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("slice_count cannot exceed total_quantity"))
	}
	if err := s.guardMarginCall(ctx, accountID, plan.Side, plan.PositionSide); err != nil {
		return "", err
	}
	// Buying-power guard: check against the worst case (entire TWAP at
	// limit_price as one limit order). If the user can't afford the full
	// notional, fail at submit instead of mid-flight.
	if err := s.guardBuyingPower(ctx, accountID, portfolio.OrderPlan{
		Symbol:       plan.Symbol,
		Side:         plan.Side,
		PositionSide: plan.PositionSide,
		OrderType:    orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		Price:        plan.LimitPrice,
		Quantity:     plan.TotalQuantity,
	}); err != nil {
		return "", err
	}

	sagaID := twapsaga.NewSagaID()
	cmd := twapsaga.StartTWAPSaga{
		SagaID:          sagaID,
		AccountID:       accountID,
		Symbol:          plan.Symbol,
		Side:            plan.Side,
		PositionSide:    plan.PositionSide,
		TotalQuantity:   plan.TotalQuantity,
		SliceCount:      plan.SliceCount,
		SliceIntervalMs: plan.SliceIntervalMs,
		LimitPrice:      plan.LimitPrice,
		Initiator:       sagav1.Initiator_INITIATOR_USER,
	}
	if err := s.twapHandler.Handle(ctx, cmd, func(saga *twapsaga.TWAPSaga) ([]es.Event, error) {
		return twapsaga.ExecuteStartTWAPSaga(saga, cmd)
	}); err != nil {
		s.log.Error("Place(twap) saga creation failed", "saga_id", sagaID, "error", err)
		return "", connect.NewError(connect.CodeInternal, err)
	}
	return sagaID, nil
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

// guardBuyingPower rejects an order at submit time when it would
// require more buying power than the account has. Mirrors the
// PreviewOrderImpact computation so the UI's preview and the
// server's rejection use the same math. Reducing-exposure orders
// (impact = 0) always pass.
func (s *Server) guardBuyingPower(ctx context.Context, accountID string, plan portfolio.OrderPlan) error {
	p, err := s.portfolioHandler.Load(ctx, portfolio.AggregateID(accountID))
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("load portfolio: %w", err))
	}
	var book portfolio.BookEstimator
	if plan.OrderType == orderbookv1.OrderType_ORDER_TYPE_MARKET && s.orderbookHandler != nil {
		b, err := s.orderbookHandler.Load(ctx, orderbook.AggregateID(plan.Symbol))
		if err == nil {
			book = b
		}
	}
	impact := portfolio.ComputeOrderImpact(ctx, p, s.marker, book, plan)
	if !impact.SufficientBuyingPower {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("insufficient buying power: order would use %d, available %d",
				impact.BuyingPowerImpact,
				p.CashBalance), // user-facing context; exact BP can also be computed but cash is clearer
		)
	}
	return nil
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
	if err := s.guardBuyingPower(ctx, accountID, portfolio.OrderPlan{
		Symbol:       plan.Symbol,
		Side:         plan.Side,
		PositionSide: plan.PositionSide,
		OrderType:    plan.OrderType,
		Price:        plan.Price,
		Quantity:     plan.Quantity,
	}); err != nil {
		return "", err
	}
	sagaID := ordersaga.NewSagaID()
	cmd := ordersaga.StartOrderSaga{
		SagaID:         sagaID,
		AccountID:      accountID,
		Symbol:         plan.Symbol,
		Side:           plan.Side,
		Price:          plan.Price,
		StopPrice:      plan.StopPrice,
		Quantity:       plan.Quantity,
		DisplayQty:     plan.DisplayQuantity,
		TrailAmount:    plan.TrailAmount,
		TrailOffsetBps: plan.TrailOffsetBps,
		LimitOffset:    plan.LimitOffset,
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
	// Bracket's entry is the cash-consuming leg; check buying power
	// against that as a limit order (brackets are always limit entries).
	if err := s.guardBuyingPower(ctx, accountID, portfolio.OrderPlan{
		Symbol:       plan.Symbol,
		Side:         plan.EntrySide,
		PositionSide: plan.PositionSide,
		OrderType:    orderbookv1.OrderType_ORDER_TYPE_LIMIT,
		Price:        plan.EntryPrice,
		Quantity:     plan.EntryQuantity,
	}); err != nil {
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
	if err := s.CancelByID(ctx, req.Msg.SagaId); err != nil {
		return nil, err
	}
	return connect.NewResponse(&sagav1.CancelSagaResponse{}), nil
}

// CancelByID is the non-Connect entry point for cancellation. The
// corpaction coordinator drives this when a corporate action affects
// a symbol that has in-flight sagas. Returns connect-typed errors so
// the Connect handler can pass them through unchanged.
func (s *Server) CancelByID(ctx context.Context, sagaID string) error {
	row, err := s.projection.Get(ctx, sagaID)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if row == nil {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("saga %s not found", sagaID))
	}
	if row.Status != sagav1.SagaStatus_SAGA_STATUS_ACTIVE {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("saga %s is %s, cannot cancel", row.SagaID, row.Status))
	}

	ctx, correlationID := es.NewCorrelation(ctx)
	s.log.Info("Cancel", "saga_id", row.SagaID, "kind", row.Kind, "correlation_id", correlationID)

	switch row.Kind {
	case sagav1.SagaKind_SAGA_KIND_SINGLE_ORDER:
		return s.cancelSingleOrder(ctx, row)
	case sagav1.SagaKind_SAGA_KIND_BRACKET:
		return s.cancelBracket(ctx, row)
	case sagav1.SagaKind_SAGA_KIND_OCO:
		return s.cancelOCO(ctx, row)
	case sagav1.SagaKind_SAGA_KIND_TWAP:
		return s.cancelTWAP(ctx, row)
	default:
		return connect.NewError(connect.CodeInternal, fmt.Errorf("unknown saga kind: %d", row.Kind))
	}
}

// cancelTWAP marks the parent TWAP as failed (which suppresses future
// slice launches) and cancels any in-flight child slice's orderbook
// order. Already-completed slices stay settled — that's the point of
// incremental execution.
func (s *Server) cancelTWAP(ctx context.Context, row *SagaRow) error {
	t, err := s.twapHandler.Load(ctx, twapsaga.AggregateID(row.SagaID))
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("load twap: %w", err))
	}
	// Mark parent failed first so subsequent reconciler ticks won't
	// schedule new slices, even if the child cancellation below races.
	if err := s.twapReactor.MarkFailed(ctx, row.SagaID, "cancelled by user"); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	// Best-effort cancel of the current in-flight child slice. IOC slices
	// usually complete in one reactor cycle, so the active-child case is
	// the exception; idempotent on already-gone.
	if cur := t.CurrentSlice(); cur != nil && !cur.Completed {
		childOrderID := ordersaga.OrderID(cur.ChildSagaID)
		if err := s.cancelOrderbookOrder(ctx, row.Symbol, childOrderID); err != nil {
			s.log.Warn("cancelTWAP: child order cancel failed",
				"saga_id", row.SagaID, "child", cur.ChildSagaID, "error", err)
		}
	}
	return nil
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
	case sagav1.SagaKind_SAGA_KIND_TWAP:
		details, err := s.twapDetails(ctx, row.SagaID)
		if err != nil {
			return nil, err
		}
		resp.Details = &sagav1.GetSagaResponse_Twap{Twap: details}
	}
	return resp, nil
}

func (s *Server) twapDetails(ctx context.Context, sagaID string) (*sagav1.TWAPDetails, error) {
	t, err := s.twapHandler.Load(ctx, twapsaga.AggregateID(sagaID))
	if err != nil {
		return nil, fmt.Errorf("load twap saga: %w", err)
	}
	slices := make([]*sagav1.TWAPSliceDetails, 0, len(t.Slices))
	for i := range t.Slices {
		sl := &t.Slices[i]
		d := &sagav1.TWAPSliceDetails{
			SliceIndex:       sl.Index,
			ChildSagaId:      sl.ChildSagaID,
			LaunchedQuantity: sl.LaunchedQuantity,
			FilledQuantity:   sl.FilledQuantity,
			CashSettled:      sl.CashSettled,
			Completed:        sl.Completed,
		}
		if !sl.LaunchedAt.IsZero() {
			d.LaunchedAt = timestamppb.New(sl.LaunchedAt)
		}
		if !sl.CompletedAt.IsZero() {
			d.CompletedAt = timestamppb.New(sl.CompletedAt)
		}
		slices = append(slices, d)
	}
	return &sagav1.TWAPDetails{
		Phase:               twapPhase(t.Status),
		Side:                orderbook.SideToProto(t.Side),
		PositionSide:        t.PositionSide,
		TotalQuantity:       t.TotalQuantity,
		SliceCount:          t.SliceCount,
		SliceIntervalMs:     t.SliceIntervalMs,
		LimitPrice:          t.LimitPrice,
		SlicesLaunched:      t.SlicesLaunched(),
		TotalFilledQuantity: t.TotalFilled,
		TotalCashSettled:    t.TotalSettled,
		StartedAt:           timestamppb.New(t.StartedAt),
		Slices:              slices,
		Initiator:           t.Initiator,
	}, nil
}

func twapPhase(s twapsaga.Status) sagav1.TWAPPhase {
	switch s {
	case twapsaga.Active:
		return sagav1.TWAPPhase_TWAP_PHASE_ACTIVE
	case twapsaga.Completed:
		return sagav1.TWAPPhase_TWAP_PHASE_COMPLETED
	default:
		return sagav1.TWAPPhase_TWAP_PHASE_UNSPECIFIED
	}
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
	details := &sagav1.SingleOrderDetails{
		Phase:           orderSagaPhase(saga.Status),
		Side:            orderbook.SideToProto(saga.Side),
		Price:           saga.Price,
		Quantity:        saga.Quantity,
		DisplayQuantity: saga.DisplayQty,
		TrailAmount:     saga.TrailAmount,
		TrailOffsetBps:  saga.TrailOffsetBps,
		LimitOffset:     saga.LimitOffset,
		OrderType:       orderbook.OrderTypeToProto(saga.OrderType),
		TimeInForce:     orderbook.TimeInForceToProto(saga.TimeInForce),
		FilledQuantity:  saga.FilledQty,
		AmountHeld:      saga.AmountHeld,
		CashSettled:     saga.CashSettled,
		OrderId:         saga.OrderID,
		PositionSide:    saga.PositionSide,
		Initiator:       saga.Initiator,
	}
	// Surface the live (post-ratchet) stop price for trailing stops by
	// loading the orderbook aggregate. Lazy — only on Get, only when the
	// caller actually cares about a trailing stop.
	if saga.OrderID != "" && saga.OrderType.IsTrailingStop() {
		if book, err := s.orderbookHandler.Load(ctx, orderbook.AggregateID(saga.Symbol)); err == nil {
			if o := book.Orders[saga.OrderID]; o != nil {
				details.CurrentStopPrice = o.StopPrice
			}
		}
	}
	return details, nil
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
