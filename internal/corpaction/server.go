package corpaction

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	corpactionv1 "github.com/ianunruh/xray/gen/corpaction/v1"
	"github.com/ianunruh/xray/gen/corpaction/v1/corpactionv1connect"
	"github.com/ianunruh/xray/pkg/es"
)

// Server is the Connect handler for CorporateActionService. Declare
// generates a server-side action_id and writes the
// CorporateActionDeclared event; the reactor later picks it up.
// List/Get serve the projection-backed ledger view.
type Server struct {
	corpactionv1connect.UnimplementedCorporateActionServiceHandler

	handler *es.Handler[*CorporateAction]
	reader  Reader
	log     *slog.Logger
}

func NewServer(handler *es.Handler[*CorporateAction], reader Reader, log *slog.Logger) *Server {
	return &Server{handler: handler, reader: reader, log: log}
}

func (s *Server) Declare(ctx context.Context, req *connect.Request[corpactionv1.DeclareCorporateActionRequest]) (*connect.Response[corpactionv1.DeclareCorporateActionResponse], error) {
	msg := req.Msg
	if msg.Symbol == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("symbol required"))
	}
	if msg.Type == corpactionv1.ActionType_ACTION_TYPE_UNSPECIFIED {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("type required"))
	}

	actionID := uuid.NewString()
	cmd := Declare{
		ActionID:         actionID,
		Symbol:           msg.Symbol,
		Type:             msg.Type,
		SplitNumerator:   msg.SplitNumerator,
		SplitDenominator: msg.SplitDenominator,
		DividendPerShare: msg.DividendPerShare,
		NewSymbol:        msg.NewSymbol,
	}
	if msg.EffectiveDate != nil {
		cmd.EffectiveDate = msg.EffectiveDate.AsTime()
	}
	if msg.RecordDate != nil {
		cmd.RecordDate = msg.RecordDate.AsTime()
	}
	if msg.PayDate != nil {
		cmd.PayDate = msg.PayDate.AsTime()
	}

	err := s.handler.Handle(ctx, cmd, func(a *CorporateAction) ([]es.Event, error) {
		return ExecuteDeclare(a, cmd)
	})
	if err != nil {
		// Validation errors (ErrInvalidSplitRatio etc.) are user
		// problems — surface as InvalidArgument so the UI can render
		// a usable message rather than a generic 500.
		if errors.Is(err, ErrInvalidSplitRatio) ||
			errors.Is(err, ErrInvalidDividendAmount) ||
			errors.Is(err, ErrMissingNewSymbol) ||
			errors.Is(err, ErrMissingEffectiveDate) ||
			errors.Is(err, ErrMissingDividendDates) ||
			errors.Is(err, ErrSameSymbol) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		s.log.Error("Declare failed", "symbol", msg.Symbol, "type", msg.Type, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	s.log.Info("Declare", "action_id", actionID, "symbol", msg.Symbol, "type", msg.Type)
	return connect.NewResponse(&corpactionv1.DeclareCorporateActionResponse{ActionId: actionID}), nil
}

func (s *Server) List(ctx context.Context, req *connect.Request[corpactionv1.ListCorporateActionsRequest]) (*connect.Response[corpactionv1.ListCorporateActionsResponse], error) {
	records, err := s.reader.List(ctx, req.Msg.Symbol, req.Msg.Status, req.Msg.Limit)
	if err != nil {
		s.log.Error("List failed", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&corpactionv1.ListCorporateActionsResponse{Actions: records}), nil
}

func (s *Server) Get(ctx context.Context, req *connect.Request[corpactionv1.GetCorporateActionRequest]) (*connect.Response[corpactionv1.GetCorporateActionResponse], error) {
	if req.Msg.ActionId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("action_id required"))
	}
	rec, err := s.reader.Get(ctx, req.Msg.ActionId)
	if err != nil {
		s.log.Error("Get failed", "action_id", req.Msg.ActionId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if rec == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("action %s not found", req.Msg.ActionId))
	}
	return connect.NewResponse(&corpactionv1.GetCorporateActionResponse{Action: rec}), nil
}
