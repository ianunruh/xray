package tradermgr

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"

	traderv1 "github.com/ianunruh/xray/gen/trader/v1"
	"github.com/ianunruh/xray/gen/trader/v1/traderv1connect"
)

// Server is the TraderService Connect handler. It is a thin wrapper around
// Manager — input validation and error mapping live here, lifecycle and
// persistence live in Manager.
type Server struct {
	traderv1connect.UnimplementedTraderServiceHandler
	mgr *Manager
}

func NewServer(mgr *Manager) *Server {
	return &Server{mgr: mgr}
}

func (s *Server) ListTraders(ctx context.Context, _ *connect.Request[traderv1.ListTradersRequest]) (*connect.Response[traderv1.ListTradersResponse], error) {
	traders, err := s.mgr.List(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&traderv1.ListTradersResponse{Traders: traders}), nil
}

func (s *Server) GetTrader(ctx context.Context, req *connect.Request[traderv1.GetTraderRequest]) (*connect.Response[traderv1.Trader], error) {
	id := strings.TrimSpace(req.Msg.Id)
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	t, err := s.mgr.Get(ctx, id)
	if err != nil {
		return nil, mapErr(err)
	}
	return connect.NewResponse(t), nil
}

func (s *Server) CreateTrader(ctx context.Context, req *connect.Request[traderv1.CreateTraderRequest]) (*connect.Response[traderv1.Trader], error) {
	name := strings.TrimSpace(req.Msg.Name)
	t, err := s.mgr.Create(ctx, name, req.Msg.Type, req.Msg.Config, req.Msg.Start)
	if err != nil {
		return nil, mapErr(err)
	}
	return connect.NewResponse(t), nil
}

func (s *Server) UpdateTrader(ctx context.Context, req *connect.Request[traderv1.UpdateTraderRequest]) (*connect.Response[traderv1.Trader], error) {
	id := strings.TrimSpace(req.Msg.Id)
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	t, err := s.mgr.Update(ctx, id, strings.TrimSpace(req.Msg.Name), req.Msg.Config)
	if err != nil {
		return nil, mapErr(err)
	}
	return connect.NewResponse(t), nil
}

func (s *Server) DeleteTrader(ctx context.Context, req *connect.Request[traderv1.DeleteTraderRequest]) (*connect.Response[traderv1.DeleteTraderResponse], error) {
	id := strings.TrimSpace(req.Msg.Id)
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	if err := s.mgr.Delete(ctx, id); err != nil {
		return nil, mapErr(err)
	}
	return connect.NewResponse(&traderv1.DeleteTraderResponse{}), nil
}

func (s *Server) StartTrader(ctx context.Context, req *connect.Request[traderv1.StartTraderRequest]) (*connect.Response[traderv1.Trader], error) {
	id := strings.TrimSpace(req.Msg.Id)
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	t, err := s.mgr.Start(ctx, id)
	if err != nil {
		return nil, mapErr(err)
	}
	return connect.NewResponse(t), nil
}

func (s *Server) StopTrader(ctx context.Context, req *connect.Request[traderv1.StopTraderRequest]) (*connect.Response[traderv1.Trader], error) {
	id := strings.TrimSpace(req.Msg.Id)
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}
	t, err := s.mgr.Stop(ctx, id)
	if err != nil {
		return nil, mapErr(err)
	}
	return connect.NewResponse(t), nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return connect.NewError(connect.CodeNotFound, err)
	}
	// Validation errors come back as plain errors with messages from
	// the config validators. Surface them as InvalidArgument so the UI
	// can show them in the form.
	return connect.NewError(connect.CodeInvalidArgument, err)
}
