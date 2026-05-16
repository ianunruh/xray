package portfolio

import (
	"context"
	"errors"
	"log/slog"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/pkg/es"
)

var (
	ErrInvalidPrice    = errors.New("price must be positive")
	ErrInvalidQuantity = errors.New("quantity must be positive")
)

type PortfolioReader interface {
	GetPortfolio(ctx context.Context, accountID string) (*portfoliov1.GetPortfolioResponse, error)
}

type PlaceOrderFunc func(ctx context.Context, req *portfoliov1.PortfolioPlaceOrderRequest) (sagaID string, err error)

type GetOrderStatusFunc func(ctx context.Context, sagaID string) (*portfoliov1.GetOrderStatusResponse, error)

type Server struct {
	portfoliov1connect.UnimplementedPortfolioServiceHandler

	portfolioHandler *es.Handler[*Portfolio]
	reader           PortfolioReader
	pnlReader        PnLReader
	placeOrder       PlaceOrderFunc
	getOrderStatus   GetOrderStatusFunc
	broker           *PortfolioBroker
	log              *slog.Logger
}

func NewServer(portfolioHandler *es.Handler[*Portfolio], reader PortfolioReader, pnlReader PnLReader, placeOrder PlaceOrderFunc, getOrderStatus GetOrderStatusFunc, broker *PortfolioBroker, log *slog.Logger) *Server {
	return &Server{
		portfolioHandler: portfolioHandler,
		reader:           reader,
		pnlReader:        pnlReader,
		placeOrder:       placeOrder,
		getOrderStatus:   getOrderStatus,
		broker:           broker,
		log:              log,
	}
}

func (s *Server) Deposit(ctx context.Context, req *connect.Request[portfoliov1.DepositRequest]) (*connect.Response[portfoliov1.DepositResponse], error) {
	msg := req.Msg

	if msg.Amount <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrInvalidAmount)
	}

	cmd := DepositCash{AccountID: msg.AccountId, Amount: msg.Amount}
	err := s.portfolioHandler.Handle(ctx, cmd, func(p *Portfolio) ([]es.Event, error) {
		return ExecuteDepositCash(p, cmd)
	})
	if err != nil {
		s.log.Error("Deposit failed", "account_id", msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.log.Info("Deposit", "account_id", msg.AccountId, "amount", msg.Amount)
	return connect.NewResponse(&portfoliov1.DepositResponse{}), nil
}

func (s *Server) Withdraw(ctx context.Context, req *connect.Request[portfoliov1.WithdrawRequest]) (*connect.Response[portfoliov1.WithdrawResponse], error) {
	msg := req.Msg

	if msg.Amount <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrInvalidAmount)
	}

	cmd := WithdrawCash{AccountID: msg.AccountId, Amount: msg.Amount}
	err := s.portfolioHandler.Handle(ctx, cmd, func(p *Portfolio) ([]es.Event, error) {
		return ExecuteWithdrawCash(p, cmd)
	})
	if err != nil {
		if err.Error() == "execute command: insufficient funds" {
			return nil, connect.NewError(connect.CodeFailedPrecondition, ErrInsufficientFunds)
		}
		s.log.Error("Withdraw failed", "account_id", msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.log.Info("Withdraw", "account_id", msg.AccountId, "amount", msg.Amount)
	return connect.NewResponse(&portfoliov1.WithdrawResponse{}), nil
}

func (s *Server) CreditShares(ctx context.Context, req *connect.Request[portfoliov1.CreditSharesRequest]) (*connect.Response[portfoliov1.CreditSharesResponse], error) {
	msg := req.Msg

	if msg.Quantity <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrInvalidQuantity)
	}

	cmd := CreditShares{AccountID: msg.AccountId, Symbol: msg.Symbol, Quantity: msg.Quantity, CostPerShare: msg.CostPerShare}
	err := s.portfolioHandler.Handle(ctx, cmd, func(p *Portfolio) ([]es.Event, error) {
		return ExecuteCreditShares(p, cmd)
	})
	if err != nil {
		s.log.Error("CreditShares failed", "account_id", msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.log.Info("CreditShares", "account_id", msg.AccountId, "symbol", msg.Symbol, "quantity", msg.Quantity)
	return connect.NewResponse(&portfoliov1.CreditSharesResponse{}), nil
}

func (s *Server) GetPortfolio(ctx context.Context, req *connect.Request[portfoliov1.GetPortfolioRequest]) (*connect.Response[portfoliov1.GetPortfolioResponse], error) {
	resp, err := s.reader.GetPortfolio(ctx, req.Msg.AccountId)
	if err != nil {
		s.log.Error("GetPortfolio failed", "account_id", req.Msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) PlaceOrder(ctx context.Context, req *connect.Request[portfoliov1.PortfolioPlaceOrderRequest]) (*connect.Response[portfoliov1.PortfolioPlaceOrderResponse], error) {
	msg := req.Msg

	if msg.Price <= 0 && msg.OrderType != orderbookv1.OrderType_ORDER_TYPE_MARKET {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrInvalidPrice)
	}
	if msg.Quantity <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrInvalidQuantity)
	}

	sagaID, err := s.placeOrder(ctx, msg)
	if err != nil {
		s.log.Error("PlaceOrder failed", "account_id", msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.log.Info("PlaceOrder",
		"saga_id", sagaID,
		"account_id", msg.AccountId,
		"symbol", msg.Symbol,
		"side", msg.Side,
		"price", msg.Price,
		"quantity", msg.Quantity)

	return connect.NewResponse(&portfoliov1.PortfolioPlaceOrderResponse{
		SagaId: sagaID,
	}), nil
}

func (s *Server) GetOrderStatus(ctx context.Context, req *connect.Request[portfoliov1.GetOrderStatusRequest]) (*connect.Response[portfoliov1.GetOrderStatusResponse], error) {
	sagaID := req.Msg.SagaId
	if sagaID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("saga_id is required"))
	}

	resp, err := s.getOrderStatus(ctx, sagaID)
	if err != nil {
		s.log.Error("GetOrderStatus failed", "saga_id", sagaID, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(resp), nil
}

func (s *Server) StreamPortfolio(ctx context.Context, req *connect.Request[portfoliov1.StreamPortfolioRequest], stream *connect.ServerStream[portfoliov1.GetPortfolioResponse]) error {
	accountID := req.Msg.AccountId

	resp, err := s.reader.GetPortfolio(ctx, accountID)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if err := stream.Send(resp); err != nil {
		return err
	}

	id, ch := s.broker.Subscribe(accountID)
	defer s.broker.Unsubscribe(id)

	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-ch:
			if !ok {
				return nil
			}
			resp, err := s.reader.GetPortfolio(ctx, accountID)
			if err != nil {
				s.log.Error("StreamPortfolio read failed", "account_id", accountID, "error", err)
				continue
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func (s *Server) GetPnL(ctx context.Context, req *connect.Request[portfoliov1.GetPnLRequest]) (*connect.Response[portfoliov1.GetPnLResponse], error) {
	resp, err := s.pnlReader.GetPnL(ctx, req.Msg.AccountId)
	if err != nil {
		s.log.Error("GetPnL failed", "account_id", req.Msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}
