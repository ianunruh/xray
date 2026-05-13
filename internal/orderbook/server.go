package orderbook

import (
	"context"
	"errors"
	"log/slog"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/gen/orderbook/v1/orderbookv1connect"
	"github.com/ianunruh/xray/pkg/es"
)

// Server implements the OrderBookService Connect handler.
type Server struct {
	orderbookv1connect.UnimplementedOrderBookServiceHandler

	handler *es.Handler[*OrderBook]
	log     *slog.Logger
	trades  TradeReader
	orders  OrderReader
	depth   DepthReader
}

// NewServer creates a new Server with the given dependencies.
func NewServer(handler *es.Handler[*OrderBook], log *slog.Logger, trades TradeReader, orders OrderReader, depth DepthReader) *Server {
	return &Server{
		handler: handler,
		log:     log,
		trades:  trades,
		orders:  orders,
		depth:   depth,
	}
}

func (s *Server) PlaceOrder(ctx context.Context, req *connect.Request[orderbookv1.PlaceOrderRequest]) (*connect.Response[orderbookv1.PlaceOrderResponse], error) {
	msg := req.Msg

	// Default market orders with unspecified TIF to IOC.
	tif := tifFromProto(msg.TimeInForce)
	if orderTypeFromProto(msg.OrderType) == Market && msg.TimeInForce == orderbookv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED {
		tif = IOC
	}

	cmd := PlaceOrder{
		Symbol:      msg.Symbol,
		Side:        sideFromProto(msg.Side),
		Price:       msg.Price,
		Quantity:    msg.Quantity,
		OrderType:   orderTypeFromProto(msg.OrderType),
		TimeInForce: tif,
	}

	var produced []es.Event
	err := s.handler.Handle(ctx, cmd, func(book *OrderBook) ([]es.Event, error) {
		events, err := ExecutePlaceOrder(book, cmd)
		if err != nil {
			return nil, err
		}
		produced = events
		return events, nil
	})
	if err != nil {
		s.log.Error("PlaceOrder failed", "symbol", msg.Symbol, "error", err)
		return nil, mapError(err)
	}

	resp := &orderbookv1.PlaceOrderResponse{}
	for _, evt := range produced {
		switch data := evt.Data.(type) {
		case *orderbookv1.OrderPlaced:
			resp.OrderId = data.OrderId
		case *orderbookv1.TradeExecuted:
			resp.Trades = append(resp.Trades, data)
		}
	}

	s.log.Info("PlaceOrder", "symbol", msg.Symbol, "side", msg.Side, "price", msg.Price, "quantity", msg.Quantity, "order_id", resp.OrderId, "trade_count", len(resp.Trades))

	return connect.NewResponse(resp), nil
}

func (s *Server) CancelOrder(ctx context.Context, req *connect.Request[orderbookv1.CancelOrderRequest]) (*connect.Response[orderbookv1.CancelOrderResponse], error) {
	msg := req.Msg

	cmd := CancelOrder{
		Symbol:  msg.Symbol,
		OrderID: msg.OrderId,
	}

	err := s.handler.Handle(ctx, cmd, func(book *OrderBook) ([]es.Event, error) {
		return ExecuteCancelOrder(book, cmd)
	})
	if err != nil {
		s.log.Error("CancelOrder failed", "symbol", msg.Symbol, "order_id", msg.OrderId, "error", err)
		return nil, mapError(err)
	}

	s.log.Info("CancelOrder", "symbol", msg.Symbol, "order_id", msg.OrderId)

	return connect.NewResponse(&orderbookv1.CancelOrderResponse{}), nil
}

func (s *Server) GetOrderBook(ctx context.Context, req *connect.Request[orderbookv1.GetOrderBookRequest]) (*connect.Response[orderbookv1.GetOrderBookResponse], error) {
	book, err := s.replayAggregate(ctx, req.Msg.Symbol)
	if err != nil {
		s.log.Error("GetOrderBook failed", "symbol", req.Msg.Symbol, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &orderbookv1.GetOrderBookResponse{
		Symbol: req.Msg.Symbol,
	}

	for _, bid := range book.Bids {
		resp.Bids = append(resp.Bids, orderToLevel(bid))
	}
	for _, ask := range book.Asks {
		resp.Asks = append(resp.Asks, orderToLevel(ask))
	}

	s.log.Info("GetOrderBook", "symbol", req.Msg.Symbol, "bid_count", len(resp.Bids), "ask_count", len(resp.Asks))

	return connect.NewResponse(resp), nil
}

func (s *Server) GetMarketDepth(ctx context.Context, req *connect.Request[orderbookv1.GetMarketDepthRequest]) (*connect.Response[orderbookv1.GetMarketDepthResponse], error) {
	bids, asks := s.depth.GetDepth(req.Msg.Symbol, req.Msg.Depth)

	resp := &orderbookv1.GetMarketDepthResponse{
		Symbol: req.Msg.Symbol,
		Bids:   bids,
		Asks:   asks,
	}

	s.log.Info("GetMarketDepth", "symbol", req.Msg.Symbol, "bid_levels", len(resp.Bids), "ask_levels", len(resp.Asks))

	return connect.NewResponse(resp), nil
}

func (s *Server) GetOrder(ctx context.Context, req *connect.Request[orderbookv1.GetOrderRequest]) (*connect.Response[orderbookv1.GetOrderResponse], error) {
	book, err := s.replayAggregate(ctx, req.Msg.Symbol)
	if err != nil {
		s.log.Error("GetOrder failed", "symbol", req.Msg.Symbol, "order_id", req.Msg.OrderId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	order, ok := book.Orders[req.Msg.OrderId]
	if !ok {
		s.log.Error("GetOrder not found", "symbol", req.Msg.Symbol, "order_id", req.Msg.OrderId)
		return nil, connect.NewError(connect.CodeNotFound, ErrOrderNotFound)
	}

	resp := &orderbookv1.GetOrderResponse{
		OrderId:           order.ID,
		Symbol:            req.Msg.Symbol,
		Side:              sideToProto(order.Side),
		Price:             order.Price,
		Quantity:          order.Quantity,
		RemainingQuantity: order.RemainingQty,
		PlacedAt:          timestamppb.New(order.PlacedAt),
		OrderType:         orderTypeToProto(order.OrderType),
		TimeInForce:       tifToProto(order.TimeInForce),
	}

	s.log.Info("GetOrder", "symbol", req.Msg.Symbol, "order_id", req.Msg.OrderId)

	return connect.NewResponse(resp), nil
}

func (s *Server) ListTrades(ctx context.Context, req *connect.Request[orderbookv1.ListTradesRequest]) (*connect.Response[orderbookv1.ListTradesResponse], error) {
	trades := s.trades.ListTrades(req.Msg.Symbol)

	s.log.Info("ListTrades", "symbol", req.Msg.Symbol, "count", len(trades))

	return connect.NewResponse(&orderbookv1.ListTradesResponse{
		Trades: trades,
	}), nil
}

func (s *Server) ListOrders(ctx context.Context, req *connect.Request[orderbookv1.ListOrdersRequest]) (*connect.Response[orderbookv1.ListOrdersResponse], error) {
	orders := s.orders.ListOrders(req.Msg.Symbol)

	s.log.Info("ListOrders", "symbol", req.Msg.Symbol, "count", len(orders))

	return connect.NewResponse(&orderbookv1.ListOrdersResponse{
		Orders: orders,
	}), nil
}

// replayAggregate loads an OrderBook aggregate, using a snapshot if available
// to avoid replaying the full event stream.
func (s *Server) replayAggregate(ctx context.Context, symbol string) (*OrderBook, error) {
	return s.handler.Load(ctx, AggregateID(symbol))
}

func orderToLevel(o *Order) *orderbookv1.OrderBookLevel {
	return &orderbookv1.OrderBookLevel{
		OrderId:           o.ID,
		Price:             o.Price,
		Quantity:          o.Quantity,
		RemainingQuantity: o.RemainingQty,
		PlacedAt:          timestamppb.New(o.PlacedAt),
	}
}

func mapError(err error) *connect.Error {
	// Unwrap "execute command: <sentinel>" from the handler.
	unwrapped := err
	for {
		inner := errors.Unwrap(unwrapped)
		if inner == nil {
			break
		}
		unwrapped = inner
	}

	switch unwrapped {
	case ErrInvalidPrice, ErrInvalidQuantity, ErrMarketGTC, ErrMarketRequiresZeroPrice:
		return connect.NewError(connect.CodeInvalidArgument, unwrapped)
	case ErrInsufficientLiquidity:
		return connect.NewError(connect.CodeFailedPrecondition, unwrapped)
	case ErrOrderNotFound, ErrNoRemainingQty:
		return connect.NewError(connect.CodeNotFound, unwrapped)
	case es.ErrOptimisticConcurrency:
		return connect.NewError(connect.CodeAborted, unwrapped)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}
