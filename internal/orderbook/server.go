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

	handler  *es.Handler[*OrderBook]
	store    es.EventStore
	registry *es.Registry
	log      *slog.Logger
}

// NewServer creates a new Server with the given dependencies.
func NewServer(handler *es.Handler[*OrderBook], store es.EventStore, registry *es.Registry, log *slog.Logger) *Server {
	return &Server{
		handler:  handler,
		store:    store,
		registry: registry,
		log:      log,
	}
}

func (s *Server) PlaceOrder(ctx context.Context, req *connect.Request[orderbookv1.PlaceOrderRequest]) (*connect.Response[orderbookv1.PlaceOrderResponse], error) {
	msg := req.Msg

	cmd := PlaceOrder{
		Symbol:   msg.Symbol,
		Side:     sideFromProto(msg.Side),
		Price:    msg.Price,
		Quantity: msg.Quantity,
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
	}

	s.log.Info("GetOrder", "symbol", req.Msg.Symbol, "order_id", req.Msg.OrderId)

	return connect.NewResponse(resp), nil
}

// replayAggregate loads all events for the given symbol and replays them
// into a fresh OrderBook aggregate.
func (s *Server) replayAggregate(ctx context.Context, symbol string) (*OrderBook, error) {
	aggregateID := "orderbook:" + symbol

	rawEvents, err := s.store.Load(ctx, aggregateID)
	if err != nil {
		return nil, err
	}

	book := NewOrderBook(aggregateID)
	for _, raw := range rawEvents {
		evt, err := s.registry.Deserialize(raw)
		if err != nil {
			return nil, err
		}
		if err := book.Apply(evt); err != nil {
			return nil, err
		}
	}

	return book, nil
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
	case ErrInvalidPrice, ErrInvalidQuantity:
		return connect.NewError(connect.CodeInvalidArgument, unwrapped)
	case ErrOrderNotFound, ErrNoRemainingQty:
		return connect.NewError(connect.CodeNotFound, unwrapped)
	case es.ErrOptimisticConcurrency:
		return connect.NewError(connect.CodeAborted, unwrapped)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}
