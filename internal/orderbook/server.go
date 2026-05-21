package orderbook

import (
	"context"
	"errors"
	"log/slog"
	"time"

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
	symbols SymbolReader
	depth   DepthReader
	status  StatusReader
	candles CandleReader
	closes  OfficialCloseReader
	broker  *Broker
}

// NewServer creates a new Server with the given dependencies.
func NewServer(handler *es.Handler[*OrderBook], log *slog.Logger, trades TradeReader, orders OrderReader, symbols SymbolReader, depth DepthReader, status StatusReader, candles CandleReader, closes OfficialCloseReader, broker *Broker) *Server {
	return &Server{
		handler: handler,
		log:     log,
		trades:  trades,
		orders:  orders,
		symbols: symbols,
		depth:   depth,
		status:  status,
		candles: candles,
		closes:  closes,
		broker:  broker,
	}
}

func (s *Server) PlaceOrder(ctx context.Context, req *connect.Request[orderbookv1.PlaceOrderRequest]) (*connect.Response[orderbookv1.PlaceOrderResponse], error) {
	msg := req.Msg

	// Default market orders with unspecified TIF to IOC.
	tif := TimeInForceFromProto(msg.TimeInForce)
	if OrderTypeFromProto(msg.OrderType) == Market && msg.TimeInForce == orderbookv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED {
		tif = IOC
	}

	cmd := PlaceOrder{
		Symbol:         msg.Symbol,
		Side:           SideFromProto(msg.Side),
		Price:          msg.Price,
		StopPrice:      msg.StopPrice,
		Quantity:       msg.Quantity,
		OrderType:      OrderTypeFromProto(msg.OrderType),
		TimeInForce:    tif,
		AccountID:      msg.AccountId,
		DisplayQty:     msg.DisplayQuantity,
		TrailAmount:    msg.TrailAmount,
		TrailOffsetBps: msg.TrailOffsetBps,
		LimitOffset:    msg.LimitOffset,
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
		s.log.Warn("PlaceOrder failed", "symbol", msg.Symbol, "error", err)
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
		s.log.Warn("CancelOrder failed", "symbol", msg.Symbol, "order_id", msg.OrderId, "error", err)
		return nil, mapError(err)
	}

	s.log.Info("CancelOrder", "symbol", msg.Symbol, "order_id", msg.OrderId)

	return connect.NewResponse(&orderbookv1.CancelOrderResponse{}), nil
}

func (s *Server) ReplaceOrder(ctx context.Context, req *connect.Request[orderbookv1.ReplaceOrderRequest]) (*connect.Response[orderbookv1.ReplaceOrderResponse], error) {
	msg := req.Msg

	tif := TimeInForceFromProto(msg.TimeInForce)
	if OrderTypeFromProto(msg.OrderType) == Market && msg.TimeInForce == orderbookv1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED {
		tif = IOC
	}

	cmd := ReplaceOrder{
		Symbol:      msg.Symbol,
		OldOrderID:  msg.OldOrderId,
		NewOrderID:  msg.NewOrderId,
		Side:        SideFromProto(msg.Side),
		Price:       msg.Price,
		Quantity:    msg.Quantity,
		OrderType:   OrderTypeFromProto(msg.OrderType),
		TimeInForce: tif,
		AccountID:   msg.AccountId,
	}

	var produced []es.Event
	err := s.handler.Handle(ctx, cmd, func(book *OrderBook) ([]es.Event, error) {
		events, err := ExecuteReplaceOrder(book, cmd)
		if err != nil {
			return nil, err
		}
		produced = events
		return events, nil
	})
	if err != nil {
		s.log.Warn("ReplaceOrder failed", "symbol", msg.Symbol, "old_order_id", msg.OldOrderId, "error", err)
		return nil, mapError(err)
	}

	resp := &orderbookv1.ReplaceOrderResponse{}
	for _, evt := range produced {
		switch data := evt.Data.(type) {
		case *orderbookv1.OrderPlaced:
			resp.OrderId = data.OrderId
		case *orderbookv1.TradeExecuted:
			resp.Trades = append(resp.Trades, data)
		}
	}

	s.log.Info("ReplaceOrder", "symbol", msg.Symbol, "old_order_id", msg.OldOrderId, "new_order_id", resp.OrderId, "trade_count", len(resp.Trades))

	return connect.NewResponse(resp), nil
}

func (s *Server) OpenAuction(ctx context.Context, req *connect.Request[orderbookv1.OpenAuctionRequest]) (*connect.Response[orderbookv1.OpenAuctionResponse], error) {
	msg := req.Msg

	cmd := OpenAuction{
		Symbol: msg.Symbol,
		Reason: msg.Reason,
	}

	err := s.handler.Handle(ctx, cmd, func(book *OrderBook) ([]es.Event, error) {
		return ExecuteOpenAuction(book, cmd)
	})
	if err != nil {
		s.log.Warn("OpenAuction failed", "symbol", msg.Symbol, "error", err)
		return nil, mapError(err)
	}

	s.log.Info("OpenAuction", "symbol", msg.Symbol, "reason", cmd.Reason)
	return connect.NewResponse(&orderbookv1.OpenAuctionResponse{}), nil
}

func (s *Server) BeginClosingAuction(ctx context.Context, req *connect.Request[orderbookv1.BeginClosingAuctionRequest]) (*connect.Response[orderbookv1.BeginClosingAuctionResponse], error) {
	msg := req.Msg

	cmd := BeginClosingAuction{
		Symbol: msg.Symbol,
		Reason: msg.Reason,
	}

	err := s.handler.Handle(ctx, cmd, func(book *OrderBook) ([]es.Event, error) {
		return ExecuteBeginClosingAuction(book, cmd)
	})
	if err != nil {
		s.log.Warn("BeginClosingAuction failed", "symbol", msg.Symbol, "error", err)
		return nil, mapError(err)
	}

	s.log.Info("BeginClosingAuction", "symbol", msg.Symbol, "reason", cmd.Reason)
	return connect.NewResponse(&orderbookv1.BeginClosingAuctionResponse{}), nil
}

func (s *Server) Uncross(ctx context.Context, req *connect.Request[orderbookv1.UncrossRequest]) (*connect.Response[orderbookv1.UncrossResponse], error) {
	msg := req.Msg

	cmd := Uncross{Symbol: msg.Symbol}

	var summary *orderbookv1.AuctionUncrossed
	var tradeCount int32
	err := s.handler.Handle(ctx, cmd, func(book *OrderBook) ([]es.Event, error) {
		events, err := ExecuteUncross(book, cmd)
		if err != nil {
			return nil, err
		}
		for _, evt := range events {
			switch d := evt.Data.(type) {
			case *orderbookv1.AuctionUncrossed:
				summary = d
			case *orderbookv1.TradeExecuted:
				if d.CrossType != orderbookv1.CrossType_CROSS_TYPE_NONE {
					tradeCount++
				}
			}
		}
		return events, nil
	})
	if err != nil {
		s.log.Warn("Uncross failed", "symbol", msg.Symbol, "error", err)
		return nil, mapError(err)
	}

	resp := &orderbookv1.UncrossResponse{TradeCount: tradeCount}
	if summary != nil {
		resp.ClearingPrice = summary.ClearingPrice
		resp.MatchedQty = summary.MatchedQty
		resp.ImbalanceQty = summary.ImbalanceQty
		resp.ImbalanceSide = summary.ImbalanceSide
		resp.CrossType = summary.CrossType
	}

	s.log.Info("Uncross", "symbol", msg.Symbol,
		"clearing_price", resp.ClearingPrice,
		"matched_qty", resp.MatchedQty,
		"imbalance_qty", resp.ImbalanceQty,
		"trades", tradeCount)

	return connect.NewResponse(resp), nil
}

func (s *Server) CloseMarket(ctx context.Context, req *connect.Request[orderbookv1.CloseMarketRequest]) (*connect.Response[orderbookv1.CloseMarketResponse], error) {
	msg := req.Msg

	cmd := CloseMarket{
		Symbol: msg.Symbol,
	}

	var cancelledOrders int32
	err := s.handler.Handle(ctx, cmd, func(book *OrderBook) ([]es.Event, error) {
		events, err := ExecuteCloseMarket(book, cmd)
		if err != nil {
			return nil, err
		}
		for _, evt := range events {
			if _, ok := evt.Data.(*orderbookv1.OrderCancelled); ok {
				cancelledOrders++
			}
		}
		return events, nil
	})
	if err != nil {
		s.log.Warn("CloseMarket failed", "symbol", msg.Symbol, "error", err)
		return nil, mapError(err)
	}

	s.log.Info("CloseMarket", "symbol", msg.Symbol, "cancelled_orders", cancelledOrders)

	return connect.NewResponse(&orderbookv1.CloseMarketResponse{
		CancelledOrders: cancelledOrders,
	}), nil
}

func (s *Server) GetOfficialClose(ctx context.Context, req *connect.Request[orderbookv1.GetOfficialCloseRequest]) (*connect.Response[orderbookv1.GetOfficialCloseResponse], error) {
	if s.closes == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("daily_close projection not configured"))
	}
	resp, err := s.closes.GetOfficialClose(ctx, req.Msg.Symbol, req.Msg.SessionDate)
	if err != nil {
		s.log.Warn("GetOfficialClose failed", "symbol", req.Msg.Symbol, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if resp == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("no close recorded"))
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) ListOfficialCloses(ctx context.Context, req *connect.Request[orderbookv1.ListOfficialClosesRequest]) (*connect.Response[orderbookv1.ListOfficialClosesResponse], error) {
	if s.closes == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("daily_close projection not configured"))
	}
	rows, err := s.closes.ListOfficialCloses(ctx, req.Msg.Symbol, req.Msg.From, req.Msg.To)
	if err != nil {
		s.log.Warn("ListOfficialCloses failed", "symbol", req.Msg.Symbol, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&orderbookv1.ListOfficialClosesResponse{Closes: rows}), nil
}

func (s *Server) GetOrderBook(ctx context.Context, req *connect.Request[orderbookv1.GetOrderBookRequest]) (*connect.Response[orderbookv1.GetOrderBookResponse], error) {
	book, err := s.replayAggregate(ctx, req.Msg.Symbol)
	if err != nil {
		s.log.Error("GetOrderBook failed", "symbol", req.Msg.Symbol, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &orderbookv1.GetOrderBookResponse{
		Symbol:         req.Msg.Symbol,
		Phase:          MarketPhaseToProto(book.Phase),
		LastTradePrice: book.LastTradePrice,
		SessionVolume:  book.SessionVolume,
	}

	for bid := range book.Bids.All() {
		resp.Bids = append(resp.Bids, orderToLevel(bid))
	}
	for ask := range book.Asks.All() {
		resp.Asks = append(resp.Asks, orderToLevel(ask))
	}

	s.log.Info("GetOrderBook", "symbol", req.Msg.Symbol, "bid_count", len(resp.Bids), "ask_count", len(resp.Asks))

	return connect.NewResponse(resp), nil
}

func (s *Server) GetMarketStatus(ctx context.Context, req *connect.Request[orderbookv1.GetMarketStatusRequest]) (*connect.Response[orderbookv1.GetMarketStatusResponse], error) {
	phase, lastTradePrice, sessionVolume := s.status.GetStatus(req.Msg.Symbol)

	resp := &orderbookv1.GetMarketStatusResponse{
		Symbol:         req.Msg.Symbol,
		Phase:          phase,
		LastTradePrice: lastTradePrice,
		SessionVolume:  sessionVolume,
	}

	s.log.Info("GetMarketStatus", "symbol", req.Msg.Symbol, "phase", phase, "last_trade_price", lastTradePrice, "session_volume", sessionVolume)

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
	order, ok := s.orders.GetOrder(req.Msg.Symbol, req.Msg.OrderId)
	if !ok {
		s.log.Warn("GetOrder not found", "symbol", req.Msg.Symbol, "order_id", req.Msg.OrderId)
		return nil, connect.NewError(connect.CodeNotFound, ErrOrderNotFound)
	}

	resp := &orderbookv1.GetOrderResponse{
		OrderId:            order.OrderId,
		Symbol:             order.Symbol,
		Side:               order.Side,
		Price:              order.Price,
		StopPrice:          order.StopPrice,
		Quantity:           order.Quantity,
		RemainingQuantity:  order.RemainingQuantity,
		DisplayQuantity:    order.DisplayQuantity,
		DisplayedRemaining: order.DisplayedRemaining,
		TrailAmount:        order.TrailAmount,
		TrailOffsetBps:     order.TrailOffsetBps,
		LimitOffset:        order.LimitOffset,
		PlacedAt:           order.PlacedAt,
		OrderType:          order.OrderType,
		TimeInForce:        order.TimeInForce,
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

func (s *Server) ListSymbols(ctx context.Context, req *connect.Request[orderbookv1.ListSymbolsRequest]) (*connect.Response[orderbookv1.ListSymbolsResponse], error) {
	symbols := s.symbols.ListSymbols()

	s.log.Info("ListSymbols", "count", len(symbols))

	return connect.NewResponse(&orderbookv1.ListSymbolsResponse{
		Symbols: symbols,
	}), nil
}

func (s *Server) StreamMarketDepth(ctx context.Context, req *connect.Request[orderbookv1.StreamMarketDepthRequest], stream *connect.ServerStream[orderbookv1.GetMarketDepthResponse]) error {
	symbol := req.Msg.Symbol
	depth := req.Msg.Depth

	bids, asks := s.depth.GetDepth(symbol, depth)
	if err := stream.Send(&orderbookv1.GetMarketDepthResponse{
		Symbol: symbol,
		Bids:   bids,
		Asks:   asks,
	}); err != nil {
		return err
	}

	id, ch := s.broker.Subscribe(symbol)
	defer s.broker.Unsubscribe(id)

	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-ch:
			if !ok {
				return nil
			}
			bids, asks := s.depth.GetDepth(symbol, depth)
			if err := stream.Send(&orderbookv1.GetMarketDepthResponse{
				Symbol: symbol,
				Bids:   bids,
				Asks:   asks,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) StreamTrades(ctx context.Context, req *connect.Request[orderbookv1.StreamTradesRequest], stream *connect.ServerStream[orderbookv1.Trade]) error {
	symbol := req.Msg.Symbol

	id, ch := s.broker.Subscribe(symbol)
	defer s.broker.Unsubscribe(id)

	for {
		select {
		case <-ctx.Done():
			return nil
		case events, ok := <-ch:
			if !ok {
				return nil
			}
			for _, evt := range events {
				data, ok := evt.Data.(*orderbookv1.TradeExecuted)
				if !ok || data.Symbol != symbol {
					continue
				}
				if err := stream.Send(&orderbookv1.Trade{
					TradeId:     data.TradeId,
					Symbol:      data.Symbol,
					BuyOrderId:  data.BuyOrderId,
					SellOrderId: data.SellOrderId,
					Price:       data.Price,
					Quantity:    data.Quantity,
					ExecutedAt:  data.ExecutedAt,
					CrossType:   data.CrossType,
				}); err != nil {
					return err
				}
			}
		}
	}
}

// StreamIndicativeAuctionState pushes a "what would uncross do right
// now" snapshot whenever the orderbook for `symbol` changes (broker
// channel wake) and on a 1Hz heartbeat. Always sends the current
// phase so the client knows when to stop rendering — the subscription
// stays open across phase transitions; the client decides whether the
// payload is interesting.
func (s *Server) StreamIndicativeAuctionState(
	ctx context.Context,
	req *connect.Request[orderbookv1.StreamIndicativeAuctionStateRequest],
	stream *connect.ServerStream[orderbookv1.IndicativeAuctionState],
) error {
	symbol := req.Msg.Symbol

	id, ch := s.broker.Subscribe(symbol)
	defer s.broker.Unsubscribe(id)

	t := time.NewTicker(time.Second)
	defer t.Stop()

	send := func() error {
		book, err := s.handler.Load(ctx, AggregateID(symbol))
		if err != nil {
			return err
		}
		out := &orderbookv1.IndicativeAuctionState{
			Symbol:     symbol,
			Phase:      MarketPhaseToProto(book.Phase),
			ComputedAt: timestamppb.Now(),
		}
		if ind := ComputeIndicative(book); ind != nil {
			out.IndicativePrice = ind.ClearingPrice
			out.MatchedQty = ind.MatchedQty
			out.ImbalanceQty = ind.ImbalanceQty
			out.ImbalanceSide = SideToProto(ind.ImbalanceSide)
		}
		return stream.Send(out)
	}

	if err := send(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-ch:
			if !ok {
				return nil
			}
			if err := send(); err != nil {
				return err
			}
		case <-t.C:
			if err := send(); err != nil {
				return err
			}
		}
	}
}

func (s *Server) GetCandles(ctx context.Context, req *connect.Request[orderbookv1.GetCandlesRequest]) (*connect.Response[orderbookv1.GetCandlesResponse], error) {
	msg := req.Msg
	candles := s.candles.GetCandles(msg.Symbol, msg.Interval, msg.From.AsTime(), msg.To.AsTime())

	s.log.Info("GetCandles", "symbol", msg.Symbol, "interval", msg.Interval, "count", len(candles))

	return connect.NewResponse(&orderbookv1.GetCandlesResponse{
		Candles: candles,
	}), nil
}

func (s *Server) StreamCandles(ctx context.Context, req *connect.Request[orderbookv1.StreamCandlesRequest], stream *connect.ServerStream[orderbookv1.Candle]) error {
	symbol := req.Msg.Symbol
	interval := req.Msg.Interval

	if latest := s.candles.GetLatestCandle(symbol, interval); latest != nil {
		if err := stream.Send(latest); err != nil {
			return err
		}
	}

	id, ch := s.broker.Subscribe(symbol)
	defer s.broker.Unsubscribe(id)

	for {
		select {
		case <-ctx.Done():
			return nil
		case events, ok := <-ch:
			if !ok {
				return nil
			}
			hasTrade := false
			for _, evt := range events {
				if _, ok := evt.Data.(*orderbookv1.TradeExecuted); ok {
					hasTrade = true
					break
				}
			}
			if !hasTrade {
				continue
			}
			if latest := s.candles.GetLatestCandle(symbol, interval); latest != nil {
				if err := stream.Send(latest); err != nil {
					return err
				}
			}
		}
	}
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
	case ErrInvalidPrice, ErrInvalidQuantity, ErrMarketGTC, ErrMarketRequiresZeroPrice,
		ErrStopRequiresStopPrice, ErrStopMarketRequiresZeroPrice, ErrStopLimitRequiresPrice,
		ErrIcebergRequiresLimit, ErrIcebergRequiresRestingTIF,
		ErrIcebergDisplayExceedsQuantity, ErrIcebergNotAllowedWithReplace,
		ErrTrailingStopRequiresTrail, ErrTrailingStopAmbiguousTrail,
		ErrTrailingStopLimitRequiresOffset, ErrTrailingStopRejectsLimitOffset:
		return connect.NewError(connect.CodeInvalidArgument, unwrapped)
	case ErrInsufficientLiquidity:
		return connect.NewError(connect.CodeFailedPrecondition, unwrapped)
	case ErrAuctionRejectsIOC, ErrAuctionRejectsMarket, ErrMarketClosed,
		ErrAlreadyInAuction, ErrNotInAuction, ErrCannotOpenAuction,
		ErrAtOpenOutsideAuction, ErrAtCloseOutsideAcceptanceWindow,
		ErrAuctionStopNotAllowed, ErrCannotBeginClosing,
		ErrClosingAuctionRejectsRegular:
		return connect.NewError(connect.CodeFailedPrecondition, unwrapped)
	case ErrOrderNotFound, ErrNoRemainingQty:
		return connect.NewError(connect.CodeNotFound, unwrapped)
	case ErrAccountMismatch:
		return connect.NewError(connect.CodePermissionDenied, unwrapped)
	case es.ErrOptimisticConcurrency:
		return connect.NewError(connect.CodeAborted, unwrapped)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}
