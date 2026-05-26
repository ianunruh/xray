package orderbook

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sort"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	"github.com/ianunruh/xray/pkg/es"
)

// replayTradeLookback bounds how far back from at_version the replay scans
// when extracting recent_trades. Trades are sparse relative to total events
// (order placements, cancels, etc.), but a window of this size reliably
// surfaces at least a few dozen trades on an active book.
const replayTradeLookback = 1000

// defaultReplayTradeLimit is the cap on recent_trades when the request does
// not specify trade_limit.
const defaultReplayTradeLimit = 50

func (s *Server) GetReplayBounds(ctx context.Context, req *connect.Request[orderbookv1.GetReplayBoundsRequest]) (*connect.Response[orderbookv1.GetReplayBoundsResponse], error) {
	symbol := req.Msg.Symbol
	if symbol == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("symbol is required"))
	}

	aggID := AggregateID(symbol)
	meta, err := s.handler.StreamMetadata(ctx, aggID)
	if err != nil {
		s.log.Error("GetReplayBounds: stream metadata", "symbol", symbol, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &orderbookv1.GetReplayBoundsResponse{
		Symbol:       symbol,
		FirstVersion: int32(meta.FirstVersion),
		LastVersion:  int32(meta.LastVersion),
	}
	if !meta.FirstTimestamp.IsZero() {
		resp.FirstTimestamp = timestamppb.New(meta.FirstTimestamp)
	}
	if !meta.LastTimestamp.IsZero() {
		resp.LastTimestamp = timestamppb.New(meta.LastTimestamp)
	}

	if meta.LastVersion > 0 {
		book, err := s.handler.Load(ctx, aggID)
		if err != nil {
			s.log.Warn("GetReplayBounds: load current phase failed", "symbol", symbol, "error", err)
		} else {
			resp.CurrentPhase = MarketPhaseToProto(book.Phase)
		}
	}

	return connect.NewResponse(resp), nil
}

func (s *Server) ReplayOrderBook(ctx context.Context, req *connect.Request[orderbookv1.ReplayOrderBookRequest]) (*connect.Response[orderbookv1.ReplayOrderBookResponse], error) {
	symbol := req.Msg.Symbol
	if symbol == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("symbol is required"))
	}

	aggID := AggregateID(symbol)

	atVersion, err := s.resolveReplayVersion(ctx, aggID, req.Msg)
	if err != nil {
		return nil, err
	}
	if atVersion <= 0 {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no events for %s at requested point", symbol))
	}

	book, err := s.handler.LoadAt(ctx, aggID, atVersion)
	if err != nil {
		s.log.Error("ReplayOrderBook: LoadAt", "symbol", symbol, "at_version", atVersion, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tradeLimit := int(req.Msg.TradeLimit)
	if tradeLimit <= 0 {
		tradeLimit = defaultReplayTradeLimit
	}

	lookback := atVersion - replayTradeLookback
	if lookback < 1 {
		lookback = 1
	}
	windowEvents, err := s.handler.LoadEvents(ctx, aggID, lookback, atVersion)
	if err != nil {
		s.log.Error("ReplayOrderBook: load trade window", "symbol", symbol, "from", lookback, "to", atVersion, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &orderbookv1.ReplayOrderBookResponse{
		Symbol:             symbol,
		AtVersion:          int32(atVersion),
		Phase:              MarketPhaseToProto(book.Phase),
		Bids:               aggregateLevels(book.Bids.All(), true),
		Asks:               aggregateLevels(book.Asks.All(), false),
		Orders:             openOrdersFromBook(book),
		RecentTrades:       recentTradesFromEvents(windowEvents, tradeLimit),
		LuldReferencePrice: book.LULDReferencePrice,
		LuldUpperBand:      book.LULDUpperBand,
		LuldLowerBand:      book.LULDLowerBand,
		LuldBandBps:        book.LULDBandBps,
		LuldHaltDeadline:   timeToProto(book.LULDHaltDeadline),
		LuldReopenAt:       timeToProto(book.LULDReopenAt),
	}

	for i := len(windowEvents) - 1; i >= 0; i-- {
		if windowEvents[i].Version == atVersion {
			resp.AtTimestamp = timestamppb.New(windowEvents[i].Timestamp)
			break
		}
	}

	s.log.Info("ReplayOrderBook", "symbol", symbol, "at_version", atVersion, "bids", len(resp.Bids), "asks", len(resp.Asks), "orders", len(resp.Orders), "trades", len(resp.RecentTrades))

	return connect.NewResponse(resp), nil
}

// resolveReplayVersion normalizes the oneof at_version/at_timestamp into a
// concrete version. Returns 0 with nil error if no events match.
func (s *Server) resolveReplayVersion(ctx context.Context, aggregateID string, req *orderbookv1.ReplayOrderBookRequest) (int, error) {
	switch at := req.At.(type) {
	case *orderbookv1.ReplayOrderBookRequest_AtVersion:
		if at.AtVersion <= 0 {
			return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("at_version must be > 0"))
		}
		return int(at.AtVersion), nil
	case *orderbookv1.ReplayOrderBookRequest_AtTimestamp:
		if at.AtTimestamp == nil {
			return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("at_timestamp is required"))
		}
		v, err := s.handler.VersionAtTimestamp(ctx, aggregateID, at.AtTimestamp.AsTime())
		if err != nil {
			return 0, connect.NewError(connect.CodeInternal, err)
		}
		return v, nil
	default:
		return 0, connect.NewError(connect.CodeInvalidArgument, errors.New("one of at_version or at_timestamp is required"))
	}
}

// aggregateLevels groups resting orders by depth-rounded price and returns
// PriceLevel entries sorted bids-descending / asks-ascending.
func aggregateLevels(orders iter.Seq[*Order], descending bool) []*orderbookv1.PriceLevel {
	type bucket struct {
		quantity   int64
		orderCount int32
	}
	buckets := make(map[int64]*bucket)
	for o := range orders {
		p := depthPrice(o.Price)
		b := buckets[p]
		if b == nil {
			b = &bucket{}
			buckets[p] = b
		}
		b.quantity += o.RemainingQty
		b.orderCount++
	}
	if len(buckets) == 0 {
		return nil
	}
	out := make([]*orderbookv1.PriceLevel, 0, len(buckets))
	for price, b := range buckets {
		out = append(out, &orderbookv1.PriceLevel{
			Price:      price,
			Quantity:   b.quantity,
			OrderCount: b.orderCount,
		})
	}
	if descending {
		sort.Slice(out, func(i, j int) bool { return out[i].Price > out[j].Price })
	} else {
		sort.Slice(out, func(i, j int) bool { return out[i].Price < out[j].Price })
	}
	return out
}

func openOrdersFromBook(book *OrderBook) []*orderbookv1.OrderSummary {
	if len(book.Orders) == 0 {
		return nil
	}
	out := make([]*orderbookv1.OrderSummary, 0, len(book.Orders))
	for _, o := range book.Orders {
		status := orderbookv1.OrderStatus_ORDER_STATUS_OPEN
		if o.RemainingQty < o.Quantity {
			status = orderbookv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED
		}
		out = append(out, &orderbookv1.OrderSummary{
			OrderId:           o.ID,
			Symbol:            book.Symbol,
			Side:              SideToProto(o.Side),
			Price:             o.Price,
			StopPrice:         o.StopPrice,
			Quantity:          o.Quantity,
			RemainingQuantity: o.RemainingQty,
			Status:            status,
			PlacedAt:          timestamppb.New(o.PlacedAt),
			OrderType:         OrderTypeToProto(o.OrderType),
			TimeInForce:       TimeInForceToProto(o.TimeInForce),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].PlacedAt.AsTime().Before(out[j].PlacedAt.AsTime())
	})
	return out
}

// recentTradesFromEvents extracts at most limit TradeExecuted events from
// the look-back window, returning the newest `limit` trades in oldest-first
// order.
func recentTradesFromEvents(events []es.Event, limit int) []*orderbookv1.Trade {
	trades := make([]*orderbookv1.Trade, 0, len(events))
	for _, evt := range events {
		te, ok := evt.Data.(*orderbookv1.TradeExecuted)
		if !ok {
			continue
		}
		trades = append(trades, &orderbookv1.Trade{
			TradeId:     te.TradeId,
			Symbol:      te.Symbol,
			BuyOrderId:  te.BuyOrderId,
			SellOrderId: te.SellOrderId,
			Price:       te.Price,
			Quantity:    te.Quantity,
			ExecutedAt:  te.ExecutedAt,
			CrossType:   te.CrossType,
		})
	}
	if len(trades) > limit {
		trades = trades[len(trades)-limit:]
	}
	return trades
}
