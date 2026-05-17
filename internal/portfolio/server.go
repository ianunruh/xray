package portfolio

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"connectrpc.com/connect"

	orderbookv1 "github.com/ianunruh/xray/gen/orderbook/v1"
	portfoliov1 "github.com/ianunruh/xray/gen/portfolio/v1"
	"github.com/ianunruh/xray/gen/portfolio/v1/portfoliov1connect"
	"github.com/ianunruh/xray/internal/margin"
	"github.com/ianunruh/xray/internal/orderbook"
	"github.com/ianunruh/xray/pkg/es"
)

var (
	ErrInvalidPrice    = errors.New("price must be positive")
	ErrInvalidQuantity = errors.New("quantity must be positive")
)

type PortfolioReader interface {
	GetPortfolio(ctx context.Context, accountID string) (*portfoliov1.GetPortfolioResponse, error)
	ListPortfolios(ctx context.Context) ([]string, error)
}

// Marker exposes mark-to-market prices for symbols. Implemented by
// orderbook.MarkProjection (via its GetMarkPrice method); kept as a
// small interface so the portfolio server doesn't depend on the
// orderbook package directly.
type Marker interface {
	GetMarkPrice(symbol string) (price int64, at time.Time, ok bool)
}

type Server struct {
	portfoliov1connect.UnimplementedPortfolioServiceHandler

	portfolioHandler  *es.Handler[*Portfolio]
	orderbookHandler  *es.Handler[*orderbook.OrderBook]
	reader            PortfolioReader
	pnlReader         PnLReader
	marker            Marker
	marginCallsReader MarginCallsReader
	broker            *PortfolioBroker
	log               *slog.Logger
}

func NewServer(
	portfolioHandler *es.Handler[*Portfolio],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	reader PortfolioReader,
	pnlReader PnLReader,
	marker Marker,
	marginCallsReader MarginCallsReader,
	broker *PortfolioBroker,
	log *slog.Logger,
) *Server {
	return &Server{
		portfolioHandler:  portfolioHandler,
		orderbookHandler:  orderbookHandler,
		reader:            reader,
		pnlReader:         pnlReader,
		marker:            marker,
		marginCallsReader: marginCallsReader,
		broker:            broker,
		log:               log,
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

func (s *Server) ListPortfolios(ctx context.Context, req *connect.Request[portfoliov1.ListPortfoliosRequest]) (*connect.Response[portfoliov1.ListPortfoliosResponse], error) {
	ids, err := s.reader.ListPortfolios(ctx)
	if err != nil {
		s.log.Error("ListPortfolios failed", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.log.Info("ListPortfolios", "count", len(ids))

	return connect.NewResponse(&portfoliov1.ListPortfoliosResponse{
		AccountIds: ids,
	}), nil
}

func (s *Server) GetPnL(ctx context.Context, req *connect.Request[portfoliov1.GetPnLRequest]) (*connect.Response[portfoliov1.GetPnLResponse], error) {
	resp, err := s.pnlReader.GetPnL(ctx, req.Msg.AccountId)
	if err != nil {
		s.log.Error("GetPnL failed", "account_id", req.Msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) GetMarginSnapshot(ctx context.Context, req *connect.Request[portfoliov1.GetMarginSnapshotRequest]) (*connect.Response[portfoliov1.GetMarginSnapshotResponse], error) {
	accountID := req.Msg.AccountId
	p, err := s.portfolioHandler.Load(ctx, AggregateID(accountID))
	if err != nil {
		s.log.Error("GetMarginSnapshot load failed", "account_id", accountID, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := buildMarginSnapshot(accountID, p, s.marker)
	return connect.NewResponse(resp), nil
}

func (s *Server) ListMarginCalls(ctx context.Context, req *connect.Request[portfoliov1.ListMarginCallsRequest]) (*connect.Response[portfoliov1.ListMarginCallsResponse], error) {
	if s.marginCallsReader == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("margin calls reader not configured"))
	}
	calls, err := s.marginCallsReader.ListMarginCalls(ctx, req.Msg.AccountId, req.Msg.Limit)
	if err != nil {
		s.log.Error("ListMarginCalls failed", "account_id", req.Msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&portfoliov1.ListMarginCallsResponse{Calls: calls}), nil
}

func (s *Server) PreviewOrderImpact(ctx context.Context, req *connect.Request[portfoliov1.PreviewOrderImpactRequest]) (*connect.Response[portfoliov1.PreviewOrderImpactResponse], error) {
	msg := req.Msg
	if msg.AccountId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("account_id required"))
	}
	if msg.Quantity <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("quantity must be positive"))
	}
	p, err := s.portfolioHandler.Load(ctx, AggregateID(msg.AccountId))
	if err != nil {
		s.log.Error("PreviewOrderImpact load failed", "account_id", msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := s.buildPreview(ctx, p, msg)
	return connect.NewResponse(resp), nil
}

// buildPreview computes deltas directly off the order parameters
// rather than cloning the aggregate — same result, less plumbing.
func (s *Server) buildPreview(ctx context.Context, p *Portfolio, req *portfoliov1.PreviewOrderImpactRequest) *portfoliov1.PreviewOrderImpactResponse {
	resp := &portfoliov1.PreviewOrderImpactResponse{}
	current := ComputeMarginStatus(p, s.marker)

	fillPrice, fillWarn := s.estimateFillPrice(ctx, req)
	if fillWarn != "" {
		resp.Warnings = append(resp.Warnings, fillWarn)
	}
	resp.EstimatedFillPrice = fillPrice

	// Use current mark if available; fall back to fill price as the
	// "this is the only price signal we have" assumption.
	mark, _, hasMark := lookupMark(s.marker, req.Symbol)
	if !hasMark {
		mark = fillPrice
	}

	qty := req.Quantity
	isShort := req.PositionSide == orderbookv1.PositionSide_POSITION_SIDE_SHORT
	switch {
	case req.Side == orderbookv1.Side_SIDE_BUY && !isShort:
		if fillPrice > 0 {
			// Long buy on margin: cash needed = full notional, but
			// the impact against buying power is the *notional itself*
			// since buying power already accounts for leverage. (The
			// borrowed portion shows up as cash going negative; the
			// equity contribution shows up as maintenance accruing.)
			resp.BuyingPowerImpact = fillPrice * qty
		}
		// Acquiring at fillPrice, immediately marked at mark. Equity
		// shifts by the price-vs-mark spread. Long maintenance grows.
		resp.ProjectedEquity = current.Equity + qty*(mark-fillPrice)
		resp.ProjectedMaintenanceRequirement = current.MaintenanceRequirement + margin.MaintenanceForLong(mark, qty)
	case req.Side == orderbookv1.Side_SIDE_SELL && !isShort:
		resp.BuyingPowerImpact = 0
		// Selling at fillPrice, releasing position marked at mark.
		resp.ProjectedEquity = current.Equity + qty*(fillPrice-mark)
		resp.ProjectedMaintenanceRequirement = current.MaintenanceRequirement - margin.MaintenanceForLong(mark, qty)
	case req.Side == orderbookv1.Side_SIDE_SELL && isShort:
		if fillPrice > 0 {
			resp.BuyingPowerImpact = margin.CollateralForShortOpen(fillPrice, qty)
		}
		resp.ProjectedEquity = current.Equity + qty*(fillPrice-mark)
		resp.ProjectedMaintenanceRequirement = current.MaintenanceRequirement + margin.MaintenanceRequirement(mark, qty)
	case req.Side == orderbookv1.Side_SIDE_BUY && isShort:
		resp.BuyingPowerImpact = 0
		short := p.ShortPositions[req.Symbol]
		if short != nil {
			// Realized PnL from covering at fillPrice vs avg open.
			resp.ProjectedEquity = current.Equity + qty*(short.AvgOpenPrice-fillPrice)
			resp.ProjectedMaintenanceRequirement = current.MaintenanceRequirement - margin.MaintenanceRequirement(mark, qty)
		} else {
			resp.Warnings = append(resp.Warnings, "no open short in this symbol to cover")
			resp.ProjectedEquity = current.Equity
			resp.ProjectedMaintenanceRequirement = current.MaintenanceRequirement
		}
	}

	resp.ProjectedMarginExcess = resp.ProjectedEquity - resp.ProjectedMaintenanceRequirement
	resp.ProjectedInCall = resp.ProjectedMarginExcess < 0
	// Check against current buying power (which is leveraged), not
	// against raw cash — a $5k buy is sufficient when buying power
	// is $20k even though cash might be $10k.
	currentBP := margin.BuyingPower(current.Equity, current.MaintenanceRequirement)
	resp.SufficientBuyingPower = currentBP >= resp.BuyingPowerImpact

	if !resp.SufficientBuyingPower {
		resp.Warnings = append(resp.Warnings, "insufficient buying power")
	}
	if resp.ProjectedInCall && !current.InCall {
		resp.Warnings = append(resp.Warnings, "this order would put the account in margin call")
	}
	return resp
}

// estimateFillPrice returns the price the server expects this order
// to clear at. For limit orders, that's the typed price. For market
// orders, walk the relevant side of the book and average. Returns 0
// (with a warning) when there's no liquidity to walk.
func (s *Server) estimateFillPrice(ctx context.Context, req *portfoliov1.PreviewOrderImpactRequest) (int64, string) {
	if req.OrderType != orderbookv1.OrderType_ORDER_TYPE_MARKET {
		return req.Price, ""
	}
	if s.orderbookHandler == nil {
		return req.Price, "market order preview needs orderbook access"
	}
	book, err := s.orderbookHandler.Load(ctx, orderbook.AggregateID(req.Symbol))
	if err != nil {
		return 0, "failed to load orderbook for preview"
	}
	if req.Side == orderbookv1.Side_SIDE_BUY {
		cost, ok := book.EstimateMarketBuyCost(req.Quantity)
		if !ok {
			return 0, "no ask liquidity for market buy"
		}
		return cost / req.Quantity, ""
	}
	proceeds, ok := book.EstimateMarketSellProceeds(req.Quantity)
	if !ok {
		return 0, "no bid liquidity for market sell"
	}
	return proceeds / req.Quantity, ""
}

// MarginStatus is the slim view of the margin computation used by
// the margin-call reactor — full mark/PnL detail lives in
// buildMarginSnapshot, but the reactor only needs equity, requirement,
// breach state, and a remediation target.
type MarginStatus struct {
	Equity                      int64
	LongMaintenanceRequirement  int64
	ShortMaintenanceRequirement int64
	MaintenanceRequirement      int64 // sum of the two above
	InCall                      bool

	// Liquidation targets — the reactor picks whichever side has the
	// larger market value when remediating a breach.
	LargestShortSymbol string
	LargestShortQty    int64
	LargestShortMark   int64
	LargestLongSymbol  string
	LargestLongQty     int64
	LargestLongMark    int64

	// AnyMarkMissing flags that one or more positions lack a mark;
	// equity treats their contribution as zero, which can mask a breach.
	AnyMarkMissing bool
}

// ComputeMarginStatus is the lightweight version of buildMarginSnapshot
// — no proto, no per-position breakdown, just enough for the reactor
// to decide whether to issue/cover a call and which position to liquidate.
func ComputeMarginStatus(p *Portfolio, marker Marker) MarginStatus {
	status := MarginStatus{}

	equity := p.CashBalance + p.CashHeld + p.CollateralPool + p.ProceedsPool
	for _, h := range p.CollateralHeldBySaga {
		equity += h.Amount
	}
	for sym, h := range p.Holdings {
		if h.Quantity <= 0 {
			continue
		}
		mark, _, ok := lookupMark(marker, sym)
		if !ok {
			status.AnyMarkMissing = true
			continue
		}
		marketValue := mark * h.Quantity
		equity += marketValue
		status.LongMaintenanceRequirement += margin.MaintenanceForLong(mark, h.Quantity)
		if marketValue > status.LargestLongQty*status.LargestLongMark {
			status.LargestLongSymbol = sym
			status.LargestLongQty = h.Quantity
			status.LargestLongMark = mark
		}
	}

	for sym, short := range p.ShortPositions {
		mark, _, ok := lookupMark(marker, sym)
		if !ok {
			status.AnyMarkMissing = true
			continue
		}
		liability := mark * short.Quantity
		equity -= liability
		status.ShortMaintenanceRequirement += margin.MaintenanceRequirement(mark, short.Quantity)
		if liability > status.LargestShortQty*status.LargestShortMark {
			status.LargestShortSymbol = sym
			status.LargestShortQty = short.Quantity
			status.LargestShortMark = mark
		}
	}

	status.Equity = equity
	status.MaintenanceRequirement = status.LongMaintenanceRequirement + status.ShortMaintenanceRequirement
	status.InCall = equity < status.MaintenanceRequirement
	return status
}

// buildMarginSnapshot is the pure-data computation, factored out so
// tests can drive it without spinning up a Connect server.
func buildMarginSnapshot(accountID string, p *Portfolio, marker Marker) *portfoliov1.GetMarginSnapshotResponse {
	resp := &portfoliov1.GetMarginSnapshotResponse{
		AccountId:      accountID,
		CashBalance:    p.CashBalance,
		CashHeld:       p.CashHeld,
		CollateralPool: p.CollateralPool,
		ProceedsPool:   p.ProceedsPool,
		MarginLoan:     p.MarginLoan(),
	}
	for _, h := range p.CollateralHeldBySaga {
		resp.CollateralHeldPreFill += h.Amount
	}

	// Deterministic ordering so responses (and tests) don't depend on
	// map iteration order.
	longSymbols := sortedKeys(p.Holdings)
	shortSymbols := sortedKeysShort(p.ShortPositions)

	for _, sym := range longSymbols {
		h := p.Holdings[sym]
		if h.Quantity <= 0 {
			continue
		}
		avg := int64(0)
		if h.Quantity > 0 {
			avg = h.TotalCost / h.Quantity
		}
		info := &portfoliov1.PositionMarginInfo{
			Symbol:   sym,
			Side:     orderbookv1.PositionSide_POSITION_SIDE_LONG,
			Quantity: h.Quantity,
			AvgPrice: avg,
		}
		if mark, _, ok := lookupMark(marker, sym); ok {
			info.MarkPrice = mark
			info.MarketValue = mark * h.Quantity
			info.UnrealizedPnl = (mark - avg) * h.Quantity
			resp.LongMarketValue += info.MarketValue
			resp.LongMaintenanceRequirement += margin.MaintenanceForLong(mark, h.Quantity)
		} else {
			info.MarkMissing = true
			resp.MissingMarks = append(resp.MissingMarks, sym)
		}
		resp.Positions = append(resp.Positions, info)
	}

	for _, sym := range shortSymbols {
		short := p.ShortPositions[sym]
		info := &portfoliov1.PositionMarginInfo{
			Symbol:   sym,
			Side:     orderbookv1.PositionSide_POSITION_SIDE_SHORT,
			Quantity: short.Quantity,
			AvgPrice: short.AvgOpenPrice,
		}
		if mark, _, ok := lookupMark(marker, sym); ok {
			info.MarkPrice = mark
			info.MarketValue = mark * short.Quantity
			info.UnrealizedPnl = (short.AvgOpenPrice - mark) * short.Quantity
			resp.ShortLiability += info.MarketValue
			resp.ShortMaintenanceRequirement += margin.MaintenanceRequirement(mark, short.Quantity)
		} else {
			info.MarkMissing = true
			resp.MissingMarks = append(resp.MissingMarks, sym)
		}
		resp.Positions = append(resp.Positions, info)
	}

	resp.Equity = resp.CashBalance + resp.CashHeld + resp.CollateralPool +
		resp.ProceedsPool + resp.CollateralHeldPreFill +
		resp.LongMarketValue - resp.ShortLiability
	resp.MaintenanceRequirement = resp.LongMaintenanceRequirement + resp.ShortMaintenanceRequirement
	resp.MarginExcess = resp.Equity - resp.MaintenanceRequirement
	resp.MarginCall = resp.MarginExcess < 0
	// Leveraged buying power: how much new exposure the user can add
	// before hitting the maintenance floor. Real brokers cap at 2:1
	// for Reg-T accounts; we use margin.BuyingPower as the policy.
	resp.BuyingPower = margin.BuyingPower(resp.Equity, resp.MaintenanceRequirement)

	return resp
}

func lookupMark(marker Marker, symbol string) (int64, time.Time, bool) {
	if marker == nil {
		return 0, time.Time{}, false
	}
	return marker.GetMarkPrice(symbol)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// sortedKeysShort exists only because Go generics don't let us use the
// same sortedKeys helper with a different value type without specifying
// both — kept separate for clarity.
func sortedKeysShort(m map[string]*ShortPosition) []string {
	return sortedKeys(m)
}
