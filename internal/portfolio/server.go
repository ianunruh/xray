package portfolio

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	portfolioHandler    *es.Handler[*Portfolio]
	orderbookHandler    *es.Handler[*orderbook.OrderBook]
	reader              PortfolioReader
	pnlReader           PnLReader
	marker              Marker
	marginCallsReader   MarginCallsReader
	feesReader          FeesReader
	shortInterestReader ShortInterestReader
	pendingReader       PendingSettlementsReader
	broker              *PortfolioBroker
	log                 *slog.Logger
}

func NewServer(
	portfolioHandler *es.Handler[*Portfolio],
	orderbookHandler *es.Handler[*orderbook.OrderBook],
	reader PortfolioReader,
	pnlReader PnLReader,
	marker Marker,
	marginCallsReader MarginCallsReader,
	feesReader FeesReader,
	shortInterestReader ShortInterestReader,
	broker *PortfolioBroker,
	log *slog.Logger,
) *Server {
	return &Server{
		portfolioHandler:    portfolioHandler,
		orderbookHandler:    orderbookHandler,
		reader:              reader,
		pnlReader:           pnlReader,
		marker:              marker,
		marginCallsReader:   marginCallsReader,
		feesReader:          feesReader,
		shortInterestReader: shortInterestReader,
		broker:              broker,
		log:                 log,
	}
}

// WithPendingSettlementsReader installs the optional pending-leg
// reader. When set, GetPortfolio populates pending_cash_credits /
// pending_cash_debits with the per-account sums; otherwise those
// fields stay zero. Optional so the server still boots in test
// configurations that don't wire the projection.
func (s *Server) WithPendingSettlementsReader(r PendingSettlementsReader) *Server {
	s.pendingReader = r
	return s
}

// ShortInterestReader exposes the venue-wide short-interest aggregate.
// Satisfied by PgShortsBySymbolProjection.
type ShortInterestReader interface {
	ListShortInterest(ctx context.Context) ([]*SymbolShortInterest, error)
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
	resp, err := s.loadPortfolioResponse(ctx, req.Msg.AccountId)
	if err != nil {
		s.log.Error("GetPortfolio failed", "account_id", req.Msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) StreamPortfolio(ctx context.Context, req *connect.Request[portfoliov1.StreamPortfolioRequest], stream *connect.ServerStream[portfoliov1.GetPortfolioResponse]) error {
	accountID := req.Msg.AccountId

	resp, err := s.loadPortfolioResponse(ctx, accountID)
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
			resp, err := s.loadPortfolioResponse(ctx, accountID)
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

// loadPortfolioResponse reads the PG-backed portfolio response and,
// when the pending-settlements reader is wired, decorates it with
// the pending cash bucket sums and per-symbol pending shares.
// Keeping the two reads here (rather than inside
// PgPortfolioProjection.GetPortfolio) means the projections stay
// decoupled.
func (s *Server) loadPortfolioResponse(ctx context.Context, accountID string) (*portfoliov1.GetPortfolioResponse, error) {
	resp, err := s.reader.GetPortfolio(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if s.pendingReader != nil {
		totals, err := s.pendingReader.PendingTotals(ctx, accountID)
		if err != nil {
			return nil, err
		}
		resp.PendingCashCredits = totals.Credits
		resp.PendingCashDebits = totals.Debits

		pendingShares, err := s.pendingReader.PendingSharesBySymbol(ctx, accountID)
		if err != nil {
			return nil, err
		}
		for _, h := range resp.Holdings {
			if qty, ok := pendingShares[h.Symbol]; ok {
				h.PendingShares = qty
			}
		}
	}
	return resp, nil
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

func (s *Server) ListFeeHistory(ctx context.Context, req *connect.Request[portfoliov1.ListFeeHistoryRequest]) (*connect.Response[portfoliov1.ListFeeHistoryResponse], error) {
	if s.feesReader == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("fees reader not configured"))
	}
	records, err := s.feesReader.ListFeeHistory(ctx, req.Msg.AccountId, req.Msg.Limit)
	if err != nil {
		s.log.Error("ListFeeHistory failed", "account_id", req.Msg.AccountId, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&portfoliov1.ListFeeHistoryResponse{Records: records}), nil
}

func (s *Server) ListShortInterest(ctx context.Context, _ *connect.Request[portfoliov1.ListShortInterestRequest]) (*connect.Response[portfoliov1.ListShortInterestResponse], error) {
	if s.shortInterestReader == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("short interest reader not configured"))
	}
	entries, err := s.shortInterestReader.ListShortInterest(ctx)
	if err != nil {
		s.log.Error("ListShortInterest failed", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	out := make([]*portfoliov1.SymbolShortInterest, 0, len(entries))
	for _, e := range entries {
		out = append(out, &portfoliov1.SymbolShortInterest{
			Symbol:        e.Symbol,
			TotalQuantity: e.TotalQty,
			AccountCount:  e.AccountCount,
		})
	}
	return connect.NewResponse(&portfoliov1.ListShortInterestResponse{Entries: out}), nil
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
	book := s.loadBookForPreview(ctx, msg.Symbol, msg.OrderType)
	impact := ComputeOrderImpact(ctx, p, s.marker, book, OrderPlan{
		Symbol:       msg.Symbol,
		Side:         msg.Side,
		PositionSide: msg.PositionSide,
		OrderType:    msg.OrderType,
		Price:        msg.Price,
		Quantity:     msg.Quantity,
	})
	return connect.NewResponse(&portfoliov1.PreviewOrderImpactResponse{
		BuyingPowerImpact:               impact.BuyingPowerImpact,
		ProjectedEquity:                 impact.ProjectedEquity,
		ProjectedMaintenanceRequirement: impact.ProjectedMaintenanceRequirement,
		ProjectedMarginExcess:           impact.ProjectedMarginExcess,
		ProjectedInCall:                 impact.ProjectedInCall,
		SufficientBuyingPower:           impact.SufficientBuyingPower,
		EstimatedFillPrice:              impact.EstimatedFillPrice,
		EstimatedFee:                    impact.EstimatedFee,
		Warnings:                        impact.Warnings,
	}), nil
}

// loadBookForPreview returns nil for limit orders (no book needed),
// otherwise loads the orderbook aggregate so ComputeOrderImpact can
// walk it. A nil return on a market order falls back to the typed
// price with a warning.
func (s *Server) loadBookForPreview(ctx context.Context, symbol string, orderType orderbookv1.OrderType) BookEstimator {
	if orderType != orderbookv1.OrderType_ORDER_TYPE_MARKET {
		return nil
	}
	if s.orderbookHandler == nil {
		return nil
	}
	book, err := s.orderbookHandler.Load(ctx, orderbook.AggregateID(symbol))
	if err != nil {
		s.log.Warn("preview: failed to load orderbook", "symbol", symbol, "error", err)
		return nil
	}
	return book
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
		SettledCash:    p.SettledCash,
	}
	for _, h := range p.CollateralHeldBySaga {
		resp.CollateralHeldPreFill += h.Amount
	}
	for _, leg := range p.PendingLegs {
		if leg.CashAmount > 0 {
			resp.PendingCashCredits += leg.CashAmount
		} else if leg.CashAmount < 0 {
			resp.PendingCashDebits += -leg.CashAmount
		}
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
	if p.ActiveMarginCall != nil && !p.ActiveMarginCall.GraceExpiresAt.IsZero() {
		resp.MarginCallGraceExpiresAt = timestamppb.New(p.ActiveMarginCall.GraceExpiresAt)
	}
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
