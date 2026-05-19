package diagnostics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	diagnosticsv1 "github.com/ianunruh/xray/gen/diagnostics/v1"
	"github.com/ianunruh/xray/gen/diagnostics/v1/diagnosticsv1connect"
	"github.com/ianunruh/xray/internal/feesaccruer"
	"github.com/ianunruh/xray/internal/margincall"
	"github.com/ianunruh/xray/internal/portfolio"
	"github.com/ianunruh/xray/internal/reconciler"
	"github.com/ianunruh/xray/internal/settlement"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/natsstore"
	"github.com/ianunruh/xray/pkg/es/pgstore"
)

// Hard caps for diagnostics responses. Aggregates and events tables in
// production grow large; the panel only needs the most recent slice.
const (
	maxListedAggregates = 250
	maxEventsPerAggregate = 1000
)

// Server implements the DiagnosticsService Connect handler.
type Server struct {
	diagnosticsv1connect.UnimplementedDiagnosticsServiceHandler

	store             *pgstore.Store
	registry          *es.Registry
	projection        *natsstore.ProjectionManager
	accruer           *feesaccruer.Accruer
	reconciler        *reconciler.Reconciler
	marginReactor     *margincall.Reactor
	settlementReactor SettlementStatusProvider
	settlementPolicy  portfolio.SettlementPolicy
	marshal           protojson.MarshalOptions
	log               *slog.Logger
}

// SettlementStatusProvider is satisfied by *settlement.Reactor.
// Kept as a small interface so this package doesn't import
// internal/settlement (which itself imports internal/portfolio,
// risking import cycles in the wider tree).
type SettlementStatusProvider interface {
	Status() settlement.Status
}

// NewServer constructs a diagnostics server backed by the given event store,
// event-type registry, and (optionally) a projection manager. When the
// manager is nil the Projection RPCs return Unimplemented; pass a non-nil
// manager in production. The three background-loop pointers may also be
// nil — GetOperationsStatus returns zero values for any unwired source.
func NewServer(
	store *pgstore.Store,
	registry *es.Registry,
	projection *natsstore.ProjectionManager,
	accruer *feesaccruer.Accruer,
	reconciler *reconciler.Reconciler,
	marginReactor *margincall.Reactor,
	log *slog.Logger,
) *Server {
	return &Server{
		store:         store,
		registry:      registry,
		projection:    projection,
		accruer:       accruer,
		reconciler:    reconciler,
		marginReactor: marginReactor,
		marshal: protojson.MarshalOptions{
			EmitUnpopulated: true,
			Indent:          "  ",
		},
		log: log,
	}
}

// WithSettlementReactor wires the T+1 settlement reactor and the
// active SettlementPolicy. Optional so the server still boots without
// it (GetOperationsStatus then surfaces a zero/disabled status).
func (s *Server) WithSettlementReactor(r SettlementStatusProvider, policy portfolio.SettlementPolicy) *Server {
	s.settlementReactor = r
	s.settlementPolicy = policy
	return s
}

func (s *Server) ListAggregates(ctx context.Context, req *connect.Request[diagnosticsv1.ListAggregatesRequest]) (*connect.Response[diagnosticsv1.ListAggregatesResponse], error) {
	summaries, err := s.store.ListAggregates(ctx, req.Msg.Filter, maxListedAggregates)
	if err != nil {
		s.log.Warn("ListAggregates failed", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &diagnosticsv1.ListAggregatesResponse{
		Aggregates: make([]*diagnosticsv1.AggregateSummary, 0, len(summaries)),
	}
	for _, sum := range summaries {
		resp.Aggregates = append(resp.Aggregates, &diagnosticsv1.AggregateSummary{
			AggregateId:  sum.AggregateID,
			Type:         aggregateType(sum.AggregateID),
			EventCount:   int32(sum.EventCount),
			FirstEventAt: timestamppb.New(sum.FirstEventAt),
			LastEventAt:  timestamppb.New(sum.LastEventAt),
		})
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) GetAggregateEvents(ctx context.Context, req *connect.Request[diagnosticsv1.GetAggregateEventsRequest]) (*connect.Response[diagnosticsv1.GetAggregateEventsResponse], error) {
	aggregateID := strings.TrimSpace(req.Msg.AggregateId)
	if aggregateID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("aggregate_id is required"))
	}

	raws, err := s.store.LoadLatest(ctx, aggregateID, maxEventsPerAggregate)
	if err != nil {
		s.log.Warn("GetAggregateEvents load failed", "aggregate_id", aggregateID, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &diagnosticsv1.GetAggregateEventsResponse{
		Events: make([]*diagnosticsv1.DiagnosticEvent, 0, len(raws)),
	}
	for _, raw := range raws {
		resp.Events = append(resp.Events, s.toDiagnosticEvent(raw))
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) GetEventChain(ctx context.Context, req *connect.Request[diagnosticsv1.GetEventChainRequest]) (*connect.Response[diagnosticsv1.GetEventChainResponse], error) {
	correlationID := strings.TrimSpace(req.Msg.CorrelationId)
	if correlationID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("correlation_id is required"))
	}

	raws, err := s.store.LoadByCorrelationID(ctx, correlationID)
	if err != nil {
		s.log.Warn("GetEventChain load failed", "correlation_id", correlationID, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &diagnosticsv1.GetEventChainResponse{
		Events: make([]*diagnosticsv1.DiagnosticEvent, 0, len(raws)),
	}
	for _, raw := range raws {
		resp.Events = append(resp.Events, s.toDiagnosticEvent(raw))
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) toDiagnosticEvent(raw es.RawEvent) *diagnosticsv1.DiagnosticEvent {
	jsonStr, err := s.encodeEvent(raw)
	if err != nil {
		s.log.Warn("encode event failed", "aggregate_id", raw.AggregateID, "event_id", raw.ID, "type", raw.Type, "error", err)
		jsonStr = fmt.Sprintf("{\"_error\": %q}", err.Error())
	}
	return &diagnosticsv1.DiagnosticEvent{
		Id:            raw.ID,
		AggregateId:   raw.AggregateID,
		Type:          raw.Type,
		Version:       int32(raw.Version),
		Position:      raw.Position,
		Timestamp:     timestamppb.New(raw.Timestamp),
		DataJson:      jsonStr,
		CausationId:   raw.CausationID,
		CorrelationId: raw.CorrelationID,
	}
}

func (s *Server) encodeEvent(raw es.RawEvent) (string, error) {
	evt, err := s.registry.Deserialize(raw)
	if err != nil {
		return "", err
	}
	b, err := s.marshal.Marshal(evt.Data)
	if err != nil {
		return "", fmt.Errorf("marshal json: %w", err)
	}
	return string(b), nil
}

// aggregateType extracts the type prefix from an aggregate ID of the form
// "type:id". Returns the full ID if no separator is present.
func aggregateType(aggregateID string) string {
	if prefix, _, ok := strings.Cut(aggregateID, ":"); ok {
		return prefix
	}
	return aggregateID
}

// --- ProjectionManager-backed RPCs -----------------------------------------

func (s *Server) ListProjections(ctx context.Context, _ *connect.Request[diagnosticsv1.ListProjectionsRequest]) (*connect.Response[diagnosticsv1.ListProjectionsResponse], error) {
	if s.projection == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("projection manager not configured"))
	}
	statuses, err := s.projection.List(ctx)
	if err != nil {
		s.log.Warn("ListProjections failed", "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &diagnosticsv1.ListProjectionsResponse{
		Projections: make([]*diagnosticsv1.ProjectionStatus, 0, len(statuses)),
	}
	for _, st := range statuses {
		resp.Projections = append(resp.Projections, projectionStatusToProto(st))
	}
	return connect.NewResponse(resp), nil
}

func (s *Server) RebuildProjection(_ context.Context, req *connect.Request[diagnosticsv1.RebuildProjectionRequest]) (*connect.Response[diagnosticsv1.RebuildProjectionResponse], error) {
	if s.projection == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("projection manager not configured"))
	}
	name := strings.TrimSpace(req.Msg.Name)
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}

	// Validate up front so the client gets a sync error for bad requests.
	// AlreadyRebuilding is also detected here to avoid spawning a goroutine
	// just to discard the result.
	st, err := s.projection.Status(context.Background(), name)
	if err != nil {
		if errors.Is(err, natsstore.ErrUnknownConsumer) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if !st.Rebuildable {
		return nil, connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("not rebuildable: %s", st.ReasonNotRebuildable))
	}
	if st.Phase == natsstore.PhaseRebuilding {
		return nil, connect.NewError(connect.CodeFailedPrecondition, natsstore.ErrAlreadyRebuilding)
	}

	// Rebuild runs in the background so the RPC returns immediately. Use
	// a detached context so the rebuild survives this request's cancel;
	// the manager itself has no long-running ops past the consumer
	// restart's catch-up phase.
	go func() {
		if err := s.projection.Rebuild(context.Background(), name); err != nil {
			s.log.Error("rebuild failed", "name", name, "error", err)
		}
	}()
	return connect.NewResponse(&diagnosticsv1.RebuildProjectionResponse{}), nil
}

func (s *Server) StreamProjectionProgress(ctx context.Context, req *connect.Request[diagnosticsv1.StreamProjectionProgressRequest], stream *connect.ServerStream[diagnosticsv1.ProjectionProgress]) error {
	if s.projection == nil {
		return connect.NewError(connect.CodeUnimplemented, fmt.Errorf("projection manager not configured"))
	}
	name := strings.TrimSpace(req.Msg.Name)
	if name == "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("name is required"))
	}

	ch, cancel, err := s.projection.Subscribe(name)
	if err != nil {
		if errors.Is(err, natsstore.ErrUnknownConsumer) {
			return connect.NewError(connect.CodeNotFound, err)
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	defer cancel()

	// Emit one initial status so a late subscriber gets the current
	// phase immediately rather than waiting for the next batch tick.
	if st, err := s.projection.Status(ctx, name); err == nil {
		if err := stream.Send(initialProgress(st)); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case evt, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(progressToProto(evt)); err != nil {
				return err
			}
			// Terminal phases close the stream so the client knows
			// the rebuild ended.
			if evt.Phase == natsstore.PhaseRunning || evt.Phase == natsstore.PhaseFailed {
				return nil
			}
		}
	}
}

func projectionStatusToProto(st natsstore.ProjectionStatus) *diagnosticsv1.ProjectionStatus {
	out := &diagnosticsv1.ProjectionStatus{
		Name:                 st.Name,
		Phase:                phaseToProto(st.Phase),
		Checkpoint:           st.Checkpoint,
		HeadSequence:         st.HeadSequence,
		Lag:                  st.Lag,
		Rebuildable:          st.Rebuildable,
		ReasonNotRebuildable: st.ReasonNotRebuildable,
		RebuildLastError:     st.RebuildLastError,
		ProjectionCount:      int32(st.ProjectionCount),
		ResettableCount:      int32(st.ResettableCount),
	}
	if !st.RebuildStartedAt.IsZero() {
		out.RebuildStartedAt = timestamppb.New(st.RebuildStartedAt)
	}
	return out
}

func initialProgress(st natsstore.ProjectionStatus) *diagnosticsv1.ProjectionProgress {
	return &diagnosticsv1.ProjectionProgress{
		Name:         st.Name,
		Phase:        phaseToProto(st.Phase),
		Position:     st.Checkpoint,
		HeadSequence: st.HeadSequence,
		Error:        st.RebuildLastError,
		At:           timestamppb.Now(),
	}
}

func progressToProto(evt natsstore.ProgressEvent) *diagnosticsv1.ProjectionProgress {
	out := &diagnosticsv1.ProjectionProgress{
		Name:         evt.Name,
		Phase:        phaseToProto(evt.Phase),
		Position:     evt.Position,
		HeadSequence: evt.HeadSequence,
		EventsPerSec: evt.EventsPerSec,
		EtaSeconds:   evt.ETASeconds,
		BatchSize:    int32(evt.BatchSize),
		Error:        evt.Err,
	}
	if !evt.At.IsZero() {
		out.At = timestamppb.New(evt.At)
	}
	return out
}

// GetOperationsStatus aggregates the live state of the background
// loops. Any unwired source surfaces as a zero-valued message so the
// UI can render "—" rather than a partial response.
func (s *Server) GetOperationsStatus(ctx context.Context, _ *connect.Request[diagnosticsv1.GetOperationsStatusRequest]) (*connect.Response[diagnosticsv1.GetOperationsStatusResponse], error) {
	out := &diagnosticsv1.GetOperationsStatusResponse{
		Accruer:           &diagnosticsv1.AccruerStatus{},
		Reconciler:        &diagnosticsv1.ReconcilerStatus{},
		MarginReactor:     &diagnosticsv1.MarginReactorStatus{},
		SettlementReactor: &diagnosticsv1.SettlementReactorStatus{},
	}
	if s.accruer != nil {
		a := s.accruer.Status()
		out.Accruer.IntervalMs = a.Interval.Milliseconds()
		out.Accruer.MinElapsedMs = a.MinElapsed.Milliseconds()
		if !a.LastTickAt.IsZero() {
			out.Accruer.LastTickAt = timestamppb.New(a.LastTickAt)
		}
		out.Accruer.LastTickMs = a.LastTickDuration.Milliseconds()
		out.Accruer.LastTickAccounts = int32(a.LastTickAccounts)
		out.Accruer.LastTickFailed = int32(a.LastTickFailed)
	}
	if s.reconciler != nil {
		r := s.reconciler.Status()
		out.Reconciler.IntervalMs = r.Interval.Milliseconds()
		if !r.LastTickAt.IsZero() {
			out.Reconciler.LastTickAt = timestamppb.New(r.LastTickAt)
		}
		out.Reconciler.LastTickMs = r.LastTickDuration.Milliseconds()
		out.Reconciler.LastTickSagasReconciled = int32(r.LastTickSagasReconciled)
		out.Reconciler.LastTickMarginCallsEvaluated = int32(r.LastTickMarginCallsEvaluated)
		out.Reconciler.LastTickFailedSagas = int32(r.LastTickFailedSagas)
	}
	if s.marginReactor != nil {
		m := s.marginReactor.Status(ctx)
		out.MarginReactor.GraceMs = m.Grace.Milliseconds()
		out.MarginReactor.ActiveCallCount = int32(m.ActiveCallCount)
	}
	out.SettlementReactor.SettlementEnabled = s.settlementPolicy.Enabled
	out.SettlementReactor.WindowMs = s.settlementPolicy.Window.Milliseconds()
	if s.settlementReactor != nil {
		sr := s.settlementReactor.Status()
		out.SettlementReactor.IntervalMs = sr.Interval.Milliseconds()
		if !sr.LastTickAt.IsZero() {
			out.SettlementReactor.LastTickAt = timestamppb.New(sr.LastTickAt)
		}
		out.SettlementReactor.LastTickMs = sr.LastTickDuration.Milliseconds()
		out.SettlementReactor.LastTickAccounts = int32(sr.LastTickAccounts)
		out.SettlementReactor.LastTickCleared = int32(sr.LastTickCleared)
		out.SettlementReactor.LastTickFailed = int32(sr.LastTickFailed)
	}
	return connect.NewResponse(out), nil
}

func phaseToProto(p natsstore.ProjectionPhase) diagnosticsv1.ProjectionPhase {
	switch p {
	case natsstore.PhaseRunning:
		return diagnosticsv1.ProjectionPhase_PROJECTION_PHASE_RUNNING
	case natsstore.PhaseRebuilding:
		return diagnosticsv1.ProjectionPhase_PROJECTION_PHASE_REBUILDING
	case natsstore.PhaseStopped:
		return diagnosticsv1.ProjectionPhase_PROJECTION_PHASE_STOPPED
	case natsstore.PhaseFailed:
		return diagnosticsv1.ProjectionPhase_PROJECTION_PHASE_FAILED
	default:
		return diagnosticsv1.ProjectionPhase_PROJECTION_PHASE_UNSPECIFIED
	}
}
