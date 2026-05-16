package diagnostics

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	diagnosticsv1 "github.com/ianunruh/xray/gen/diagnostics/v1"
	"github.com/ianunruh/xray/gen/diagnostics/v1/diagnosticsv1connect"
	"github.com/ianunruh/xray/pkg/es"
	"github.com/ianunruh/xray/pkg/es/pgstore"
)

// Server implements the DiagnosticsService Connect handler.
type Server struct {
	diagnosticsv1connect.UnimplementedDiagnosticsServiceHandler

	store    *pgstore.Store
	registry *es.Registry
	marshal  protojson.MarshalOptions
	log      *slog.Logger
}

// NewServer constructs a diagnostics server backed by the given event store
// and event-type registry.
func NewServer(store *pgstore.Store, registry *es.Registry, log *slog.Logger) *Server {
	return &Server{
		store:    store,
		registry: registry,
		marshal: protojson.MarshalOptions{
			EmitUnpopulated: true,
			Indent:          "  ",
		},
		log: log,
	}
}

func (s *Server) ListAggregates(ctx context.Context, req *connect.Request[diagnosticsv1.ListAggregatesRequest]) (*connect.Response[diagnosticsv1.ListAggregatesResponse], error) {
	summaries, err := s.store.ListAggregates(ctx, req.Msg.Filter)
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

	raws, err := s.store.Load(ctx, aggregateID)
	if err != nil {
		s.log.Warn("GetAggregateEvents load failed", "aggregate_id", aggregateID, "error", err)
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := &diagnosticsv1.GetAggregateEventsResponse{
		Events: make([]*diagnosticsv1.DiagnosticEvent, 0, len(raws)),
	}
	for _, raw := range raws {
		jsonStr, err := s.encodeEvent(raw)
		if err != nil {
			s.log.Warn("encode event failed", "aggregate_id", aggregateID, "event_id", raw.ID, "type", raw.Type, "error", err)
			jsonStr = fmt.Sprintf("{\"_error\": %q}", err.Error())
		}
		resp.Events = append(resp.Events, &diagnosticsv1.DiagnosticEvent{
			Id:          raw.ID,
			AggregateId: raw.AggregateID,
			Type:        raw.Type,
			Version:     int32(raw.Version),
			Position:    raw.Position,
			Timestamp:   timestamppb.New(raw.Timestamp),
			DataJson:    jsonStr,
		})
	}
	return connect.NewResponse(resp), nil
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
