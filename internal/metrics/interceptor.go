package metrics

import (
	"context"
	"strings"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ConnectInterceptor returns a connectrpc.UnaryInterceptorFunc that
// records duration + error counts for every RPC handler. The "procedure"
// attribute keeps service+method together (e.g. "/xray.orderbook.v1.
// OrderBookService/PlaceOrder") so dashboards can filter without
// reconstructing the path.
func ConnectInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			resp, err := next(ctx, req)

			procedure := req.Spec().Procedure
			service, method := splitProcedure(procedure)
			attrs := metric.WithAttributes(
				attribute.String("service", service),
				attribute.String("method", method),
			)

			if RPCDurationSeconds != nil {
				RPCDurationSeconds.Record(ctx, time.Since(start).Seconds(), attrs)
			}
			if err != nil && RPCErrorsTotal != nil {
				code := connect.CodeOf(err).String()
				RPCErrorsTotal.Add(ctx, 1, metric.WithAttributes(
					attribute.String("service", service),
					attribute.String("method", method),
					attribute.String("code", code),
				))
			}
			return resp, err
		}
	}
}

// splitProcedure parses Connect's "/pkg.Service/Method" form. Returns
// ("unknown", procedure) when the format doesn't match so we still get
// a usable label.
func splitProcedure(procedure string) (service, method string) {
	p := strings.TrimPrefix(procedure, "/")
	if i := strings.LastIndexByte(p, '/'); i > 0 {
		return p[:i], p[i+1:]
	}
	return "unknown", procedure
}
