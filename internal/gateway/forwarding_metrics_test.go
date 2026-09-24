package gateway

import (
	"context"
	"io"
	"testing"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	forward "github.com/batchstream/sink/gen/forward"
	"github.com/batchstream/sink/internal/config"
	"github.com/batchstream/sink/internal/forwarding"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type metricsEngineClient struct {
	stream  *metricsEngineStream
	failure error
}

func (c *metricsEngineClient) Forward(context.Context, *forward.ForwardRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[forward.ForwardResponse], error) {
	return c.stream, c.failure
}

type metricsEngineStream struct {
	grpc.ClientStream
	frames   []*forward.ForwardResponse
	failure  error
	trailers metadata.MD
}

func (s *metricsEngineStream) Recv() (*forward.ForwardResponse, error) {
	if len(s.frames) > 0 {
		frame := s.frames[0]
		s.frames = s.frames[1:]
		return frame, nil
	}
	if s.failure != nil {
		return nil, s.failure
	}
	return nil, io.EOF
}

func (s *metricsEngineStream) Trailer() metadata.MD { return s.trailers }

type metricsQueryStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *metricsQueryStream) Context() context.Context       { return s.ctx }
func (s *metricsQueryStream) Send(*sink.QueryResponse) error { return nil }

func TestForwardingMetricsRequireConfirmedEngineError(t *testing.T) {
	detail := &errdetails.ErrorInfo{Domain: "sink", Reason: "ENGINE_CAPACITY_REJECTED"}
	rejection, err := status.New(codes.ResourceExhausted, "capacity exhausted").WithDetails(detail)
	if err != nil {
		t.Fatal(err)
	}
	failure := rejection.Err()
	marker := forwarding.ErrorStatusMarker(failure)
	unavailable := status.Error(codes.Unavailable, "connection refused")
	sendLimit := status.Error(codes.ResourceExhausted, "trying to send message larger than max")
	receiveLimit := status.Error(codes.ResourceExhausted, "received message larger than max")
	withoutDetails := status.Error(codes.ResourceExhausted, "capacity exhausted")
	cases := []struct {
		name          string
		startErr      error
		receiveErr    error
		markers       []string
		wantError     error
		gatewayErrors float64
	}{
		{name: "success"},
		{name: "confirmed Engine rejection", receiveErr: failure, markers: []string{marker}, wantError: failure},
		{name: "confirmed Engine unavailable", receiveErr: unavailable, markers: []string{forwarding.ErrorStatusMarker(unavailable)}, wantError: unavailable},
		{name: "Gateway RPC initiation failed", startErr: unavailable, wantError: unavailable, gatewayErrors: 1},
		{name: "Gateway send limit", startErr: sendLimit, wantError: sendLimit, gatewayErrors: 1},
		{name: "Gateway dial failed during receive", receiveErr: unavailable, wantError: unavailable, gatewayErrors: 1},
		{name: "Gateway receive limit", receiveErr: receiveLimit, wantError: receiveLimit, gatewayErrors: 1},
		{name: "receive failure with different Engine status", receiveErr: receiveLimit, markers: []string{marker}, wantError: receiveLimit, gatewayErrors: 1},
		{name: "different status details", receiveErr: withoutDetails, markers: []string{marker}, wantError: withoutDetails, gatewayErrors: 1},
		{name: "legacy Engine without marker", receiveErr: failure, wantError: failure, gatewayErrors: 1},
		{name: "empty marker", receiveErr: failure, markers: []string{""}, wantError: failure, gatewayErrors: 1},
		{name: "duplicate marker", receiveErr: failure, markers: []string{marker, marker}, wantError: failure, gatewayErrors: 1},
		{name: "malformed marker", receiveErr: failure, markers: []string{"invalid"}, wantError: failure, gatewayErrors: 1},
	}
	for _, method := range []string{"Execute", "Query"} {
		for _, test := range cases {
			t.Run(method+"/"+test.name, func(t *testing.T) {
				trailers := metadata.MD{forwarding.ErrorStatusTrailer: test.markers}
				stream := &metricsEngineStream{failure: test.receiveErr, trailers: trailers}
				if test.wantError == nil {
					stream.frames = metricsTestFrames(method)
				}
				client := &metricsEngineClient{stream: stream, failure: test.startErr}
				gateway := metricsTestGateway(client)
				err := metricsTestCall(t, gateway, method)
				if !proto.Equal(status.Convert(err).Proto(), status.Convert(test.wantError).Proto()) {
					t.Fatalf("public status or details changed: got %v, want %v", err, test.wantError)
				}
				requests, failures := metricsTestCounts(t, gateway)
				var totalFailures float64
				for _, count := range failures {
					totalFailures += count
				}
				code := status.Code(test.wantError).String()
				if requests != 1 || totalFailures != test.gatewayErrors || failures[code] != test.gatewayErrors {
					t.Fatalf("Gateway requests=%v failures=%v, want requests=1 own errors=%v", requests, failures, test.gatewayErrors)
				}
			})
		}
	}
}

func TestForwardingErrorOwnershipDoesNotImplyNonExecution(t *testing.T) {
	failure := status.Error(codes.ResourceExhausted, "Engine failure")
	for _, notStarted := range []bool{false, true} {
		trailers := metadata.Pairs(forwarding.ErrorStatusTrailer, forwarding.ErrorStatusMarker(failure))
		if notStarted {
			trailers.Set(forwarding.NotStartedTrailer, "true")
		}
		stream := &metricsEngineStream{failure: failure, trailers: trailers}
		client := &metricsEngineClient{stream: stream}
		gateway := metricsTestGateway(client)
		command := &sink.Command{Uri: "sink://primary/fixture"}
		execute := &sink.ExecuteRequest{Command: command}
		body := &forward.ForwardRequest_Execute{Execute: execute}
		request := &forward.ForwardRequest{Request: body}
		call := forwardCall{route: gateway.current.routes["primary"], request: request}
		actual, err := gateway.forwardEach(t.Context(), call)
		if actual != notStarted || status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("non-execution evidence changed: notStarted=%t want=%t error=%v", actual, notStarted, err)
		}
	}
}

func metricsTestGateway(client forward.EngineClient) *Server {
	route := Route{Store: "primary", Target: "passthrough:///metrics-test", endpoint: "metrics-test"}
	entry := &connection{client: client}
	current := &snapshot{routes: map[string]Route{"primary": route}}
	request := config.Request{MaxOperations: 10, MaxReadBytes: 1 << 20}
	gateway := &Server{metrics: newMetrics(), current: current, request: request}
	gateway.pool.entries = map[Route]*connection{route: entry}
	gateway.pool.maximum = 1
	return gateway
}

func metricsTestFrames(method string) []*forward.ForwardResponse {
	if method == "Execute" {
		reply := &sink.ExecuteResponse{Success: true}
		body := &forward.ForwardResponse_Execute{Execute: reply}
		frame := &forward.ForwardResponse{Version: forwarding.Version, Store: "primary", Response: body}
		frames := []*forward.ForwardResponse{frame}
		return frames
	}
	reply := &sink.QueryResponse{Complete: true}
	body := &forward.ForwardResponse_Query{Query: reply}
	frame := &forward.ForwardResponse{Version: forwarding.Version, Store: "primary", Response: body}
	frames := []*forward.ForwardResponse{frame}
	return frames
}

func metricsTestCall(t *testing.T, gateway *Server, method string) error {
	t.Helper()
	command := &sink.Command{Uri: "sink://primary/fixture"}
	if method == "Execute" {
		request := &sink.ExecuteRequest{Command: command}
		info := &grpc.UnaryServerInfo{FullMethod: sink.Sink_Execute_FullMethodName}
		handler := func(ctx context.Context, _ any) (any, error) {
			return gateway.Execute(ctx, request)
		}
		_, err := gateway.UnaryInterceptor()(t.Context(), request, info, handler)
		return err
	}
	request := &sink.QueryRequest{Command: command}
	info := &grpc.StreamServerInfo{FullMethod: sink.Sink_Query_FullMethodName, IsServerStream: true}
	stream := &metricsQueryStream{ctx: t.Context()}
	handler := func(any, grpc.ServerStream) error {
		return gateway.Query(request, stream)
	}
	return gateway.StreamInterceptor()(gateway, stream, info, handler)
}

func metricsTestCounts(t *testing.T, gateway *Server) (float64, map[string]float64) {
	t.Helper()
	families, err := gateway.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var requests float64
	failures := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			switch family.GetName() {
			case "sink_gateway_requests_total":
				requests += metric.GetCounter().GetValue()
			case "sink_gateway_errors_total":
				for _, label := range metric.GetLabel() {
					if label.GetName() == "code" {
						failures[label.GetValue()] += metric.GetCounter().GetValue()
					}
				}
			}
		}
	}
	return requests, failures
}
