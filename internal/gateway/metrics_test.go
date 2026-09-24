package gateway

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGatewayMetricsExcludeDownstreamFailures(t *testing.T) {
	server := &Server{metrics: newMetrics()}
	interceptor := server.UnaryInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/sink.v1.Sink/Count"}
	downstreamErr := status.Error(codes.ResourceExhausted, "Engine admission rejected request")
	markedDownstreamErr := &downstreamError{cause: downstreamErr}
	downstreamHandler := func(context.Context, any) (any, error) {
		return nil, markedDownstreamErr
	}
	_, err := interceptor(context.Background(), nil, info, downstreamHandler)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("downstream status = %s, want ResourceExhausted", status.Code(err))
	}

	gatewayErr := status.Error(codes.ResourceExhausted, "Gateway capacity exhausted")
	gatewayHandler := func(context.Context, any) (any, error) {
		return nil, gatewayErr
	}
	_, err = interceptor(context.Background(), nil, info, gatewayHandler)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Gateway status = %s, want ResourceExhausted", status.Code(err))
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/metrics", nil)
	server.MetricsHandler().ServeHTTP(recorder, request)
	body := recorder.Body.String()
	if !strings.Contains(body, `sink_gateway_requests_total{method="Count"} 2`) {
		t.Fatalf("request counter missing: %s", body)
	}
	if !strings.Contains(body, `sink_gateway_errors_total{code="ResourceExhausted",method="Count"} 1`) {
		t.Fatalf("Gateway-owned error counter missing: %s", body)
	}
	if strings.Contains(body, `sink_gateway_errors_total{code="ResourceExhausted",method="Count"} 2`) {
		t.Fatalf("downstream failure was attributed to Gateway: %s", body)
	}
}
