package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
)

func TestForwardedRPCUsesPublicMethodMetrics(t *testing.T) {
	observed, err := New("test", "a")
	if err != nil {
		t.Fatal(err)
	}
	address := &sink.RecordAddress{Store: "a"}
	operation := &sink.WriteOperation{Address: address}
	write := &sink.WriteRequest{Operations: []*sink.WriteOperation{operation}}
	body := &forward.ForwardRequest_Write{Write: write}
	request := &forward.ForwardRequest{Request: body}
	applied := &sink.WriteResult{Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
	result := &sink.WriteResponse{Results: []*sink.WriteResult{applied}}
	responseBody := &forward.ForwardResponse_Write{Write: result}
	response := &forward.ForwardResponse{Response: responseBody}
	observed.ObserveForward(request, response, time.Millisecond)
	recorder := httptest.NewRecorder()
	httpRequest := httptest.NewRequest("GET", "/metrics", nil)
	observed.Handler().ServeHTTP(recorder, httpRequest)
	if !strings.Contains(recorder.Body.String(), `sink_grpc_server_requests_total{code="OK",method="Write",store="a"} 1`) {
		t.Fatal("forwarded Write missing public method metric")
	}
}
