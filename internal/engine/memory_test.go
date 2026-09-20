package engine

import (
	"context"
	"os"
	"testing"
	"testing/fstest"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/forwarding"
	"google.golang.org/grpc/codes"
)

type pressureService struct {
	sink.UnimplementedSinkServer
	calls int
}

func (s *pressureService) Write(context.Context, *sink.WriteRequest) (*sink.WriteResponse, error) {
	s.calls++
	response := &sink.WriteResponse{}
	return response, nil
}

func TestEnginePressureRejectsBeforeExecution(t *testing.T) {
	files := fstest.MapFS{"proc/self/statm": {Data: []byte("100 90")}}
	memoryOpts := capacity.Options{Bytes: int64(100 * os.Getpagesize()), HighPercent: 80, LowPercent: 70, Files: files}
	guard, err := capacity.New(memoryOpts)
	if err != nil {
		t.Fatal(err)
	}
	backend := &pressureService{}
	opts := Options{Memory: guard, Service: backend, Store: "primary", MaxReadBytes: 1 << 20}
	server, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	write := &sink.WriteRequest{}
	body := &forward.ForwardRequest_Write{Write: write}
	request := &forward.ForwardRequest{Version: forwarding.Version, Store: "primary", Grant: forwarding.FullBudget(1 << 20), Request: body}
	response, err := server.Forward(t.Context(), request)
	if err != nil || !response.GetNotStarted() || response.GetCode() != uint32(codes.ResourceExhausted) || backend.calls != 0 {
		t.Fatalf("pressure reached service: %v %v calls=%d", response, err, backend.calls)
	}
	if response.GetUsed().GetReturns() != 0 {
		t.Fatal("rejection consumed response allowance")
	}
}
