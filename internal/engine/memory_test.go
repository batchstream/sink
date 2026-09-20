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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type pressureService struct {
	sink.UnimplementedSinkServer
	calls int
}

func (s *pressureService) Write(_ *sink.WriteRequest, stream grpc.ServerStreamingServer[sink.WriteResponse]) error {
	s.calls++
	response := &sink.WriteResponse{}
	return stream.Send(response)
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
	stream := &forwardRecorder{ctx: t.Context()}
	err = server.Forward(request, stream)
	response := stream.response
	if err != nil || !response.GetNotStarted() || response.GetCode() != uint32(codes.ResourceExhausted) || backend.calls != 0 {
		t.Fatalf("pressure reached service: %v %v calls=%d", response, err, backend.calls)
	}
	if response.GetUsed().GetReturns() != 0 {
		t.Fatal("rejection consumed response allowance")
	}
}

type forwardRecorder struct {
	grpc.ServerStream
	ctx      context.Context
	response *forward.ForwardResponse
}

func (s *forwardRecorder) Context() context.Context { return s.ctx }
func (s *forwardRecorder) Send(response *forward.ForwardResponse) error {
	s.response = response
	return nil
}
