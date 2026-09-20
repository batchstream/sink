package engine

import (
	"context"
	"os"
	"testing"
	"testing/fstest"

	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/capacity"
	"github.com/batchstream/sink/internal/forwarding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	request := &forward.ForwardRequest{Version: forwarding.Version, Store: "primary", Request: body}
	stream := &forwardRecorder{ctx: t.Context()}
	err = server.Forward(request, stream)
	response := stream.response
	if status.Code(err) != codes.ResourceExhausted || response != nil || backend.calls != 0 {
		t.Fatalf("pressure reached service: %v %v calls=%d", response, err, backend.calls)
	}
	if marker := stream.trailer.Get(forwarding.NotStartedTrailer); len(marker) != 1 || marker[0] != "true" {
		t.Fatal("rejection omitted not-started evidence")
	}
}

type forwardRecorder struct {
	grpc.ServerStream
	ctx      context.Context
	response *forward.ForwardResponse
	trailer  metadata.MD
}

func (s *forwardRecorder) Context() context.Context { return s.ctx }
func (s *forwardRecorder) Send(response *forward.ForwardResponse) error {
	s.response = response
	return nil
}

func (s *forwardRecorder) SetTrailer(trailer metadata.MD) {
	s.trailer = metadata.Join(s.trailer, trailer)
}
