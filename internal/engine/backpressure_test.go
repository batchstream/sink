package engine

import (
	"context"
	"testing"

	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/backpressure"
	"github.com/batchstream/sink/internal/forwarding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type admissionFailureService struct {
	sink.UnimplementedSinkServer
	err error
}

func (s *admissionFailureService) Execute(context.Context, *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	return nil, s.err
}

func TestEngineDistinguishesAdmissionFromUnknownNativeOutcome(t *testing.T) {
	for _, failure := range []error{backpressure.ErrBusy, status.Error(codes.ResourceExhausted, "backend outcome unknown")} {
		service := &admissionFailureService{err: failure}
		opts := Options{Service: service, Store: "primary", MaxReadBytes: 1 << 20}
		server, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		command := &sink.Command{Uri: "sink://primary", Method: "POST", Path: "/_bulk"}
		execute := &sink.ExecuteRequest{Command: command}
		body := &forward.ForwardRequest_Execute{Execute: execute}
		request := &forward.ForwardRequest{Version: forwarding.Version, Store: "primary", Request: body}
		stream := &forwardRecorder{ctx: t.Context()}
		err = server.Forward(request, stream)
		marker := stream.trailer.Get(forwarding.NotStartedTrailer)
		if err != failure || (len(marker) == 1) != (failure == backpressure.ErrBusy) || stream.response != nil {
			t.Fatalf("unknown outcome or admission evidence changed: error=%v marker=%v", err, marker)
		}
	}
}
