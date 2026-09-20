package engine

import (
	"context"

	forward "github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/forwarding"
	"github.com/batchstream/sink/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resultStream adapts execution messages to the private typed stream without
// marshalling or retaining an aggregate response.
type resultStream[T any] struct {
	metrics *metrics.Metrics
	request *forward.ForwardRequest
	grpc.ServerStream
	ctx     context.Context
	store   string
	maximum int
	send    func(*forward.ForwardResponse) error
}

func (s *resultStream[T]) Context() context.Context { return s.ctx }
func (s *resultStream[T]) Send(result *T) error {
	if sized, ok := any(result).(interface{ SizeVT() int }); ok && sized.SizeVT() > s.maximum {
		return status.Error(codes.ResourceExhausted, "result exceeds public message limit")
	}
	frame := &forward.ForwardResponse{Version: forwarding.Version, Store: s.store}
	switch result := any(result).(type) {
	case *sink.ReadResponse:
		frame.Response = &forward.ForwardResponse_Read{Read: result}
	case *sink.WriteResponse:
		frame.Response = &forward.ForwardResponse_Write{Write: result}
	case *sink.DeleteResponse:
		frame.Response = &forward.ForwardResponse_Delete{Delete: result}
	case *sink.ExecuteResponse:
		frame.Response = &forward.ForwardResponse_Execute{Execute: result}
	case *sink.CountResponse:
		frame.Response = &forward.ForwardResponse_Count{Count: result}
	case *sink.QueryResponse:
		frame.Response = &forward.ForwardResponse_Query{Query: result}
	case *sink.ScanResponse:
		frame.Response = &forward.ForwardResponse_Scan{Scan: result}
	default:
		return status.Error(codes.Internal, "invalid streaming result type")
	}
	err := s.send(frame)
	if err == nil {
		s.metrics.ObserveForwardResult(s.request, frame)
	}
	return err
}
