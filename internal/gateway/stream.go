package gateway

import (
	"context"
	"io"
	"time"

	forward "github.com/batchstream/sink/gen/forward"
	"github.com/batchstream/sink/internal/forwarding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type forwardCall struct {
	route   Route
	request *forward.ForwardRequest
	emit    func(*forward.ForwardResponse) error
}

// forwardEach reads the next result only after the public send completes. The
// transport therefore supplies backpressure without an intermediate queue.
func (s *Server) forwardEach(ctx context.Context, call forwardCall) (bool, error) {
	route, req := call.route, call.request
	if route.endpoint == "" {
		targets, release, err := s.pool.destinations(ctx, route)
		if err != nil {
			return true, err
		}
		defer release()
		route = targets[(s.nativeSequence.Add(1)-1)%uint64(len(targets))]
	}
	req.Version, req.Store = forwarding.Version, route.Store
	entry, err := s.pool.acquire(route)
	if err != nil {
		return true, err
	}
	defer s.pool.release(entry)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := time.Now()
	defer func() { s.metrics.downstream.Observe(time.Since(started).Seconds()) }()
	stream, err := entry.client.Forward(ctx, req)
	if err != nil {
		return false, err
	}
	received := false
	for {
		frame, err := stream.Recv()
		if err != nil {
			marker := stream.Trailer().Get(forwarding.NotStartedTrailer)
			notStarted := len(marker) == 1 && marker[0] == "true"
			if len(marker) > 0 && (!notStarted || received || err == io.EOF) {
				return false, status.Error(codes.Internal, "invalid Engine rejection marker")
			}
			if err == io.EOF {
				return false, nil
			}
			return notStarted, err
		}
		if frame.GetVersion() != forwarding.Version || frame.GetStore() != route.Store {
			return false, status.Error(codes.Internal, "invalid Engine stream identity")
		}
		size := responseSize(req, frame)
		if size < 0 {
			return false, status.Error(codes.Internal, "invalid Engine result type")
		}
		if size > s.request.MaxReadBytes {
			return false, status.Error(codes.ResourceExhausted, "Engine result exceeds public message limit")
		}
		received = true
		if err := call.emit(frame); err != nil {
			return false, err
		}
	}
}

func responseSize(req *forward.ForwardRequest, frame *forward.ForwardResponse) int {
	switch body := frame.GetResponse().(type) {
	case *forward.ForwardResponse_Read:
		if req.GetRead() != nil && body.Read != nil {
			return body.Read.SizeVT()
		}
	case *forward.ForwardResponse_Write:
		if req.GetWrite() != nil && body.Write != nil {
			return body.Write.SizeVT()
		}
	case *forward.ForwardResponse_Delete:
		if req.GetDelete() != nil && body.Delete != nil {
			return body.Delete.SizeVT()
		}
	case *forward.ForwardResponse_Execute:
		if req.GetExecute() != nil && body.Execute != nil {
			return body.Execute.SizeVT()
		}
	case *forward.ForwardResponse_Query:
		if req.GetQuery() != nil && body.Query != nil {
			return body.Query.SizeVT()
		}
	case *forward.ForwardResponse_Count:
		if req.GetCount() != nil && body.Count != nil {
			return body.Count.SizeVT()
		}
	case *forward.ForwardResponse_Scan:
		if req.GetScan() != nil && body.Scan != nil {
			return body.Scan.SizeVT()
		}
	}
	return -1
}
