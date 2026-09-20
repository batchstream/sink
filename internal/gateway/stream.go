package gateway

import (
	"context"
	"io"
	"time"

	forward "github.com/liran/sink/gen/forward"
	"github.com/liran/sink/internal/forwarding"
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
func (s *Server) forwardEach(ctx context.Context, call forwardCall) (*forward.ForwardResponse, error) {
	route, req := call.route, call.request
	if route.endpoint == "" {
		targets, release, err := s.pool.destinations(ctx, route)
		if err != nil {
			return localRejection(route, status.Code(err), status.Convert(err).Message()), nil
		}
		defer release()
		route = targets[(s.nativeSequence.Add(1)-1)%uint64(len(targets))]
	}
	req.Version, req.Store = forwarding.Version, route.Store
	entry, err := s.pool.acquire(route)
	if err != nil {
		return localRejection(route, status.Code(err), status.Convert(err).Message()), nil
	}
	defer s.pool.release(entry)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	started := time.Now()
	defer func() { s.metrics.downstream.Observe(time.Since(started).Seconds()) }()
	stream, err := entry.client.Forward(ctx, req)
	if err != nil {
		return nil, err
	}
	var final *forward.ForwardResponse
	received := false
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			if final == nil {
				return nil, status.Error(codes.Internal, "Engine omitted stream settlement")
			}
			return final, nil
		}
		if err != nil {
			return nil, err
		}
		if final != nil || frame.GetVersion() != forwarding.Version || frame.GetStore() != route.Store {
			return nil, status.Error(codes.Internal, "invalid Engine stream identity or frame order")
		}
		if frame.GetComplete() {
			if frame.GetResponse() != nil || frame.GetUsed() == nil || !validUsage(req.GetGrant(), frame.GetUsed()) || frame.GetCode() > uint32(codes.Unauthenticated) || (received && frame.GetNotStarted()) {
				return nil, status.Error(codes.Internal, "invalid Engine stream settlement")
			}
			final = frame
			continue
		}
		if frame.GetResponse() == nil || frame.GetUsed() != nil || frame.GetCode() != 0 || frame.GetNotStarted() || frame.GetMessage() != "" || len(frame.GetStatusDetails()) != 0 {
			return nil, status.Error(codes.Internal, "invalid Engine result frame")
		}
		received = true
		if err := call.emit(frame); err != nil {
			return nil, err
		}
	}
}
