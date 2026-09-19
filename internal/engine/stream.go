package engine

import (
	forward "github.com/liran/sink/gen/forward"
	"github.com/liran/sink/internal/forwarding"
	"google.golang.org/grpc"
)

func (s *Server) ForwardStream(req *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ResponseFrame]) error {
	ctx := stream.Context()
	response, err := s.Forward(ctx, req)
	if err != nil {
		return err
	}
	size := response.SizeVT()
	frame := &forward.ResponseFrame{Size: uint64(size)}
	if err := stream.SendMsg(frame); err != nil {
		return err
	}
	encoded, err := response.MarshalVT()
	if err != nil {
		return err
	}
	for offset := 0; offset < len(encoded); offset += forwarding.FrameBytes {
		frame := &forward.ResponseFrame{Data: encoded[offset:min(offset+forwarding.FrameBytes, len(encoded))]}
		if err := stream.SendMsg(frame); err != nil {
			return err
		}
	}
	return nil
}
