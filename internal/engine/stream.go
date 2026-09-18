package engine

import (
	forward "github.com/liran/sink/gen/forward"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/grpc"
)

func (s *Server) ForwardStream(req *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ResponseFrame]) error {
	ctx := stream.Context()
	scope := capacity.FromContext(ctx)
	if err := scope.Admit(ctx, 2*req.SizeVT()+1024); err != nil {
		return protocol.MemoryAdmissionError(req, err)
	}
	if err := scope.AdmitCompletion(ctx, protocol.CompletionBytes(req)); err != nil {
		return protocol.MemoryAdmissionError(req, err)
	}
	scratch := scope.NewLease()
	if scratch != nil {
		if err := scratch.Grow(ctx, forwarding.StreamScratchBytes, capacity.Request); err != nil {
			return protocol.MemoryAdmissionError(req, err)
		}
	}
	response, err := s.Forward(ctx, req)
	if err != nil {
		return err
	}
	size := response.SizeVT()
	if err := scope.EnsureOutput(ctx, size+1024); err != nil {
		return err
	}
	frame := &forward.ResponseFrame{Size: uint64(size)}
	var header any = frame
	if scope != nil {
		header = &protocol.ManagedMessage{Message: frame, Context: ctx}
	}
	if err := stream.SendMsg(header); err != nil {
		return err
	}
	encoded, err := response.MarshalVT()
	if err != nil {
		return err
	}
	for offset := 0; offset < len(encoded); offset += forwarding.FrameBytes {
		frame := &forward.ResponseFrame{Data: encoded[offset:min(offset+forwarding.FrameBytes, len(encoded))]}
		var message any = frame
		if scope != nil {
			message = &protocol.ManagedMessage{Message: frame, Context: ctx}
		}
		if err := stream.SendMsg(message); err != nil {
			return err
		}
	}
	return nil
}
