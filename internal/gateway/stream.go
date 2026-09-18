package gateway

import (
	"context"
	"io"

	forward "github.com/liran/sink/gen/forward"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) forwardStream(ctx context.Context, entry *connection, req *forward.ForwardRequest) (*forward.ForwardResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	scope := capacity.FromContext(ctx)
	scratch := scope.NewLease()
	defer capacity.Close(scratch)
	if scratch != nil {
		if err := scratch.Grow(ctx, forwarding.StreamScratchBytes, capacity.Request); err != nil {
			// No request has left this process. Preserve the zero usage settlement
			// and safe-retry signal instead of reporting an ambiguous mutation.
			marked := protocol.MemoryAdmissionError(req, err)
			route := Route{Store: req.GetStore()}
			rejected := localRejection(route, status.Code(marked), status.Convert(marked).Message())
			rejected.StatusDetails = status.Convert(marked).Proto().GetDetails()
			return rejected, nil
		}
	}
	stream, err := entry.client.ForwardStream(ctx, req, grpc.MaxCallRecvMsgSize(forwarding.FrameMessageBytes))
	if err != nil {
		return nil, err
	}
	header, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if header.GetSize() == 0 || header.GetSize() > uint64(s.pool.messageBytes) || len(header.GetData()) != 0 {
		return nil, status.Error(codes.Internal, "invalid Engine response size frame")
	}
	size := int(header.GetSize())
	// The receiver stops reading the stream until capacity exists. A bounded
	// frame receive limit and static HTTP/2 window also bound dishonest peers.
	if err := scope.Output(ctx, size+1024); err != nil {
		return nil, err
	}
	encoded := make([]byte, size)
	offset := 0
	for offset < size {
		frame, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if frame.GetSize() != 0 || len(frame.GetData()) == 0 || len(frame.GetData()) > min(forwarding.FrameBytes, size-offset) {
			return nil, status.Error(codes.Internal, "invalid Engine response data frame")
		}
		offset += copy(encoded[offset:], frame.GetData())
	}
	if _, err := stream.Recv(); err != io.EOF {
		if err != nil {
			return nil, err
		}
		return nil, status.Error(codes.Internal, "unexpected Engine response frame")
	}
	response := &forward.ForwardResponse{}
	if err := response.UnmarshalVT(encoded); err != nil {
		return nil, status.Error(codes.Internal, "invalid encoded Engine response")
	}
	return response, nil
}
