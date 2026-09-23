package gateway

import (
	"context"

	"google.golang.org/grpc"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	forward "github.com/batchstream/sink/gen/forward"
	"github.com/batchstream/sink/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Read(req *sink.ReadRequest, stream grpc.ServerStreamingServer[sink.ReadResponse]) error {
	body := &forward.ForwardRequest_Read{Read: req}
	request := &forward.ForwardRequest{Request: body}
	return s.streamRecords(stream.Context(), request, func(frame *forward.ForwardResponse) error { return stream.Send(frame.GetRead()) })
}

func (s *Server) Write(req *sink.WriteRequest, stream grpc.ServerStreamingServer[sink.WriteResponse]) error {
	body := &forward.ForwardRequest_Write{Write: req}
	request := &forward.ForwardRequest{Request: body}
	return s.streamRecords(stream.Context(), request, func(frame *forward.ForwardResponse) error { return stream.Send(frame.GetWrite()) })
}

func (s *Server) Delete(ctx context.Context, req *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	body := &forward.ForwardRequest_Delete{Delete: req}
	request := &forward.ForwardRequest{Request: body}
	response, err := s.deleteRecords(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.GetDelete(), nil
}

func (s *Server) Execute(ctx context.Context, req *sink.ExecuteRequest) (*sink.ExecuteResponse, error) {
	if req == nil || req.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	body := &forward.ForwardRequest_Execute{Execute: req}
	request := &forward.ForwardRequest{Request: body}
	ctx, release, err := s.begin(ctx, request)
	if err != nil {
		return nil, err
	}
	defer release()
	route, err := routeFor(s.current, protocol.CommandStore(req.GetCommand()))
	if err != nil {
		return nil, err
	}
	response, _, err := s.forward(ctx, route, request)
	if err != nil {
		return nil, err
	}
	if response.GetExecute() == nil {
		return nil, status.Error(codes.Internal, "Engine returned an invalid response type")
	}
	return response.GetExecute(), nil
}

func (s *Server) Query(req *sink.QueryRequest, stream grpc.ServerStreamingServer[sink.QueryResponse]) error {
	if req.GetCommand() == nil {
		return status.Error(codes.InvalidArgument, "command is required")
	}
	body := &forward.ForwardRequest_Query{Query: req}
	request := &forward.ForwardRequest{Request: body}
	ctx, release, err := s.begin(stream.Context(), request)
	if err != nil {
		return err
	}
	defer release()
	route, err := routeFor(s.current, protocol.CommandStore(req.GetCommand()))
	if err != nil {
		return err
	}
	complete := false
	call := forwardCall{route: route, request: request}
	call.emit = func(frame *forward.ForwardResponse) error {
		result := frame.GetQuery()
		if result == nil || complete || len(result.GetDocuments()) > 1 {
			return status.Error(codes.Internal, "invalid Query stream frame")
		}
		if result.GetComplete() {
			if len(result.GetDocuments()) != 0 {
				return status.Error(codes.Internal, "invalid Query completion frame")
			}
			complete = true
		} else if len(result.GetDocuments()) != 1 {
			return status.Error(codes.Internal, "empty Query document frame")
		}
		return stream.Send(result)
	}
	_, err = s.forwardEach(ctx, call)
	if err != nil {
		return err
	}
	if !complete {
		return status.Error(codes.Internal, "Engine omitted Query completion")
	}
	return nil
}

func (s *Server) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	if req == nil || req.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	body := &forward.ForwardRequest_Count{Count: req}
	request := &forward.ForwardRequest{Request: body}
	ctx, release, err := s.begin(ctx, request)
	if err != nil {
		return nil, err
	}
	defer release()
	route, err := routeFor(s.current, protocol.CommandStore(req.GetCommand()))
	if err != nil {
		return nil, err
	}
	response, _, err := s.forward(ctx, route, request)
	if err != nil {
		return nil, err
	}
	if response.GetCount() == nil {
		return nil, status.Error(codes.Internal, "Engine returned an invalid response type")
	}
	return response.GetCount(), nil
}

func (s *Server) Scan(req *sink.ScanRequest, stream grpc.ServerStreamingServer[sink.ScanResponse]) error {
	if req.GetCommand() == nil {
		return status.Error(codes.InvalidArgument, "command is required")
	}
	body := &forward.ForwardRequest_Scan{Scan: req}
	request := &forward.ForwardRequest{Request: body}
	ctx, release, err := s.begin(stream.Context(), request)
	if err != nil {
		return err
	}
	defer release()
	route, err := routeFor(s.current, protocol.CommandStore(req.GetCommand()))
	if err != nil {
		return err
	}
	complete := false
	call := forwardCall{route: route, request: request}
	call.emit = func(frame *forward.ForwardResponse) error {
		result := frame.GetScan()
		if result == nil || complete || len(result.GetDocuments()) > 1 {
			return status.Error(codes.Internal, "invalid Scan stream frame")
		}
		if result.GetComplete() {
			if len(result.GetDocuments()) != 0 {
				return status.Error(codes.Internal, "invalid Scan completion frame")
			}
			complete = true
		} else if len(result.GetDocuments()) != 1 {
			return status.Error(codes.Internal, "empty Scan document frame")
		}
		return stream.Send(result)
	}
	_, err = s.forwardEach(ctx, call)
	if err != nil {
		return err
	}
	if !complete {
		return status.Error(codes.Internal, "Engine omitted Scan completion")
	}
	return nil
}
