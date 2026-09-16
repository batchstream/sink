package gateway

import (
	"context"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Read(ctx context.Context, req *sink.ReadRequest) (*sink.ReadResponse, error) {
	body := &forward.ForwardRequest_Read{Read: req}
	request := &forward.ForwardRequest{Request: body}
	response, err := s.records(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.GetRead(), nil
}

func (s *Server) Write(ctx context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	body := &forward.ForwardRequest_Write{Write: req}
	request := &forward.ForwardRequest{Request: body}
	response, err := s.records(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.GetWrite(), nil
}

func (s *Server) Delete(ctx context.Context, req *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	body := &forward.ForwardRequest_Delete{Delete: req}
	request := &forward.ForwardRequest{Request: body}
	response, err := s.records(ctx, request)
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
	request := &forward.ForwardRequest{Request: body, Grant: forwarding.FullBudget(s.request.MaxReadBytes)}
	ctx, release, err := s.begin(ctx, request)
	if err != nil {
		return nil, err
	}
	defer release()
	route, err := routeFor(s.current.Load(), protocol.CommandStore(req.GetCommand()))
	if err != nil {
		return nil, err
	}
	response, err := s.forward(ctx, route, request)
	if err != nil {
		return nil, err
	}
	if response.GetCode() != 0 {
		return nil, forwardedError(response)
	}
	if response.GetExecute() == nil {
		return nil, status.Error(codes.Internal, "Engine returned an invalid response type")
	}
	return response.GetExecute(), nil
}

func (s *Server) Query(ctx context.Context, req *sink.QueryRequest) (*sink.QueryResponse, error) {
	if req == nil || req.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	body := &forward.ForwardRequest_Query{Query: req}
	request := &forward.ForwardRequest{Request: body, Grant: forwarding.FullBudget(s.request.MaxReadBytes)}
	ctx, release, err := s.begin(ctx, request)
	if err != nil {
		return nil, err
	}
	defer release()
	route, err := routeFor(s.current.Load(), protocol.CommandStore(req.GetCommand()))
	if err != nil {
		return nil, err
	}
	response, err := s.forward(ctx, route, request)
	if err != nil {
		return nil, err
	}
	if response.GetCode() != 0 {
		return nil, forwardedError(response)
	}
	if response.GetQuery() == nil {
		return nil, status.Error(codes.Internal, "Engine returned an invalid response type")
	}
	return response.GetQuery(), nil
}

func (s *Server) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	if req == nil || req.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	body := &forward.ForwardRequest_Count{Count: req}
	request := &forward.ForwardRequest{Request: body, Grant: forwarding.FullBudget(s.request.MaxReadBytes)}
	ctx, release, err := s.begin(ctx, request)
	if err != nil {
		return nil, err
	}
	defer release()
	route, err := routeFor(s.current.Load(), protocol.CommandStore(req.GetCommand()))
	if err != nil {
		return nil, err
	}
	response, err := s.forward(ctx, route, request)
	if err != nil {
		return nil, err
	}
	if response.GetCode() != 0 {
		return nil, forwardedError(response)
	}
	if response.GetCount() == nil {
		return nil, status.Error(codes.Internal, "Engine returned an invalid response type")
	}
	return response.GetCount(), nil
}

func (s *Server) Scan(ctx context.Context, req *sink.ScanRequest) (*sink.ScanResponse, error) {
	if req == nil || req.GetCommand() == nil {
		return nil, status.Error(codes.InvalidArgument, "command is required")
	}
	body := &forward.ForwardRequest_Scan{Scan: req}
	request := &forward.ForwardRequest{Request: body, Grant: forwarding.FullBudget(s.request.MaxReadBytes)}
	ctx, release, err := s.begin(ctx, request)
	if err != nil {
		return nil, err
	}
	defer release()
	route, err := routeFor(s.current.Load(), protocol.CommandStore(req.GetCommand()))
	if err != nil {
		return nil, err
	}
	response, err := s.forward(ctx, route, request)
	if err != nil {
		return nil, err
	}
	if response.GetCode() != 0 {
		return nil, forwardedError(response)
	}
	if response.GetScan() == nil {
		return nil, status.Error(codes.Internal, "Engine returned an invalid response type")
	}
	return response.GetScan(), nil
}
