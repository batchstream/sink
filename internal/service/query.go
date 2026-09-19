package service

import (
	"context"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/forwarding"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxCountResponseBytes = 256 << 10

func (s *Server) Query(ctx context.Context, req *sink.QueryRequest) (*sink.QueryResponse, error) {
	maximum := forwarding.FromContext(ctx).Limit(forwarding.Returns, s.maxReadBytes)
	if maximum <= 0 {
		return nil, status.Error(codes.ResourceExhausted, "native response budget is exhausted")
	}
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	request, err := nativeRequest(req.GetCommand(), maximum)
	if err != nil {
		return nil, err
	}
	pageSize := int(req.GetPageSize())
	if pageSize == 0 {
		pageSize = 100
	}
	if pageSize > 1000 {
		return nil, status.Error(codes.InvalidArgument, "query page size exceeds 1000")
	}
	page := max(req.GetPage(), 1)
	query := storage.QueryRequest{Request: request, Offset: int64(page-1) * int64(pageSize), PageSize: pageSize}
	for _, field := range req.GetSort() {
		item := storage.SortField{Field: field.GetField(), Descending: field.GetDescending()}
		query.Sort = append(query.Sort, item)
	}
	if projection := req.GetProjection(); projection != nil {
		query.Projection = &storage.Projection{Fields: projection.GetFields(), Exclude: projection.GetExclude()}
	}
	if err := query.Validate(); err != nil {
		return nil, nativeStatus(err)
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	encodedBytes := nativeExecutionBytes(req.GetCommand(), request) + req.SizeVT() - req.GetCommand().SizeVT()
	admission := admissionRequest{encodedBytes: encodedBytes, stores: []string{protocol.CommandStore(req.GetCommand())}}
	admission.inputBytes = req.SizeVT()
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()
	result, err := backend.Query(ctx, query)
	if err != nil {
		return nil, nativeStatus(err)
	}
	if len(result.Documents) > pageSize || (result.HasMore && len(result.Documents) != pageSize) {
		return nil, status.Error(codes.Internal, "backend returned an invalid query page")
	}
	response := &sink.QueryResponse{HasMore: result.HasMore}
	for _, document := range result.Documents {
		encoded := &sink.Document{Encoding: sink.DocumentEncoding(document.Encoding), Payload: document.Payload}
		response.Documents = append(response.Documents, encoded)
	}
	if response.SizeVT() > maximum {
		return nil, status.Error(codes.ResourceExhausted, "query response exceeds byte limit")
	}
	return response, nil
}

func (s *Server) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	maximum := forwarding.FromContext(ctx).Limit(forwarding.Returns, s.maxReadBytes)
	if maximum <= 0 {
		return nil, status.Error(codes.ResourceExhausted, "native response budget is exhausted")
	}
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	// Count retains only totals and bounded backend metadata. Enforce the
	// smaller buffer in the adapter as well as reserving it at admission.
	request, err := nativeRequest(req.GetCommand(), min(maximum, maxCountResponseBytes))
	if err != nil {
		return nil, err
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	admission := admissionRequest{encodedBytes: nativeExecutionBytes(req.GetCommand(), request), stores: []string{protocol.CommandStore(req.GetCommand())}}
	admission.inputBytes = req.SizeVT()
	ctx, release, err := s.admitRequest(ctx, admission)
	if err != nil {
		return nil, err
	}
	defer release()
	countRequest := storage.CountRequest{Request: request}
	count, err := backend.Count(ctx, countRequest)
	if err != nil {
		return nil, nativeStatus(err)
	}
	response := &sink.CountResponse{Count: count.Count, Estimated: count.Estimated}
	return response, nil
}

func (s *BatchingServer) Query(ctx context.Context, req *sink.QueryRequest) (*sink.QueryResponse, error) {
	return s.server.Query(ctx, req)
}

func (s *BatchingServer) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	return s.server.Count(ctx, req)
}
