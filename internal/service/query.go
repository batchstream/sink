package service

import (
	"context"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/protocol"
	"github.com/batchstream/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxCountResponseBytes = 256 << 10

func (s *Server) query(ctx context.Context, req *sink.QueryRequest, send func(*sink.QueryResponse) error) error {
	maximum := s.maxReadBytes
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return err
	}
	request, err := nativeRequest(req.GetCommand(), maximum)
	if err != nil {
		return err
	}
	pageSize := int(req.GetPageSize())
	if pageSize == 0 {
		pageSize = 100
	}
	if pageSize > 1000 {
		return status.Error(codes.InvalidArgument, "query page size exceeds 1000")
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
		return nativeStatus(err)
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nativeStatus(storage.ErrNativeUnsupported)
	}
	count := 0
	query.Emit = func(document storage.Document) error {
		count++
		if count > pageSize {
			return status.Error(codes.Internal, "backend exceeded query page size")
		}
		encoded := &sink.Document{Encoding: sink.DocumentEncoding(document.Encoding), Payload: document.Payload}
		frame := &sink.QueryResponse{Documents: []*sink.Document{encoded}}
		if frame.SizeVT() > maximum {
			return status.Error(codes.ResourceExhausted, "query document exceeds message limit")
		}
		return send(frame)
	}
	ctx, permit, err := s.admission.Admit(ctx)
	if err != nil {
		return err
	}
	defer permit.Release()
	result, err := backend.Query(ctx, query)
	if err != nil {
		return nativeStatus(err)
	}
	if len(result.Documents) != 0 || (result.HasMore && count != pageSize) {
		return status.Error(codes.Internal, "backend returned an invalid streaming query page")
	}
	final := &sink.QueryResponse{Complete: true, HasMore: result.HasMore}
	return send(final)
}

func (s *Server) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	maximum := s.maxReadBytes
	if err := protocol.CheckStore(req, s.boundStore); err != nil {
		return nil, err
	}
	// Count retains only totals and bounded backend metadata. Enforce the
	// smaller buffer in the adapter for this bounded scalar response.
	request, err := nativeRequest(req.GetCommand(), min(maximum, maxCountResponseBytes))
	if err != nil {
		return nil, err
	}
	backend, ok := s.storage.(storage.NativeStorage)
	if !ok {
		return nil, nativeStatus(storage.ErrNativeUnsupported)
	}
	ctx, permit, err := s.admission.Admit(ctx)
	if err != nil {
		return nil, err
	}
	defer permit.Release()
	countRequest := storage.CountRequest{Request: request}
	count, err := backend.Count(ctx, countRequest)
	if err != nil {
		return nil, nativeStatus(err)
	}
	response := &sink.CountResponse{Count: count.Count, Estimated: count.Estimated}
	return response, nil
}

func (s *BatchingServer) Count(ctx context.Context, req *sink.CountRequest) (*sink.CountResponse, error) {
	return s.server.Count(ctx, req)
}
