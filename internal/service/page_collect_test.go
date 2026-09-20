package service

import (
	"context"
	sink "github.com/batchstream/sink/gen/sink"
)

func (s *Server) Query(ctx context.Context, req *sink.QueryRequest) (*sink.QueryResponse, error) {
	response := &sink.QueryResponse{}
	err := s.query(ctx, req, func(frame *sink.QueryResponse) error {
		response.Documents = append(response.Documents, frame.Documents...)
		response.HasMore = frame.HasMore
		response.Complete = frame.Complete
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *BatchingServer) Query(ctx context.Context, req *sink.QueryRequest) (*sink.QueryResponse, error) {
	return s.server.Query(ctx, req)
}

func (s *Server) Scan(ctx context.Context, req *sink.ScanRequest) (*sink.ScanResponse, error) {
	response := &sink.ScanResponse{}
	err := s.scan(ctx, req, func(frame *sink.ScanResponse) error {
		response.Documents = append(response.Documents, frame.Documents...)
		response.NextCursor = frame.NextCursor
		response.Complete = frame.Complete
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *BatchingServer) Scan(ctx context.Context, req *sink.ScanRequest) (*sink.ScanResponse, error) {
	return s.server.Scan(ctx, req)
}
