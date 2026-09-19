package service_test

import (
	"context"
	"net/http"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

type nativeFixtureStorage struct {
	*memory.Store
	stopped         chan struct{}
	silent          bool
	started         chan struct{}
	queries         chan storage.QueryRequest
	scans           chan storage.ScanRequest
	counts          chan storage.CountRequest
	executeErr      error
	executeResponse *storage.NativeResponse
}

func (s *nativeFixtureStorage) Query(_ context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	if s.queries != nil {
		s.queries <- req
	}
	result := storage.QueryResponse{}
	return result, nil
}

func (s *nativeFixtureStorage) Count(_ context.Context, req storage.CountRequest) (storage.CountResponse, error) {
	if s.counts != nil {
		s.counts <- req
	}
	result := storage.CountResponse{Count: 123, Estimated: true}
	return result, nil
}

func (s *nativeFixtureStorage) Execute(_ context.Context, _ storage.NativeRequest) (storage.NativeResponse, error) {
	if s.executeResponse != nil {
		return *s.executeResponse, s.executeErr
	}
	response := storage.NativeResponse{ContentType: "application/json", Payload: []byte("{\"error\":\"native\"}\n"), StatusCode: 400,
		Headers: http.Header{"Warning": {"first", "second"}}}
	return response, s.executeErr
}

func (s *nativeFixtureStorage) Scan(ctx context.Context, req storage.ScanRequest) (storage.ScanResponse, error) {
	var response storage.ScanResponse
	if s.scans != nil {
		s.scans <- req
	}
	if s.silent {
		close(s.started)
		defer close(s.stopped)
		<-ctx.Done()
		return response, ctx.Err()
	}
	document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{"value":1}`)}
	response.Documents = []storage.Document{document}
	return response, nil
}
