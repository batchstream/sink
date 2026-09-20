package service

import (
	"context"
	"net/http"
	"strings"
	"testing"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nativeBoundaryStore struct {
	*memory.Store
	calls     int
	request   storage.NativeRequest
	query     storage.QueryRequest
	scan      storage.ScanRequest
	failure   error
	output    storage.NativeResponse
	documents []storage.Document
	more      bool
}

func (s *nativeBoundaryStore) Execute(_ context.Context, req storage.NativeRequest) (storage.NativeResponse, error) {
	s.calls++
	s.request = req
	return s.output, s.failure
}
func (s *nativeBoundaryStore) Query(_ context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	s.calls++
	s.query = req
	s.request = req.Request
	response := storage.QueryResponse{Documents: s.documents, HasMore: s.more}
	if req.Emit != nil && s.failure == nil {
		for _, doc := range response.Documents {
			if err := req.Emit(doc); err != nil {
				return response, err
			}
		}
		response.Documents = nil
	}
	return response, s.failure
}
func (s *nativeBoundaryStore) Count(_ context.Context, req storage.CountRequest) (storage.CountResponse, error) {
	s.calls++
	s.request = req.Request
	response := storage.CountResponse{Count: 123, Estimated: true}
	return response, s.failure
}
func (s *nativeBoundaryStore) Scan(_ context.Context, req storage.ScanRequest) (storage.ScanResponse, error) {
	s.calls++
	s.scan = req
	s.request = req.Request
	response := storage.ScanResponse{Documents: s.documents}
	if req.Emit != nil && s.failure == nil {
		for _, doc := range response.Documents {
			if err := req.Emit(doc); err != nil {
				return response, err
			}
		}
		response.Documents = nil
	}
	return response, s.failure
}

func TestNativeBoundariesRetainWireLimitsAndCallerContext(t *testing.T) {
	backend := &nativeBoundaryStore{Store: memory.New()}
	server := completionServer(t, backend)
	command := &sink.Command{Uri: "sink://primary", Method: "POST", Path: "/records/_search", ContentType: "application/json", Payload: []byte(`{}`)}
	header := &sink.Header{Name: "X-Request", Values: []string{"one", "two"}}
	command.Headers = []*sink.Header{header}
	payload := []byte(`{"value":1}`)
	document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: payload}
	backend.documents = []storage.Document{document}
	backend.output = storage.NativeResponse{ContentType: "application/json", Payload: payload, StatusCode: 400, Headers: http.Header{"Warning": {"first", "second"}}}
	execute := &sink.ExecuteRequest{Command: command}
	response, err := server.Execute(t.Context(), execute)
	if err != nil || response.GetStatusCode() != 400 || response.GetSuccess() || string(response.GetPayload()) != string(payload) || len(response.GetHeaders()[0].GetValues()) != 2 {
		t.Fatalf("native response changed: %v %v", response, err)
	}
	if backend.request.Headers.Get("X-Request") != "one" {
		t.Fatal("request headers lost")
	}
	projection := &sink.Projection{Fields: []string{"value"}}
	field := &sink.SortField{Field: "value", Descending: true}
	query := &sink.QueryRequest{Command: command, Page: 2, PageSize: 1, Sort: []*sink.SortField{field}, Projection: projection}
	page, err := server.Query(t.Context(), query)
	if err != nil || len(page.GetDocuments()) != 1 || backend.query.Offset != 1 || !backend.query.Sort[0].Descending || backend.query.Projection.Fields[0] != "value" {
		t.Fatalf("query settings or page lost: %v %v", page, err)
	}
	count := &sink.CountRequest{Command: command}
	total, err := server.Count(t.Context(), count)
	if err != nil || total.GetCount() != 123 || !total.GetEstimated() || backend.request.MaxBytes != 256<<10 {
		t.Fatalf("count bound/result: %v %v", total, err)
	}
	scan := &sink.ScanRequest{Command: command, Projection: projection}
	scanned, err := server.Scan(t.Context(), scan)
	if err != nil || len(scanned.GetDocuments()) != 1 || backend.scan.BatchSize != 100 || backend.scan.Request.MaxBytes >= 4<<20 {
		t.Fatalf("scan page/bound: %v %v", scanned, err)
	}
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded, codes.Unavailable} {
		backend.failure = status.Error(code, "backend stopped")
		calls := []func() error{
			func() error { _, err := server.Execute(t.Context(), execute); return err },
			func() error { _, err := server.Query(t.Context(), query); return err },
			func() error { _, err := server.Count(t.Context(), count); return err },
			func() error { _, err := server.Scan(t.Context(), scan); return err },
		}
		for _, call := range calls {
			if err := call(); status.Code(err) != code {
				t.Fatalf("native error changed: %v", err)
			}
		}
	}
	backend.failure = nil
	server.server.maxReadBytes = 1024
	backend.output.Payload = []byte(strings.Repeat("x", 2048))
	if _, err := server.Execute(t.Context(), execute); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized execute: %v", err)
	}
	backend.documents[0].Payload = backend.output.Payload
	if _, err := server.Query(t.Context(), query); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized query: %v", err)
	}
	if _, err := server.Scan(t.Context(), scan); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized scan: %v", err)
	}
}

func TestNativeValidationStopsBeforeBackend(t *testing.T) {
	backend := &nativeBoundaryStore{Store: memory.New()}
	server := completionServer(t, backend)
	invalid := []*sink.Command{
		{Uri: "invalid"}, {Uri: "sink://other"}, {Uri: "sink://primary", Method: string([]byte{0xff})},
		{Uri: "sink://primary", Payload: []byte(`{}`)}, {Uri: "sink://primary", ContentType: "application/json; =bad"},
		{Uri: "sink://primary", Headers: []*sink.Header{nil}},
		{Uri: "sink://primary", Headers: []*sink.Header{{Name: string([]byte{0xff}), Values: []string{"x"}}}},
		{Uri: "sink://primary", Headers: []*sink.Header{{Name: "X", Values: []string{string([]byte{0xff})}}}},
	}
	for _, command := range invalid {
		execute := &sink.ExecuteRequest{Command: command}
		query := &sink.QueryRequest{Command: command}
		count := &sink.CountRequest{Command: command}
		scan := &sink.ScanRequest{Command: command}
		calls := []func() error{
			func() error { _, err := server.Execute(t.Context(), execute); return err },
			func() error { _, err := server.Query(t.Context(), query); return err },
			func() error { _, err := server.Count(t.Context(), count); return err },
			func() error { _, err := server.Scan(t.Context(), scan); return err },
		}
		for _, call := range calls {
			if call() == nil {
				t.Fatal("invalid command accepted")
			}
		}
	}
	command := &sink.Command{Uri: "sink://primary"}
	query := &sink.QueryRequest{Command: command, PageSize: 1001}
	if _, err := server.Query(t.Context(), query); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	scan := &sink.ScanRequest{Command: command, BatchSize: 1001}
	if _, err := server.Scan(t.Context(), scan); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	scan.BatchSize = 1
	scan.Cursor = []byte("invalid")
	if _, err := server.Scan(t.Context(), scan); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if backend.calls != 0 {
		t.Fatalf("invalid request reached backend: %d", backend.calls)
	}
}
