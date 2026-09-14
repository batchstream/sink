package search

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestManagedQueriesFailOverToHealthyEndpoint(t *testing.T) {
	for _, method := range []string{"query", "count", "scan", "execute"} {
		t.Run(method, func(t *testing.T) {
			var firstCalls, secondCalls atomic.Int32
			firstHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				firstCalls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			})
			first := httptest.NewServer(firstHandler)
			t.Cleanup(first.Close)
			secondHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				secondCalls.Add(1)
				_, _ = io.WriteString(w, `{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`)
			})
			second := httptest.NewServer(secondHandler)
			t.Cleanup(second.Close)
			opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{first.URL, second.URL}}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			command := storage.NativeRequest{Store: "search", Method: "POST", Path: "/products/_search", ContentType: ContentTypeJSON, Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 4096}
			switch method {
			case "query":
				request := storage.QueryRequest{Request: command, PageSize: 2}
				_, err = store.Query(t.Context(), request)
			case "count":
				request := storage.CountRequest{Request: command}
				_, err = store.Count(t.Context(), request)
			case "scan":
				request := storage.ScanRequest{Request: command, BatchSize: 2}
				_, err = store.Scan(t.Context(), request)
			case "execute":
				response, callErr := store.Execute(t.Context(), command)
				err = callErr
				if response.Success || response.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("Execute lost native failure: %+v", response)
				}
			}
			wantSecond := int32(1)
			if method == "execute" {
				wantSecond = 0
			}
			if err != nil || firstCalls.Load() != 1 || secondCalls.Load() != wantSecond {
				t.Fatalf("query failed to recover: first=%d second=%d error=%v", firstCalls.Load(), secondCalls.Load(), err)
			}
		})
	}
}
