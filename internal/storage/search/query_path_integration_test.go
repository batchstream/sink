//go:build integration

package search_test

import (
	"net/http"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestManagedQueriesCannotOverwriteSearchNamedDocument(t *testing.T) {
	for _, method := range []string{"Query", "Count", "Scan"} {
		t.Run(method, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			path := "/" + fixture.index + "/_doc/_search"
			code, body := fixture.request(t, http.MethodPut, path, []byte(`{"original":true}`))
			if code != http.StatusCreated {
				t.Fatalf("insert HTTP %d: %s", code, body)
			}
			_, before := fixture.request(t, http.MethodGet, path, nil)
			command := storage.NativeRequest{URI: "sink://primary", Method: "POST", Path: path,
				ContentType: "application/json", Payload: []byte(`{"sort":["uid"]}`), MaxBytes: 4096}
			var err error
			switch method {
			case "Query":
				request := storage.QueryRequest{Request: command, PageSize: 1}
				_, err = fixture.store.Query(t.Context(), request)
			case "Count":
				request := storage.CountRequest{Request: command}
				_, err = fixture.store.Count(t.Context(), request)
			case "Scan":
				request := storage.ScanRequest{Request: command, BatchSize: 1}
				_, err = fixture.store.Scan(t.Context(), request)
			}
			failureCode, retryable := storage.ErrorDetails(err)
			if failureCode != storage.ErrorCodeInvalidArgument || retryable {
				t.Errorf("mutation endpoint was not rejected: %v", err)
			}
			_, after := fixture.request(t, http.MethodGet, path, nil)
			if string(before) != string(after) {
				t.Fatalf("managed read changed the document: before=%s after=%s", before, after)
			}
		})
	}
}
