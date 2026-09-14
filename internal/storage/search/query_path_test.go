package search

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestManagedQueriesRejectMutationPathsBeforeTransport(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"result":"created"}`))
	})
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{backend.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/products/_doc/_search", "/products/_create/_search", "/products/_update/_search",
		"/products/_doc/id%2F_search", "/products%2F_doc/_search", "/products%2F_search",
		"/_scripts/_search", "/_ingest/pipeline/_search", "/_index_template/_search",
	} {
		t.Run(path, func(t *testing.T) {
			command := storage.NativeRequest{Store: "search", Method: "POST", Path: path,
				ContentType: ContentTypeJSON, Payload: []byte(`{"sort":["uid"]}`)}
			query := storage.QueryRequest{Request: command, PageSize: 1}
			_, queryErr := store.Query(t.Context(), query)
			count := storage.CountRequest{Request: command}
			_, countErr := store.Count(t.Context(), count)
			scan := storage.ScanRequest{Request: command, BatchSize: 1}
			_, scanErr := store.Scan(t.Context(), scan)
			for method, err := range map[string]error{"Query": queryErr, "Count": countErr, "Scan": scanErr} {
				code, retryable := storage.ErrorDetails(err)
				if code != storage.ErrorCodeInvalidArgument || retryable {
					t.Errorf("%s accepted a mutation endpoint: %v", method, err)
				}
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("managed reads sent %d requests to mutation endpoints", calls.Load())
	}
}

func TestManagedQueriesAcceptSearchPaths(t *testing.T) {
	for _, path := range []string{"/_search", "/products/_search", "/products/%5fsearch", "/_all/_search", "/_all,products/_search", "/*/_search", "/products-*/_search", "/local,remote:products/_search", "/_remote:products/_search"} {
		command := storage.NativeRequest{Method: "GET", Path: path}
		if _, _, err := pageOptions(command); err != nil {
			t.Errorf("valid search path %q rejected: %v", path, err)
		}
	}
}
