package search

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestQueryHasMoreRequiresProofFromTheSamePage(t *testing.T) {
	tests := []struct {
		name    string
		total   string
		count   int
		more    bool
		invalid bool
	}{
		{name: "before page", total: `{"value":1,"relation":"eq"}`},
		{name: "empty page", total: `{"value":2,"relation":"eq"}`},
		{name: "partial page", total: `{"value":3,"relation":"eq"}`, count: 1},
		{name: "full last page", total: `{"value":4,"relation":"eq"}`, count: 2},
		{name: "exact more", total: `{"value":5,"relation":"eq"}`, count: 2, more: true},
		{name: "bounded more", total: `{"value":5,"relation":"gte"}`, count: 2, more: true},
		{name: "missing", count: 2, invalid: true},
		{name: "null", total: `null`, invalid: true},
		{name: "negative", total: `{"value":-1,"relation":"eq"}`, invalid: true},
		{name: "unknown relation", total: `{"value":5,"relation":"approx"}`, count: 2, invalid: true},
		{name: "insufficient lower bound", total: `{"value":4,"relation":"gte"}`, count: 2, invalid: true},
		{name: "missing hit", total: `{"value":4,"relation":"eq"}`, count: 1, invalid: true},
		{name: "extra hit", total: `{"value":3,"relation":"eq"}`, count: 2, invalid: true},
		{name: "incomplete bounded page", total: `{"value":5,"relation":"gte"}`, count: 1, invalid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hits := &scanHits{Total: json.RawMessage(test.total), Hits: make([]json.RawMessage, test.count)}
			req := storage.QueryRequest{Offset: 2, PageSize: 2}
			more, err := queryHasMore(hits, req)
			if (err != nil) != test.invalid || more != test.more {
				t.Fatalf("more=%t err=%v", more, err)
			}
		})
	}
}

func TestQueryRejectsUnsupportedPageControlsBeforeTransport(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("invalid page reached backend") })
	backend := httptest.NewServer(handler)
	defer backend.Close()
	opts := Options{Driver: DriverElasticsearch, Store: "search", Endpoints: []string{backend.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.NativeRequest{Store: "search", Method: "POST", Path: "/products/_search", ContentType: ContentTypeJSON, Payload: []byte(`{"collapse":{"field":"category"}}`)}
	query := storage.QueryRequest{Request: command, PageSize: 1}
	_, err = store.Query(t.Context(), query)
	code, retryable := storage.ErrorDetails(err)
	if code != storage.ErrorCodeInvalidArgument || retryable {
		t.Fatalf("collapse: %v", err)
	}
	query.Request.Payload = []byte(`{}`)
	query.Offset = math.MaxInt64
	_, err = store.Query(t.Context(), query)
	code, retryable = storage.ErrorDetails(err)
	if code != storage.ErrorCodeInvalidArgument || retryable {
		t.Fatalf("overflow: %v", err)
	}
}
