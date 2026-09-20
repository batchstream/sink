package search

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/batchstream/sink/internal/storage"
)

func TestManagedQueryRejectsInvalidUTF8BeforeJSONConversion(t *testing.T) {
	requests := make(chan []byte, 8)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- body
		_, _ = io.WriteString(w, `{"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`)
	})
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	opts := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{backend.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.NativeRequest{URI: "sink://search", Method: "POST", Path: "/products/_search", ContentType: ContentTypeJSON, Payload: []byte(`{}`), MaxBytes: 4096}
	for _, field := range []string{"sort", "projection", "payload keys", "payload value"} {
		request := storage.QueryRequest{Request: command, PageSize: 1}
		switch field {
		case "sort":
			sort := storage.SortField{Field: "field\xff"}
			request.Sort = []storage.SortField{sort}
		case "projection":
			request.Projection = &storage.Projection{Fields: []string{"field\xff"}}
		case "payload keys":
			request.Request.Payload = []byte("{\"\xff\":1,\"\xfe\":2}")
		case "payload value":
			request.Request.Payload = []byte("{\"query\":{\"match\":{\"name\":\"\xff\"}}}")
		}
		_, err := store.Query(t.Context(), request)
		code, _ := storage.ErrorDetails(err)
		if code != storage.ErrorCodeInvalidArgument {
			t.Errorf("invalid %s was accepted: %v", field, err)
		}
		select {
		case body := <-requests:
			t.Errorf("invalid %s reached backend after conversion: %q", field, body)
		default:
		}
	}
	sort := storage.SortField{Field: "价格"}
	projection := &storage.Projection{Fields: []string{"名称�"}}
	valid := storage.QueryRequest{Request: command, PageSize: 1, Sort: []storage.SortField{sort}, Projection: projection}
	if _, err := store.Query(t.Context(), valid); err != nil {
		t.Fatal(err)
	}
	body := <-requests
	if !bytes.Contains(body, []byte("价格")) || !bytes.Contains(body, []byte("名称�")) {
		t.Fatalf("valid Unicode fields were changed: %s", body)
	}
}
