package search

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestSearchReadResultsOwnTheirDocuments(t *testing.T) {
	source := []byte(`{"value":1}`)
	payload := []byte(`{"docs":[{"_index":"legacy-records","_id":"one","found":true,"_seq_no":0,"_primary_term":1,"_source":{"value":1}},{"_index":"legacy-records","_id":"one","found":true,"_seq_no":0,"_primary_term":1,"_source":{"value":1}}]}`)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(payload) })
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	operation := storage.ReadOperation{Address: testAddress("one")}
	request := storage.ReadRequest{Operations: []storage.ReadOperation{operation, operation}}
	first, err := store.Read(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Results) != 2 || first.Results[0].Status != storage.ReadStatusFound || first.Results[1].Status != storage.ReadStatusFound {
		t.Fatalf("first read: %+v", first)
	}
	first.Results[0].Document.Payload[0] = '['
	first.Results[0].Revision.Data[0] = 1
	if !bytes.Equal(first.Results[1].Document.Payload, source) || first.Results[1].Revision.Data[0] != 0 {
		t.Fatal("duplicate reads share mutable storage")
	}
	second, err := store.Read(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range second.Results {
		if result.Status != storage.ReadStatusFound || !bytes.Equal(result.Document.Payload, source) {
			t.Fatalf("later read changed: %+v", result)
		}
	}
	second.Results[0].Document.Payload[0] = ']'
	if !bytes.Equal(first.Results[1].Document.Payload, source) {
		t.Fatal("later read modified a retained earlier result")
	}
}
