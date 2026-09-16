package search

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liran/sink/internal/storage"
)

func TestNativeURITargetsResourceAndKeepsOperationSeparate(t *testing.T) {
	var observed []string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = append(observed, r.URL.EscapedPath())
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	options := Options{Driver: DriverOpenSearch, Store: "search", Endpoints: []string{backend.URL}}
	store, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	commands := []storage.NativeRequest{
		{URI: "sink://search/products", Method: "GET", Path: "/_search"},
		{URI: "sink://search/products", Method: "GET", Path: "/_doc/a%2Fb"},
		{URI: "sink://search", Method: "GET"},
	}
	for _, command := range commands {
		if _, err := store.Execute(t.Context(), command); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"/products/_search", "/products/_doc/a%2Fb", "/"}
	if len(observed) != len(want) {
		t.Fatalf("unexpected requests: %v", observed)
	}
	for i := range want {
		if observed[i] != want[i] {
			t.Fatalf("request %d = %q, want %q", i, observed[i], want[i])
		}
	}
	invalid := []storage.NativeRequest{
		{URI: "sink://other/products", Method: "GET", Path: "/_search"},
		{URI: "sink://search/products/_search", Method: "GET"},
		{URI: "sink://search/products%2Fother", Method: "GET", Path: "/_search"},
		{URI: "sink://search/products", Method: "GET", Path: "/../other/_search"},
		{URI: "sink://search/products", Method: "GET", Path: "/%2e%2e/other/_search"},
		{URI: "sink://search/products", Method: "GET", Path: "//other/_search"},
	}
	for _, command := range invalid {
		_, err := store.Execute(t.Context(), command)
		code, _ := storage.ErrorDetails(err)
		if code != storage.ErrorCodeInvalidArgument {
			t.Errorf("invalid target reached backend: %+v: %v", command, err)
		}
	}
	if len(observed) != len(want) {
		t.Fatal("invalid resource or operation reached transport")
	}
}

func TestNativeURIQueryUsesResourceWithoutOperationSuffix(t *testing.T) {
	store := &Store{logicalStore: "search"}
	command := storage.NativeRequest{URI: "sink://search/products"}
	opts, _, err := store.pageOptions(command)
	if err != nil || opts.path != "/products/_search" || opts.method != http.MethodPost {
		t.Fatalf("resource query: %+v, %v", opts, err)
	}
	if command.Path != "" || command.Method != "" {
		t.Fatal("query mutated caller command")
	}
}
