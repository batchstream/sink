//go:build integration

package search_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/search"
)

func TestSearchNativeMutationTransportBoundary(t *testing.T) {
	for _, header := range []string{"X-Opaque-Id", "Idempotency-Key", "x-IDEMPOTENCY-key"} {
		t.Run(header, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			code, body := fixture.request(t, http.MethodPut, "/"+fixture.index+"/_doc/item", []byte(`{"counter":0}`))
			if code != http.StatusCreated {
				t.Fatalf("seed: %d %s", code, body)
			}
			target, err := url.Parse(fixture.endpoint)
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(target)
			var mutations atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/"+fixture.index+"/_update/item" {
					proxy.ServeHTTP(w, r)
					return
				}
				recorded := httptest.NewRecorder()
				proxy.ServeHTTP(recorded, r)
				if recorded.Code != http.StatusOK {
					t.Errorf("native increment: %d %s", recorded.Code, recorded.Body.String())
				}
				if mutations.Add(1) == 1 {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				for key, values := range recorded.Header() {
					w.Header()[key] = values
				}
				w.WriteHeader(recorded.Code)
				_, _ = io.Copy(w, recorded.Body)
			})
			backend := httptest.NewServer(handler)
			t.Cleanup(backend.Close)
			opts := search.Options{Driver: fixture.driver, Store: "primary", Endpoints: []string{backend.URL}}
			store, err := search.New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(store.Close)
			if err := store.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}
			command := storage.NativeRequest{URI: "sink://primary/" + fixture.index, Method: http.MethodPost, Path: "/_update/item", ContentType: search.ContentTypeJSON, Payload: []byte(`{"script":{"source":"ctx._source.counter += 1"}}`), Headers: http.Header{header: {"test-operation"}}, MaxBytes: 4096}
			response, err := store.Execute(t.Context(), command)
			if header != "X-Opaque-Id" {
				code, _ := storage.ErrorDetails(err)
				if code != storage.ErrorCodeInvalidArgument || mutations.Load() != 0 {
					t.Fatalf("retry-control header reached storage: mutations=%d response=%+v error=%v", mutations.Load(), response, err)
				}
				command.Headers = nil
				response, err = store.Execute(t.Context(), command)
			}
			if err == nil || response.Success || mutations.Load() != 1 {
				t.Fatalf("lost increment acknowledgement: mutations=%d response=%+v error=%v", mutations.Load(), response, err)
			}
			read := storage.ReadOperation{Address: fixture.address("item")}
			request := storage.ReadRequest{Operations: []storage.ReadOperation{read}}
			found, err := fixture.store.Read(t.Context(), request)
			if err != nil || len(found.Results) != 1 || found.Results[0].Status != storage.ReadStatusFound {
				t.Fatalf("read persisted increment: %+v %v", found, err)
			}
			var persisted struct {
				Counter int `json:"counter"`
			}
			if err := json.Unmarshal(found.Results[0].Document.Payload, &persisted); err != nil {
				t.Fatal(err)
			}
			if persisted.Counter != 1 {
				t.Fatalf("persisted counter = %d, want 1", persisted.Counter)
			}
			command.Headers = nil
			response, err = store.Execute(t.Context(), command)
			if err != nil || !response.Success || mutations.Load() != 2 {
				t.Fatalf("explicit subsequent call did not recover: %+v %v", response, err)
			}
		})
	}
}

func TestSearchRecordRedirectIsNotReplayed(t *testing.T) {
	for _, redirect := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(redirect), func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			target, err := url.Parse(fixture.endpoint)
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(target)
			var mutations atomic.Int32
			proxy.ModifyResponse = func(response *http.Response) error {
				if response.Request.URL.Path == "/_bulk" && mutations.Add(1) == 1 {
					if response.StatusCode != http.StatusOK {
						t.Errorf("real bulk request returned HTTP %d", response.StatusCode)
					}
					response.StatusCode = redirect
					response.Header.Set("Location", "/_bulk")
				}
				return nil
			}
			backend := httptest.NewServer(proxy)
			t.Cleanup(backend.Close)
			opts := search.Options{Driver: fixture.driver, Store: "primary", Endpoints: []string{backend.URL}}
			store, err := search.New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(store.Close)
			document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{"counter":1}`)}
			operation := storage.WriteOperation{Address: fixture.address("item"), Document: document}
			request := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
			response, err := store.Write(t.Context(), request)
			if err != nil || len(response.Results) != 1 || response.Results[0].Status != storage.WriteStatusFailed || mutations.Load() != 1 {
				t.Fatalf("redirect replayed or acknowledged bulk: mutations=%d response=%+v error=%v", mutations.Load(), response, err)
			}
			code, body := fixture.request(t, http.MethodGet, "/"+fixture.index+"/_doc/item", nil)
			var persisted struct {
				Sequence *int64 `json:"_seq_no"`
				Source   struct {
					Counter int `json:"counter"`
				} `json:"_source"`
			}
			if err := json.Unmarshal(body, &persisted); err != nil {
				t.Fatal(err)
			}
			if code != http.StatusOK || persisted.Sequence == nil || *persisted.Sequence != 0 || persisted.Source.Counter != 1 {
				t.Fatalf("redirect changed persisted write sequence: HTTP %d %s", code, body)
			}
		})
	}
}
