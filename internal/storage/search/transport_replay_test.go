package search

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/batchstream/sink/internal/storage"
)

func TestNativeMutationDoesNotReplayAfterLostReply(t *testing.T) {
	for _, header := range []string{"Idempotency-Key", "X-Idempotency-Key", "iDeMpOtEnCy-KeY"} {
		t.Run(header, func(t *testing.T) {
			var applied atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_, _ = io.WriteString(w, `{}`)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				if applied.Add(1) == 1 {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				_, _ = io.WriteString(w, `{"result":"updated"}`)
			})
			backend := httptest.NewServer(handler)
			t.Cleanup(backend.Close)
			opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(store.Close)
			if err := store.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}
			req := storage.NativeRequest{URI: "sink://primary/products", Method: http.MethodPost, Path: "/_update/one", ContentType: ContentTypeJSON, Payload: []byte(`{"script":{"source":"ctx._source.count++"}}`), Headers: http.Header{header: {"logical-operation"}}, MaxBytes: 4096}
			result, err := store.Execute(t.Context(), req)
			code, retryable := storage.ErrorDetails(err)
			if code != storage.ErrorCodeInvalidArgument || retryable || applied.Load() != 0 || result.Success {
				t.Fatalf("retry-control header reached backend: applied=%d response=%+v error=%v", applied.Load(), result, err)
			}
			req.Headers = nil
			result, err = store.Execute(t.Context(), req)
			code, _ = storage.ErrorDetails(err)
			if code != storage.ErrorCodeUnavailable || applied.Load() != 1 || result.Success {
				t.Fatalf("lost mutation reply was replayed or acknowledged: applied=%d response=%+v error=%v", applied.Load(), result, err)
			}
			result, err = store.Execute(t.Context(), req)
			if err != nil || !result.Success || applied.Load() != 2 {
				t.Fatalf("subsequent explicit request failed: applied=%d response=%+v error=%v", applied.Load(), result, err)
			}
		})
	}
}

func TestRecordMutationDoesNotReplayOnRedirect(t *testing.T) {
	for _, redirect := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(http.StatusText(redirect), func(t *testing.T) {
			var applied atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if applied.Add(1) == 1 {
					w.Header().Set("Location", "/_bulk")
					w.WriteHeader(redirect)
					return
				}
				_, _ = io.WriteString(w, `{"items":[{"index":{"_index":"legacy-records","_id":"one","status":200,"_seq_no":1,"_primary_term":1}}]}`)
			})
			backend := httptest.NewServer(handler)
			t.Cleanup(backend.Close)
			opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(store.Close)
			document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{"value":1}`)}
			operation := storage.WriteOperation{Address: testAddress("one"), Document: document}
			req := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
			result, err := store.Write(t.Context(), req)
			if err != nil || len(result.Results) != 1 || result.Results[0].Status != storage.WriteStatusFailed || result.Results[0].Err == nil || applied.Load() != 1 {
				t.Fatalf("redirect replayed or acknowledged mutation: applied=%d result=%+v error=%v", applied.Load(), result, err)
			}
		})
	}
}

func TestSearchRedirectPolicyDoesNotChangeSuppliedClient(t *testing.T) {
	var redirects atomic.Int32
	callerError := errors.New("caller redirect policy")
	client := &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		redirects.Add(1)
		return callerError
	}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "/redirected")
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}, HTTPClient: client}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Ping(t.Context()); err == nil || redirects.Load() != 0 {
		t.Fatalf("Store invoked supplied redirect policy: calls=%d error=%v", redirects.Load(), err)
	}
	response, err := client.Get(backend.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if !errors.Is(err, callerError) || redirects.Load() != 1 {
		t.Fatalf("Store changed supplied client: calls=%d error=%v", redirects.Load(), err)
	}
}
