package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liran/sink/internal/storage"
)

func TestReadSplitsOversizedResponsesWithoutEndpointFailover(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var request multiGetRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if len(request.Documents) > 1 {
			_, _ = io.WriteString(w, strings.Repeat("x", 513))
			return
		}
		document := request.Documents[0]
		_, _ = fmt.Fprintf(w, `{"docs":[{"_index":%q,"_id":%q,"found":true,"_source":{"value":1},"_seq_no":0,"_primary_term":1}]}`, document.Index, document.ID)
	})
	first := httptest.NewServer(handler)
	t.Cleanup(first.Close)
	second := httptest.NewServer(handler)
	t.Cleanup(second.Close)
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{first.URL, second.URL}, MaxResponseSize: 512}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.ReadRequest{Operations: []storage.ReadOperation{{Address: testAddress("a")}, {Address: testAddress("b")}}}
	response, err := store.Read(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range response.Results {
		if result.Status != storage.ReadStatusFound {
			t.Fatalf("split read failed: %+v", result)
		}
	}
	if calls.Load() != 3 {
		t.Errorf("calls = %d; want one oversized request and two halves", calls.Load())
	}
	for index, endpoint := range store.endpoints {
		if endpoint.retryAfter.Load() != 0 {
			t.Errorf("healthy endpoint %d entered cooldown after a response size limit", index)
		}
	}
}

func TestCanceledReadDoesNotFailOverOrCoolDownEndpoints(t *testing.T) {
	for _, inFlight := range []bool{false, true} {
		t.Run(fmt.Sprintf("in_flight=%t", inFlight), func(t *testing.T) {
			started := make(chan struct{}, 1)
			handler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				started <- struct{}{}
				<-r.Context().Done()
			})
			first := httptest.NewServer(handler)
			t.Cleanup(first.Close)
			var secondCalls atomic.Int32
			secondHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				secondCalls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			})
			second := httptest.NewServer(secondHandler)
			t.Cleanup(second.Close)
			opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{first.URL, second.URL}}
			store, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if !inFlight {
				cancel()
			}
			request := storage.ReadRequest{Operations: []storage.ReadOperation{{Address: testAddress("a")}}}
			finished := make(chan error, 1)
			go func() {
				response, err := store.Read(ctx, request)
				if err == nil {
					err = response.Results[0].Err
				}
				finished <- err
			}()
			if inFlight {
				select {
				case <-started:
					cancel()
				case err := <-finished:
					t.Fatalf("read finished before cancellation: %v", err)
				case <-t.Context().Done():
					t.Fatal("read did not start")
				}
			}
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatalf("read error = %v; want cancellation", err)
			}
			attempts := uint64(0)
			if inFlight {
				attempts = 1
			}
			if got := store.nextEndpoint.Load(); got != attempts || secondCalls.Load() != 0 {
				t.Errorf("attempts = %d, second calls = %d; want %d attempts without failover", got, secondCalls.Load(), attempts)
			}
			for index, endpoint := range store.endpoints {
				if endpoint.retryAfter.Load() != 0 {
					t.Errorf("healthy endpoint %d entered cooldown after caller cancellation", index)
				}
			}
		})
	}
}

func TestReadStillFailsOverOnEndpointTimeout(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	firstHandler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	})
	first := httptest.NewServer(firstHandler)
	t.Cleanup(first.Close)
	secondHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		_, _ = io.WriteString(w, `{"docs":[{"_index":"legacy-records","_id":"a","found":false}]}`)
	})
	second := httptest.NewServer(secondHandler)
	t.Cleanup(second.Close)
	client := &http.Client{Timeout: 100 * time.Millisecond}
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{first.URL, second.URL}, HTTPClient: client}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	request := storage.ReadRequest{Operations: []storage.ReadOperation{{Address: testAddress("a")}}}
	response, err := store.Read(t.Context(), request)
	if err != nil || response.Results[0].Status != storage.ReadStatusNotFound {
		t.Fatalf("read did not recover from endpoint timeout: %+v, %v", response, err)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("endpoint calls = %d, %d; want one each", firstCalls.Load(), secondCalls.Load())
	}
	if store.endpoints[0].retryAfter.Load() == 0 || store.endpoints[1].retryAfter.Load() != 0 {
		t.Fatal("only the timed-out endpoint should enter cooldown")
	}
}
