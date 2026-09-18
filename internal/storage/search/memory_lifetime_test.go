package search

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/storage"
)

func TestSplitReadsReleaseDiscardedHTTPBodies(t *testing.T) {
	payload := `{"value":"` + strings.Repeat("x", 4000) + `"}`
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var request multiGetRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		documents := make([]string, 0, len(request.Documents))
		for _, document := range request.Documents {
			documents = append(documents, fmt.Sprintf(`{"_index":%q,"_id":%q,"found":true,"_seq_no":0,"_primary_term":1,"_source":%s}`, document.Index, document.ID, payload))
		}
		fmt.Fprintf(w, `{"docs":[%s]}`, strings.Join(documents, ","))
	})
	backend := httptest.NewServer(handler)
	defer backend.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}, MaxResponseSize: 16 << 10}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	poolOpts := capacity.Options{Bytes: 192 << 10, BurstPercent: 10, WaitTimeout: 10 * time.Millisecond}
	pool, err := capacity.New(poolOpts)
	if err != nil {
		t.Fatal(err)
	}
	scope := pool.NewScope()
	defer scope.Release()
	ctx := capacity.WithScope(t.Context(), scope)
	snapshots := scope.NewLease()
	budget := storage.WithMemoryBudget(ctx, storage.NewReadBudget(1<<20), snapshots)
	request := storage.ReadRequest{Budget: budget}
	for index := range 8 {
		operation := storage.ReadOperation{Address: testAddress(fmt.Sprint(index))}
		request.Operations = append(request.Operations, operation)
	}
	response, err := store.Read(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, result := range response.Results {
		if result.Status == storage.ReadStatusFound {
			found++
		} else {
			t.Logf("failed: %v", result.Err)
		}
	}
	t.Logf("found=%d/8 requests=%d pool_used=%d actual_payload=%d", found, calls.Load(), pool.Used(), len(payload)*found)
	if pool.Used() != snapshots.Bytes() {
		t.Fatalf("HTTP working buffers retained after decode: %d", pool.Used())
	}
	scope.Release()
	request.Budget = storage.NewReadBudget(1 << 20)
	store.maxResponseSize = 64 << 10
	controlScope := pool.NewScope()
	defer controlScope.Release()
	controlCtx := capacity.WithScope(t.Context(), controlScope)
	control, controlErr := store.Read(controlCtx, request)
	controlFound := 0
	for _, result := range control.Results {
		if result.Status == storage.ReadStatusFound {
			controlFound++
		}
	}
	t.Logf("control with same pool and documents, unsplit response: found=%d/8 error=%v", controlFound, controlErr)
	if controlErr != nil || controlFound != 8 {
		t.Fatal("invalid control")
	}
	if found != 8 {
		t.Fatal("discarded oversized bodies consumed capacity needed by split reads")
	}
}

func TestLocalMemoryPressureDoesNotCoolDownSearchEndpoints(t *testing.T) {
	var requests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		fmt.Fprint(w, strings.Repeat("x", 24000))
	})
	first, second := httptest.NewServer(handler), httptest.NewServer(handler)
	defer first.Close()
	defer second.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{first.URL, second.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	poolOpts := capacity.Options{Bytes: 32 << 10, BurstPercent: 10, WaitTimeout: 10 * time.Millisecond}
	pool, err := capacity.New(poolOpts)
	if err != nil {
		t.Fatal(err)
	}
	scope := pool.NewScope()
	defer scope.Release()
	ctx := capacity.WithScope(t.Context(), scope)
	request := requestOptions{method: http.MethodGet, path: "/", retrySafe: true}
	_, err = store.perform(ctx, request)
	code, _ := storage.ErrorDetails(err)
	cooled := 0
	for _, endpoint := range store.endpoints {
		if endpoint.retryAfter.Load() > time.Now().UnixNano() {
			cooled++
		}
	}
	t.Logf("error=%v code=%v requests=%d cooled=%d", err, code, requests.Load(), cooled)
	if code != storage.ErrorCodeResourceExhausted || cooled != 0 || requests.Load() != 1 {
		t.Fatal("local memory exhaustion retried and marked healthy endpoints unavailable")
	}
}

func TestSearchFailoverReleasesConsumedResponseMemory(t *testing.T) {
	payload := strings.Repeat("x", 24000)
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, payload)
	}))
	defer failed.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, payload)
	}))
	defer healthy.Close()
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{failed.URL, healthy.URL}}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	options := capacity.Options{Bytes: 96 << 10, BurstPercent: 10, WaitTimeout: time.Second}
	pool, err := capacity.New(options)
	if err != nil {
		t.Fatal(err)
	}
	scope := pool.NewScope()
	defer scope.Release()
	ctx := capacity.WithScope(t.Context(), scope)
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("discarded failover response consumed the next attempt's capacity: %v", err)
	}
	if pool.Used() != 0 {
		t.Fatalf("completed ping retained HTTP response capacity: %d", pool.Used())
	}
}
