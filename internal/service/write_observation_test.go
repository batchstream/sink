package service

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/merge"
	sinkmetrics "github.com/batchstream/sink/internal/metrics"
	"github.com/batchstream/sink/internal/storage/memory"
)

func TestWriteObservationsCountOnlyRetriedDocuments(t *testing.T) {
	observed, err := sinkmetrics.New("test", "primary")
	if err != nil {
		t.Fatal(err)
	}
	backend := newHeldReadStorage(t)
	backend.conflict = true
	backend.unblock()
	core := completionServer(t, backend).server
	core.metrics = observed
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	call := completionWriteCall(t.Context(), mode, completionMerge("fast", 1), completionMerge("slow", 1))
	response, err := core.Write(t.Context(), call.request)
	if err != nil || len(response.GetResults()) != 2 {
		t.Fatalf("write: %v, %v", response, err)
	}
	recorder := httptest.NewRecorder()
	observed.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	wanted := []string{
		`sink_write_phase_duration_seconds_count{phase="storage_read",store="primary"} 2`,
		`sink_write_phase_duration_seconds_count{phase="storage_write_applied",store="primary"} 2`,
		`sink_write_phase_duration_seconds_count{phase="lua",store="primary"} 3`,
		`sink_write_execution_rounds_sum{phase="storage_read",store="primary"} 2`,
		`sink_write_execution_rounds_count{phase="storage_write",store="primary"} 1`,
	}
	for _, line := range wanted {
		if !strings.Contains(body, line) {
			t.Errorf("missing metric: %s", line)
		}
	}
}

func TestBatchQueueObservesEveryRPCIncludingCanceledAndShutdown(t *testing.T) {
	observed, err := sinkmetrics.New("test", "primary")
	if err != nil {
		t.Fatal(err)
	}
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		for _, call := range calls {
			completeCall(call, call.request, nil)
		}
	}
	opts := requestBatcherOptions[int, int]{Store: "primary", Method: "Read", Metrics: observed, MaxWait: time.Hour, MaxOperations: 2, MaxBytes: 100, MaxQueuedOperations: 10, MaxQueuedBytes: 1000, Execute: execute}
	batcher := newRequestBatcher(opts)
	t.Cleanup(batcher.Close)
	results := make(chan batcherSubmission, 2)
	go submitBatcherTestRequest(batcher, 1, results)
	waitForQueuedCalls(t, batcher, 1)
	go submitBatcherTestRequest(batcher, 2, results)
	for range 2 {
		if result := awaitCompletion(t, results); result.err != nil {
			t.Fatal(result)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	canceled := make(chan error, 1)
	go func() { _, err := batcher.Submit(ctx, 3, 1, 1); canceled <- err }()
	waitForQueuedCalls(t, batcher, 1)
	cancel()
	if err := awaitCompletion(t, canceled); err == nil {
		t.Fatal("queued cancellation succeeded")
	}
	waitForQueuedCalls(t, batcher, 0)
	go submitBatcherTestRequest(batcher, 4, results)
	waitForQueuedCalls(t, batcher, 1)
	batcher.Close()
	if result := awaitCompletion(t, results); result.err == nil {
		t.Fatal("queued shutdown succeeded")
	}
	recorder := httptest.NewRecorder()
	observed.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	wanted := []string{
		`sink_batcher_request_queue_duration_seconds_count{method="Read",store="primary"} 4`,
		`sink_batcher_request_queue_exits_total{method="Read",outcome="execute",store="primary"} 2`,
		`sink_batcher_request_queue_exits_total{method="Read",outcome="canceled",store="primary"} 1`,
		`sink_batcher_request_queue_exits_total{method="Read",outcome="shutdown",store="primary"} 1`,
		`sink_batcher_batches_total{method="Read",reason="max_operations",store="primary"} 1`,
	}
	for _, line := range wanted {
		if !strings.Contains(body, line) {
			t.Errorf("missing metric: %s", line)
		}
	}
}

func TestWriteObservationsDoNotLabelArbitraryStores(t *testing.T) {
	observed, err := sinkmetrics.New("test", "primary")
	if err != nil {
		t.Fatal(err)
	}
	backend := newHeldReadStorage(t)
	core := completionServer(t, backend).server
	core.metrics = observed
	for index := range 10 {
		op := completionPut("key", index)
		op.Address.Uri = fmt.Sprintf("sink://untrusted-client-store-%d/catalog/products/s:key", index)
		call := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, op)
		observation := core.newWriteObservation(call.request)
		// Exercise the real request-to-store classification without a long sleep.
		observation.phase("storage_write", time.Now().Add(-6*time.Second))
	}
	recorder := httptest.NewRecorder()
	observed.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	body := recorder.Body.String()
	if strings.Contains(body, "untrusted-client-store") || !strings.Contains(body, `sink_write_slow_phases_total{phase="storage_write_applied",store="_unconfigured"} 10`) {
		t.Fatal("client input escaped bounded metric labels")
	}
}

func TestBatchedWritesKeepQueueAndPhaseMetricsSeparateByStore(t *testing.T) {
	stores := []string{"alpha", "beta"}
	observed, err := sinkmetrics.New("test", stores...)
	if err != nil {
		t.Fatal(err)
	}
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	batchers := make(map[string]*BatchingServer)
	for _, store := range stores {
		coreOptions := Options{BoundStore: store, Storage: memory.New(), Lua: lua, Metrics: observed}
		core, err := New(coreOptions)
		if err != nil {
			t.Fatal(err)
		}
		batchOptions := BatchingOptions{Metrics: observed, MaxOperations: 1}
		batching, err := NewBatchingServer(core, batchOptions)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(batching.Close)
		batchers[store] = batching
	}
	for _, store := range []string{"alpha", "beta", "alpha"} {
		operation := completionPut("key", 1)
		operation.Address.Uri = "sink://" + store + "/catalog/products/s:key"
		request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
		response, err := batchers[store].Write(t.Context(), request)
		if err != nil || len(response.GetResults()) != 1 || response.Results[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("%s: response %v, error %v", store, response, err)
		}
	}
	for _, batching := range batchers {
		batching.Close()
	}
	request := httptest.NewRequest("GET", "/metrics", nil)
	recorder := httptest.NewRecorder()
	observed.Handler().ServeHTTP(recorder, request)
	body := recorder.Body.String()
	for store, count := range map[string]int{"alpha": 2, "beta": 1} {
		wanted := []string{
			fmt.Sprintf(`sink_batcher_batches_total{method="Write",reason="max_operations",store="%s"} %d`, store, count),
			fmt.Sprintf(`sink_batcher_request_queue_duration_seconds_count{method="Write",store="%s"} %d`, store, count),
			fmt.Sprintf(`sink_batcher_queued_operations{method="Write",store="%s"} 0`, store),
			fmt.Sprintf(`sink_batcher_queued_bytes{method="Write",store="%s"} 0`, store),
			fmt.Sprintf(`sink_write_phase_duration_seconds_count{phase="storage_write_applied",store="%s"} %d`, store, count),
			fmt.Sprintf(`sink_write_execution_rounds_sum{phase="storage_write",store="%s"} %d`, store, count),
		}
		for _, line := range wanted {
			if !strings.Contains(body, line) {
				t.Errorf("missing metric: %s", line)
			}
		}
	}
}
