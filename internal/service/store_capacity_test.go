package service

import (
	"context"
	"testing"
	"testing/synctest"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteBatchSplitsAtStoreByteLimit(t *testing.T) {
	backend := &syncCapacityStorage{Storage: memory.New()}
	batching := completionServer(t, backend)
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	first := completionWriteCall(t.Context(), mode, completionPut("first", 1))
	second := completionWriteCall(t.Context(), mode, completionPut("second", 1))
	limit := batching.server.estimateWriteExecution(second.request, 1, 0).bytes
	batching.server.maxStoreBytes = map[string]int{"primary": limit}
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{first, second}
	batching.executeWrites(t.Context(), calls)
	assertCompletionWrites(t, calls)
	if backend.writes.Load() != 2 || batching.server.storeBytes["primary"] != 0 {
		t.Fatal("batch ignored store cap or leaked bytes")
	}
}

func TestReadBatchSplitsAtStoreByteLimit(t *testing.T) {
	backend := &syncCapacityStorage{Storage: memory.New()}
	batching := completionServer(t, backend)
	first := readCapacityCall(t.Context(), "first")
	second := readCapacityCall(t.Context(), "second")
	limit := 2*batching.server.maxReadBytes + second.request.SizeVT() + failureResponseBytes(1)
	batching.server.maxStoreBytes = map[string]int{"primary": limit}
	calls := []*batchCall[*sink.ReadRequest, *sink.ReadResponse]{first, second}
	batching.executeReads(t.Context(), calls)
	for _, call := range calls {
		result := awaitCompletion(t, call.result)
		if result.err != nil || len(result.response.GetResults()) != 1 {
			t.Fatalf("store-capped read lost result: %v, %v", result.response, result.err)
		}
	}
	if backend.reads.Load() != 2 || batching.server.storeBytes["primary"] != 0 {
		t.Fatal("read batch ignored store cap or leaked bytes")
	}
}

func TestStoreByteLimitDoesNotBlockOtherStoresOrPublishing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core := completionServer(t, memory.New()).server
		core.maxInFlightBytes = 100
		core.maxStoreBytes = map[string]int{"search": 60}
		core.storeBytes = map[string]int{"search": 0, "mongo": 0}
		busy := admissionRequest{encodedBytes: 60, stores: []string{"search"}}
		_, release, err := core.admitRequest(t.Context(), busy)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		waiting := admissionRequest{encodedBytes: 10, stores: []string{"search"}, wait: true}
		finished := make(chan error, 1)
		go func() { _, _, err := core.admitRequest(ctx, waiting); finished <- err }()
		synctest.Wait()
		scan := admissionRequest{encodedBytes: 40, stores: []string{"mongo"}, scan: true}
		_, releaseScan, err := core.admitRequest(t.Context(), scan)
		if err != nil {
			t.Fatalf("store-limited waiter reserved another store's free bytes: %v", err)
		}
		releaseScan()
		busy.publish = true
		_, releasePublish, err := core.admitRequest(t.Context(), busy)
		if err != nil {
			t.Fatalf("execution store cap leaked into publish pool: %v", err)
		}
		releasePublish()
		cancel()
		if err := <-finished; status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
		if core.storeBytes["search"] != 60 || core.storeBytes["mongo"] != 0 {
			t.Fatalf("incorrect store accounting: %v", core.storeBytes)
		}
	})
}

func TestCrossStoreReservationResizeChargesEveryStore(t *testing.T) {
	core := completionServer(t, memory.New()).server
	core.maxInFlightBytes = 100
	core.maxStoreBytes = map[string]int{"a": 60, "b": 50}
	core.storeBytes = map[string]int{"a": 0, "b": 0}
	reservation := &admissionReservation{}
	request := admissionRequest{encodedBytes: 40, stores: []string{"a", "b"}, reservation: reservation}
	_, release, err := core.admitRequest(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if core.inFlightBytes != 40 || core.storeBytes["a"] != 40 || core.storeBytes["b"] != 40 {
		t.Fatal("cross-store reservation was not charged conservatively")
	}
	if err := reservation.resize(10); err != nil {
		t.Fatal(err)
	}
	other := admissionRequest{encodedBytes: 30, stores: []string{"b"}}
	_, releaseOther, err := core.admitRequest(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.resize(40); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("resize bypassed a store byte limit: %v", err)
	}
	releaseOther()
	if err := reservation.resize(40); err != nil {
		t.Fatal(err)
	}
	release()
	if core.inFlightBytes != 0 || core.storeBytes["a"] != 0 || core.storeBytes["b"] != 0 {
		t.Fatal("resized cross-store reservation leaked capacity")
	}
	oversized := admissionRequest{encodedBytes: 51, stores: []string{"a", "b"}, wait: true, scan: true}
	_, _, err = core.admitRequest(t.Context(), oversized)
	if status.Code(err) != codes.ResourceExhausted || len(status.Convert(err).Details()) != 0 {
		t.Fatalf("impossible per-store reservation waited or advertised retry: %v", err)
	}
}
