package service

import (
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage/memory"
)

func TestWriteBatchSplitsAtExecutionByteLimit(t *testing.T) {
	backend := &syncCapacityStorage{Storage: memory.New()}
	batching := completionServer(t, backend)
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	first := completionWriteCall(t.Context(), mode, completionPut("first", 1))
	second := completionWriteCall(t.Context(), mode, completionPut("second", 1))
	limit := batching.server.estimateWriteExecution(second.request, 1, 0).bytes
	batching.server.maxInFlightBytes = limit
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{first, second}
	batching.executeWrites(t.Context(), calls)
	assertCompletionWrites(t, calls)
	if backend.writes.Load() != 2 || batching.server.inFlightBytes != 0 {
		t.Fatal("batch ignored store cap or leaked bytes")
	}
}

func TestReadBatchSplitsAtExecutionByteLimit(t *testing.T) {
	backend := &syncCapacityStorage{Storage: memory.New()}
	batching := completionServer(t, backend)
	first := readCapacityCall(t.Context(), "first")
	second := readCapacityCall(t.Context(), "second")
	limit := 2*batching.server.maxReadBytes + second.request.SizeVT() + failureResponseBytes(1)
	batching.server.maxInFlightBytes = limit
	calls := []*batchCall[*sink.ReadRequest, *sink.ReadResponse]{first, second}
	batching.executeReads(t.Context(), calls)
	for _, call := range calls {
		result := awaitCompletion(t, call.result)
		if result.err != nil || len(result.response.GetResults()) != 1 {
			t.Fatalf("store-capped read lost result: %v, %v", result.response, result.err)
		}
	}
	if backend.reads.Load() != 2 || batching.server.inFlightBytes != 0 {
		t.Fatal("read batch ignored store cap or leaked bytes")
	}
}
