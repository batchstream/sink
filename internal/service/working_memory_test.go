package service

import (
	"fmt"
	"strings"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage/memory"
)

func TestCollectedMergesUseProcessMemoryInsteadOfDocumentQuotas(t *testing.T) {
	for _, batched := range []bool{false, true} {
		t.Run(fmt.Sprint(batched), func(t *testing.T) {
			backend := &syncCapacityStorage{Storage: memory.New()}
			server := completionServer(t, backend)
			server.server.maxReadBytes = 1024
			var operations []*sink.WriteOperation
			for index := range 8 {
				operation := completionMerge(fmt.Sprint(index), 1)
				operation.GetMerge().IncomingDocument.Payload = []byte(`{"value":1,"padding":"` + strings.Repeat("x", 4096) + `"}`)
				operation.GetMerge().LuaProgram.Source = []byte(`return function(current, incoming) return incoming end`)
				operations = append(operations, operation)
			}
			// Both the first outputs and the second round's snapshots/inputs
			// exceed the response limit, including within one original RPC.
			for range 2 {
				if batched {
					var calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]
					for start := 0; start < len(operations); start += 2 {
						call := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, operations[start:start+2]...)
						calls = append(calls, call)
					}
					server.executeWrites(t.Context(), calls)
					assertCompletionWrites(t, calls)
				} else {
					request := &sink.WriteRequest{Operations: operations, CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
					response, err := server.server.Write(t.Context(), request)
					if err != nil {
						t.Fatal(err)
					}
					for _, result := range response.Results {
						if result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
							t.Fatal(result)
						}
					}
				}
			}
			if backend.reads.Load() != 2 || backend.writes.Load() != 2 {
				t.Fatalf("collected operations were split: reads=%d writes=%d", backend.reads.Load(), backend.writes.Load())
			}
		})
	}
}
