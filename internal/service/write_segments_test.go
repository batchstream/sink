package service_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/storage/memory"
)

func TestReturnedWriteFoldsSurroundingSegments(t *testing.T) {
	cases := []struct {
		name     string
		returned []int
		commits  int64
	}{
		{name: "middle", returned: []int{3}, commits: 3},
		{name: "first", returned: []int{0}, commits: 2},
		{name: "last", returned: []int{6}, commits: 2},
		{name: "adjacent", returned: []int{2, 3}, commits: 4},
		{name: "separated", returned: []int{1, 4}, commits: 5},
	}
	for _, method := range []string{"upsert", "merge"} {
		for _, test := range cases {
			t.Run(method+"/"+test.name, func(t *testing.T) {
				backend := &foldingBenchmarkStorage{Storage: memory.New()}
				server := newTestServer(t, backend, nil)
				operations := make([]*sink.WriteOperation, 7)
				for index := range operations {
					operation := foldingPut("hot", sink.WriteMode_WRITE_MODE_UPSERT, index+1)
					if method == "merge" {
						operation = foldingMerge("hot", incrementLua, `{"value":1}`)
					}
					operations[index] = operation
				}
				for _, index := range test.returned {
					operations[index].ReturnDocument = true
				}
				response, err := server.Write(t.Context(), foldingRequest(operations...))
				if err != nil {
					t.Fatal(err)
				}
				for index, result := range response.Results {
					if result.GetOperationIndex() != uint32(index) || result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
						t.Fatalf("operation %d: %v", index, result)
					}
					if (result.GetDocument() != nil) != slices.Contains(test.returned, index) {
						t.Fatalf("unexpected returned document for operation %d", index)
					}
				}
				for _, index := range test.returned {
					if value := returnedCounter(t, response.Results[index]); value != index+1 {
						t.Fatalf("returned value = %d, want %d", value, index+1)
					}
				}
				if writes := backend.writes.Load(); writes != test.commits {
					t.Fatalf("committed %d documents, want %d", writes, test.commits)
				}
				if method == "upsert" && backend.reads.Load() != 0 {
					t.Fatal("upsert segments acquired snapshots")
				}
				if value := foldingValue(t, backend, "hot"); value != 7 {
					t.Fatalf("final value = %d, want 7", value)
				}
			})
		}
	}
}

func BenchmarkReturnedWriteSegments(b *testing.B) {
	for _, delay := range []time.Duration{0, time.Millisecond} {
		b.Run(fmt.Sprintf("io=%s", delay), func(b *testing.B) {
			backend := &foldingBenchmarkStorage{Storage: memory.New(), delay: delay}
			server := newTestServer(b, backend, nil)
			operations := make([]*sink.WriteOperation, 64)
			for index := range operations {
				operations[index] = foldingPut("hot", sink.WriteMode_WRITE_MODE_UPSERT, index)
			}
			operations[32].ReturnDocument = true
			request := foldingRequest(operations...)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				response, err := server.Write(b.Context(), request)
				if err != nil {
					b.Fatal(err)
				}
				for _, result := range response.Results {
					if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
						b.Fatal(result)
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(backend.writes.Load())/float64(b.N), "writes/batch")
		})
	}
}
