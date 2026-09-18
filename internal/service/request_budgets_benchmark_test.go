package service

import (
	"fmt"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

var benchmarkSnapshotBudget *storage.ReadBudget

func BenchmarkSharedSnapshotBudget(b *testing.B) {
	for _, callers := range []int{1, 128} {
		for _, operations := range []int{1, 128, 1024} {
			b.Run(fmt.Sprintf("callers=%d/operations=%d", callers, operations), func(b *testing.B) {
				budgets := make([]*storage.ReadBudget, callers)
				for i := range budgets {
					budgets[i] = storage.NewReadBudget(4096)
				}
				owners := make([]int, operations)
				for i := range owners {
					owners[i] = i % callers
				}
				b.ReportAllocs()
				for b.Loop() {
					benchmarkSnapshotBudget = sharedSnapshotBudget(owners, budgets)
				}
			})
		}
	}
}

func BenchmarkRepeatedRecordRead(b *testing.B) {
	for _, count := range []int{128, 1000} {
		b.Run(fmt.Sprintf("operations=%d", count), func(b *testing.B) {
			backend := memory.New()
			seedReadCapacity(b, backend, "shared", 128)
			luaOptions := merge.LuaOptions{}
			lua, err := merge.NewLuaEngine(luaOptions)
			if err != nil {
				b.Fatal(err)
			}
			opts := Options{Storage: backend, Lua: lua, BoundStore: "primary"}
			server, err := New(opts)
			if err != nil {
				b.Fatal(err)
			}
			request := &sink.ReadRequest{Operations: make([]*sink.ReadOperation, count)}
			for i := range request.Operations {
				operation := &sink.ReadOperation{Address: completionAddress("shared")}
				request.Operations[i] = operation
			}
			b.ReportAllocs()
			for b.Loop() {
				response, err := server.Read(b.Context(), request)
				if err != nil {
					b.Fatal(err)
				}
				for _, result := range response.Results {
					if result.Status != sink.ReadStatus_READ_STATUS_FOUND {
						b.Fatalf("read failed: %v", result)
					}
				}
			}
		})
	}
}
