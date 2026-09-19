package service

import (
	"fmt"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/storage/memory"
)

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
