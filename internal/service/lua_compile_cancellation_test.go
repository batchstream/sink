package service_test

import (
	"context"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteCancellationInterruptsLuaCompilation(t *testing.T) {
	for _, mode := range []sink.CompletionMode{
		sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED,
		sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			for _, canceled := range []bool{false, true} {
				name := "request deadline"
				if canceled {
					name = "caller cancellation"
				}
				t.Run(name, func(t *testing.T) {
					luaOptions := merge.LuaOptions{Timeout: time.Second, MaxInstructions: 1 << 60}
					engine, err := merge.NewLuaEngine(luaOptions)
					if err != nil {
						t.Fatal(err)
					}
					publisher := &recordingPublisher{}
					options := service.Options{BoundStore: "primary", Storage: memory.New(), Lua: engine, Publisher: publisher}
					server, err := service.New(options)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(t.Context())
					if !canceled {
						cancel()
						ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
					}
					defer cancel()
					wantCode := codes.DeadlineExceeded
					if canceled {
						timer := time.AfterFunc(20*time.Millisecond, cancel)
						defer timer.Stop()
						wantCode = codes.Canceled
					}
					operation := foldingMerge("compile", `while true do end; return function(current, incoming) return incoming end`, `{}`)
					request := foldingRequest(operation)
					request.CompletionMode = mode
					started := time.Now()
					response, err := server.Write(ctx, request)
					elapsed := time.Since(started)
					if status.Code(err) != wantCode || response != nil || elapsed > 500*time.Millisecond {
						t.Fatalf("compile ignored request cancellation: elapsed=%s response=%v error=%v", elapsed, response, err)
					}
					if publisher.mutationCount() != 0 {
						t.Fatal("canceled compilation published a mutation")
					}
				})
			}
		})
	}
}

func TestLuaCompileLimitDoesNotCancelSiblingWrites(t *testing.T) {
	luaOptions := merge.LuaOptions{Timeout: 20 * time.Millisecond, MaxInstructions: 1 << 60}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := service.Options{BoundStore: "primary", Storage: memory.New(), Lua: engine}
	server, err := service.New(options)
	if err != nil {
		t.Fatal(err)
	}
	limited := foldingMerge("compile", `while true do end; return function(current, incoming) return incoming end`, `{}`)
	healthy := foldingPut("healthy", sink.WriteMode_WRITE_MODE_UPSERT, 1)
	request := foldingRequest(limited, healthy)
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_FAILED ||
		response.Results[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT ||
		response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("individual compile timeout affected the request: %v", response)
	}
}
