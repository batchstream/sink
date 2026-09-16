package service_test

import (
	"context"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

type shrinkingCASStorage struct {
	storage.Storage
	attempts int
	reject   bool
}

func (s *shrinkingCASStorage) Write(ctx context.Context, request storage.WriteRequest) (storage.WriteResponse, error) {
	s.attempts++
	if s.attempts == 1 || (s.reject && request.Operations[0].Precondition.Kind == storage.PreconditionRevisionMatches) {
		response := storage.WriteResponse{Results: make([]storage.WriteResult, len(request.Operations))}
		for index := range response.Results {
			response.Results[index].Status = storage.WriteStatusPreconditionFailed
		}
		if !s.reject {
			operation := request.Operations[0]
			operation.Precondition = storage.Precondition{}
			operation.Document = storageJSONDocument(`{"value":"short"}`)
			concurrent := storage.WriteRequest{Operations: []storage.WriteOperation{operation}}
			if _, err := s.Storage.Write(ctx, concurrent); err != nil {
				return response, err
			}
		}
		return response, nil
	}
	return s.Storage.Write(ctx, request)
}

func TestReturningQuotaSettlesFinalCASOutcome(t *testing.T) {
	for _, batching := range []bool{false, true} {
		for _, reject := range []bool{false, true} {
			name := "smaller commit"
			if reject {
				name = "exhausted conflict"
			}
			if batching {
				name += " batched"
			}
			t.Run(name, func(t *testing.T) {
				store := memory.New()
				seedServer := newTestServer(t, store, nil)
				seed := putWriteOperation("same", strings.Repeat("x", 250))
				seedRequest := &sink.WriteRequest{Operations: []*sink.WriteOperation{seed}, CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
				seedResult, err := seedServer.Write(t.Context(), seedRequest)
				if err != nil || seedResult.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
					t.Fatalf("seed=%v err=%v", seedResult, err)
				}
				backend := &shrinkingCASStorage{Storage: store, reject: reject}
				luaOpts := merge.LuaOptions{}
				lua, err := merge.NewLuaEngine(luaOpts)
				if err != nil {
					t.Fatal(err)
				}
				opts := service.Options{BoundStore: "primary", Storage: backend, Lua: lua, MaxReadBytes: 400}
				core, err := service.New(opts)
				if err != nil {
					t.Fatal(err)
				}
				var server sink.SinkServer = core
				if batching {
					batchOpts := service.BatchingOptions{MaxWait: time.Millisecond}
					batch, err := service.NewBatchingServer(core, batchOpts)
					if err != nil {
						t.Fatal(err)
					}
					defer batch.Close()
					server = batch
				}
				request := mergeWriteRequestWithSource("same", "1", `return function(current, incoming) current.value = string.sub(current.value, 1, -2); return current end`)
				request.Operations[0].ReturnDocument = true
				second := putWriteOperation("same", strings.Repeat("y", 100))
				second.ReturnDocument = true
				request.Operations = append(request.Operations, second)
				response, err := server.Write(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				if reject {
					if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED || response.Results[0].Document != nil {
						t.Fatalf("failed CAS result=%v", response.Results[0])
					}
				} else if string(response.Results[0].GetDocument().GetPayload()) != `{"value":"shor"}` {
					t.Fatalf("returned speculative candidate: %v", response.Results[0])
				}
				if response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || response.Results[1].Document == nil {
					t.Fatalf("obsolete CAS candidate consumed return quota: %v", response)
				}
			})
		}
	}
}
