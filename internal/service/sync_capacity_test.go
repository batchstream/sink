package service

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/batchstream/sink/internal/protocol"
	"github.com/batchstream/sink/internal/testuri"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
)

func TestSynchronousSmallMergesShareBoundedWorkingSet(t *testing.T) {
	backend := &syncCapacityStorage{Storage: memory.New()}
	server := completionServer(t, backend)
	var calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]
	for index := range 128 {
		operation := completionMerge(fmt.Sprintf("record-%d", index), 1)
		calls = append(calls, completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, operation))
	}
	server.executeWrites(t.Context(), calls)
	assertCompletionWrites(t, calls)
	if backend.reads.Load() != 1 || backend.writes.Load() != 1 {
		t.Fatalf("small records split by hypothetical document size: reads=%d writes=%d", backend.reads.Load(), backend.writes.Load())
	}
}

func TestReturnedPutsKeepCollectedBatch(t *testing.T) {
	backend := &syncCapacityStorage{Storage: memory.New()}
	server := completionServer(t, backend)
	calls := make([]*batchCall[*sink.WriteRequest, *sink.WriteResponse], 128)
	for index := range calls {
		operation := completionPut(fmt.Sprintf("returned-%d", index), index)
		operation.ReturnDocument = true
		calls[index] = completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, operation)
	}
	server.executeWrites(t.Context(), calls)
	for index, call := range calls {
		result := <-call.result
		want := fmt.Sprintf(`{"value":%d}`, index)
		if result.err != nil || result.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || string(result.response.Results[0].GetDocument().GetPayload()) != want {
			t.Fatalf("returned Put %d lost its document: %v, %v", index, result.response, result.err)
		}
	}
}

func TestSynchronousMergeProcessesCollectedSnapshotsAndOutputs(t *testing.T) {
	for _, scenario := range []string{"snapshots", "outputs", "conflicts", "returns"} {
		t.Run(scenario, func(t *testing.T) {
			memoryStore := memory.New()
			backend := &chunkConflictStorage{Storage: memoryStore, attempts: make(map[string]int)}
			if scenario == "conflicts" {
				backend.conflict = "record-0"
			}
			server := completionServer(t, backend)
			server.server.maxReadBytes = 1024

			var calls []*batchCall[*sink.WriteRequest, *sink.WriteResponse]
			for index := range 8 {
				key := fmt.Sprintf("record-%d", index)
				padding := ""
				if scenario != "outputs" {
					padding = strings.Repeat("x", 700)
				}
				address, err := protocol.ParseAddress(completionAddress(key))
				if err != nil {
					t.Fatal(err)
				}
				document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(fmt.Sprintf(`{"value":0,"padding":%q}`, padding))}
				seed := memory.SeedRequest{Address: address, Document: document}
				memoryStore.Seed(seed)
				operation := completionMerge(key, 1)
				operation.ReturnDocument = scenario == "returns"
				operation.GetMerge().LuaProgram.Source = []byte(fmt.Sprintf(`return function(current, incoming) return {value=current.value+incoming.value,padding=%q} end`, strings.Repeat("x", 700)))
				call := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, operation)
				calls = append(calls, call)
			}
			server.executeWrites(t.Context(), calls)
			if scenario == "returns" {
				for _, call := range calls {
					result := awaitCompletion(t, call.result)
					if result.err != nil || !strings.Contains(string(result.response.GetResults()[0].GetDocument().GetPayload()), `"value":1`) {
						t.Fatalf("chunk lost returned document: %v, %v", result.response, result.err)
					}
				}
			} else {
				assertCompletionWrites(t, calls)
			}
			for index := range 8 {
				key := fmt.Sprintf("record-%d", index)
				want := 1
				if key == backend.conflict {
					want = 2
				}
				if backend.attempts[key] != want {
					t.Fatalf("%s attempted %d times, want %d", key, backend.attempts[key], want)
				}
			}
			if backend.maxWriteBytes <= server.server.maxReadBytes {
				t.Fatalf("batch was split by the response byte limit: %d", backend.maxWriteBytes)
			}
		})
	}
}

func TestSynchronousMergesProcessEntireRequestAndSharedRecord(t *testing.T) {
	backend := memory.New()
	server := completionServer(t, backend)
	server.server.maxReadBytes = 400

	for _, key := range []string{"a", "b"} {
		address, err := protocol.ParseAddress(completionAddress(key))
		if err != nil {
			t.Fatal(err)
		}
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(fmt.Sprintf(`{"value":0,"padding":%q}`, strings.Repeat("x", 140)))}
		seed := memory.SeedRequest{Address: address, Document: document}
		backend.Seed(seed)
	}
	first := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionMerge("a", 10), completionMerge("b", 100))
	second := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionMerge("b", 1))
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{first, second}
	server.executeWrites(t.Context(), calls)
	firstResult := awaitCompletion(t, first.result)
	healthy := awaitCompletion(t, second.result)
	if firstResult.err != nil || healthy.err != nil {
		t.Fatalf("chunk errors: %v, %v", firstResult.err, healthy.err)
	}
	if firstResult.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || firstResult.response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("request was limited by an intermediate quota: %v", firstResult.response)
	}
	if healthy.response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatal(healthy.response)
	}
	address, err := protocol.ParseAddress(completionAddress("b"))
	if err != nil {
		t.Fatal(err)
	}
	operation := storage.ReadOperation{Address: address}
	request := storage.ReadRequest{Operations: []storage.ReadOperation{operation}}
	read, err := backend.Read(t.Context(), request)
	if err != nil || string(read.Results[0].Document.Payload) != `{"value":101}` {
		t.Fatalf("folded chain lost a caller: %v, %v", read, err)
	}
}

type chunkConflictStorage struct {
	storage.Storage
	conflict      string
	attempts      map[string]int
	maxWriteBytes int
}

func (s *chunkConflictStorage) Write(ctx context.Context, request storage.WriteRequest) (storage.WriteResponse, error) {
	response := storage.WriteResponse{Results: make([]storage.WriteResult, len(request.Operations))}
	bytes := 0
	for index, operation := range request.Operations {
		key := string(testuri.Key(operation.Address).Data)
		s.attempts[key]++
		bytes += len(operation.Document.Payload) + 128
		if key == s.conflict && s.attempts[key] == 1 {
			response.Results[index].Status = storage.WriteStatusPreconditionFailed
			continue
		}
		write := storage.WriteRequest{Operations: []storage.WriteOperation{operation}, WaitUntilVisible: request.WaitUntilVisible}
		stored, err := s.Storage.Write(ctx, write)
		if err != nil {
			return response, err
		}
		response.Results[index] = stored.Results[0]
	}
	s.maxWriteBytes = max(s.maxWriteBytes, bytes)
	return response, nil
}
