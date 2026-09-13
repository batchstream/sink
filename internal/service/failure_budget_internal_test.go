package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failureBudgetStorage struct {
	storage.Storage
}

func (s failureBudgetStorage) Read(_ context.Context, request storage.ReadRequest) (storage.ReadResponse, error) {
	response := storage.ReadResponse{Results: make([]storage.ReadResult, len(request.Operations))}
	for index := range response.Results {
		response.Results[index].Status = storage.ReadStatusFailed
		response.Results[index].Err = errors.New(strings.Repeat("错\xff误", 1000))
	}
	return response, nil
}

func (s failureBudgetStorage) Delete(_ context.Context, request storage.DeleteRequest) (storage.DeleteResponse, error) {
	response := storage.DeleteResponse{Results: make([]storage.DeleteResult, len(request.Operations))}
	for index := range response.Results {
		response.Results[index].Status = storage.DeleteStatusFailed
		response.Results[index].Err = errors.New(strings.Repeat("错\xff误", 1000))
	}
	return response, nil
}

func TestCoalescedFailureBudgetsBelongToOriginalRPC(t *testing.T) {
	backend := failureBudgetStorage{Storage: memory.New()}
	server := completionServer(t, backend)
	server.server.maxReadBytes = 1024
	var reads []*batchCall[*sink.ReadRequest, *sink.ReadResponse]
	var deletes []*batchCall[*sink.DeleteRequest, *sink.DeleteResponse]
	for range 12 {
		operation := &sink.ReadOperation{Address: completionAddress("hot")}
		request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
		call := &batchCall[*sink.ReadRequest, *sink.ReadResponse]{ctx: t.Context(), request: request, operationCount: 1, result: make(chan batchResult[*sink.ReadResponse], 1)}
		reads = append(reads, call)
		deletes = append(deletes, completionDeleteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, "hot"))
	}
	server.executeReads(t.Context(), reads)
	server.executeDeletes(t.Context(), deletes)
	for _, call := range reads {
		result := awaitCompletion(t, call.result)
		if result.err != nil {
			t.Fatal(result.err)
		}
		failure := result.response.Results[0].GetFailure()
		if len(failure.GetMessage()) < 800 || result.response.SizeVT() > 1024 || !utf8.ValidString(failure.GetMessage()) {
			t.Fatalf("read lost its original caller budget: %v", result.response)
		}
	}
	for _, call := range deletes {
		result := awaitCompletion(t, call.result)
		if result.err != nil {
			t.Fatal(result.err)
		}
		failure := result.response.Results[0].GetFailure()
		if len(failure.GetMessage()) < 800 || result.response.SizeVT() > 1024 || !utf8.ValidString(failure.GetMessage()) {
			t.Fatalf("delete lost its original caller budget: %v", result.response)
		}
	}
}

func TestFailureSpaceIsReservedBeforeMutation(t *testing.T) {
	backend := &completionStorage{Storage: memory.New(), events: make(chan completionEvent, 8)}
	server := completionServer(t, backend).server
	operation := completionPut("key", 1)
	request := &sink.WriteRequest{Operations: []*sink.WriteOperation{operation}, CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
	server.maxInFlightBytes = request.SizeVT() + 100
	_, err := server.Write(t.Context(), request)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("write ignored failure reservation: %v", err)
	}
	deleteOperation := &sink.DeleteOperation{Address: completionAddress("key")}
	deleteRequest := &sink.DeleteRequest{Operations: []*sink.DeleteOperation{deleteOperation}, CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
	_, err = server.Delete(t.Context(), deleteRequest)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("delete ignored failure reservation: %v", err)
	}
	if len(backend.events) != 0 {
		t.Fatal("mutation reached storage without response reservation")
	}
}
