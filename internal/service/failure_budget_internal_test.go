package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
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

func TestTinyFailureBudgetsKeepNonemptyMessages(t *testing.T) {
	backend := failureBudgetStorage{Storage: memory.New()}
	server := completionServer(t, backend)
	server.server.maxReadBytes = 1
	operation := &sink.ReadOperation{Address: completionAddress("key")}
	request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation, operation}}
	read, err := server.server.Read(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range read.Results {
		if result.GetFailure().GetMessage() == "" {
			t.Fatal("tiny read budget returned a protocol-invalid failure")
		}
	}
	deleteOperation := &sink.DeleteOperation{Address: completionAddress("key")}
	deleteRequest := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.DeleteOperation{deleteOperation, deleteOperation}}
	deleted, err := server.server.Delete(t.Context(), deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range deleted.Results {
		if result.GetFailure().GetMessage() == "" {
			t.Fatal("tiny delete budget returned a protocol-invalid failure")
		}
	}
	invalid := completionPut("key", 1)
	invalid.GetPut().Document = nil
	call := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, invalid, invalid)
	server.executeWrites(t.Context(), []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{call})
	written := awaitCompletion(t, call.result)
	if written.err != nil {
		t.Fatal(written.err)
	}
	for _, result := range written.response.Results {
		if result.GetFailure().GetMessage() == "" {
			t.Fatal("tiny write budget returned a protocol-invalid failure")
		}
	}
	failure := newFailure(sink.FailureCode_FAILURE_CODE_INTERNAL, errors.New(""), false)
	if failure.Message == "" {
		t.Fatal("empty backend error returned a protocol-invalid failure")
	}
}

func TestDuplicateReadBudgetFailureKeepsProtocolMessage(t *testing.T) {
	server := completionServer(t, memory.New())
	server.server.maxReadBytes = 256
	seed := completionWriteCall(t.Context(), sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, completionPut("key", 1))
	seeded, err := server.server.Write(t.Context(), seed.request)
	if err != nil || seeded.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("seed: %v %v", seeded, err)
	}
	operation := &sink.ReadOperation{Address: completionAddress("key")}
	request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation, operation}}
	call := &batchCall[*sink.ReadRequest, *sink.ReadResponse]{ctx: t.Context(), request: request, operationCount: 2, result: make(chan batchResult[*sink.ReadResponse], 1)}
	server.executeReads(t.Context(), []*batchCall[*sink.ReadRequest, *sink.ReadResponse]{call})
	read := awaitCompletion(t, call.result)
	if read.err != nil || len(read.response.GetResults()) != 2 {
		t.Fatalf("read: %+v", read)
	}
	failure := read.response.Results[1].GetFailure()
	if read.response.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND || failure.GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED || failure.GetMessage() == "" {
		t.Fatalf("budget failure invalidates successful sibling for SDK clients: %v", read.response)
	}
}
