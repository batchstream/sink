package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/batchstream/sink/internal/testuri"
	"github.com/liran/sink-go/uri"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/queue"
)

type failingApplier struct {
	failure *sink.Failure
	calls   int
}

func (a *failingApplier) Write(_ context.Context, req *sink.WriteRequest) (*sink.WriteResponse, error) {
	a.calls++
	result := &sink.WriteResult{Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
	if a.calls == 1 {
		result.Status, result.Failure = sink.WriteStatus_WRITE_STATUS_FAILED, a.failure
	}
	response := &sink.WriteResponse{Results: []*sink.WriteResult{result}}
	return response, nil
}

func (a *failingApplier) Delete(context.Context, *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	return nil, errors.New("unexpected delete")
}

func TestUnknownFailuresRetainSameRecordBarrier(t *testing.T) {
	for _, code := range []sink.FailureCode{sink.FailureCode_FAILURE_CODE_UNSPECIFIED, sink.FailureCode_FAILURE_CODE_INTERNAL,
		sink.FailureCode_FAILURE_CODE_UNAVAILABLE, sink.FailureCode_FAILURE_CODE_DEADLINE_EXCEEDED,
		sink.FailureCode_FAILURE_CODE_CONFLICT, 99, -1} {
		t.Run(code.String(), func(t *testing.T) {
			failure := &sink.Failure{Code: code, Message: "unknown/environment failure", Retryable: false}
			assertFailureBarrier(t, failure, true)
		})
	}
	t.Run("missing details", func(t *testing.T) { assertFailureBarrier(t, nil, true) })
}

func TestConfirmedPermanentFailureReleasesSameRecordBarrier(t *testing.T) {
	for _, code := range []sink.FailureCode{sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT,
		sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED, sink.FailureCode_FAILURE_CODE_NOT_FOUND,
		sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED} {
		t.Run(code.String(), func(t *testing.T) {
			failure := &sink.Failure{Code: code, Message: "confirmed invalid record", Retryable: false}
			assertFailureBarrier(t, failure, false)
			failure.Retryable = true
			assertFailureBarrier(t, failure, true)
		})
	}
}

func assertFailureBarrier(t *testing.T, failure *sink.Failure, retain bool) {
	t.Helper()
	applier := &failingApplier{failure: failure}
	processor, err := NewProcessor(applier)
	if err != nil {
		t.Fatal(err)
	}
	key := uri.StringKey("same-record")
	address := &sink.RecordAddress{Uri: testuri.Record("primary", []string{"catalog", "records"}, key)}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{}`)}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	write := &sink.WriteOperation{Address: address, Action: &sink.WriteOperation_Put{Put: put}}
	mutation := queue.Mutation{Write: write}
	results := processor.HandleBatch(t.Context(), []queue.Mutation{mutation, mutation})
	var first *ApplyError
	if len(results) != 2 || !errors.As(results[0], &first) || first.Retryable() != retain {
		t.Fatalf("incorrect first failure: %v", results)
	}
	if retain {
		var following *ApplyError
		if applier.calls != 1 || !errors.As(results[1], &following) || !following.Retryable() {
			t.Fatalf("unknown failure lost same-record barrier: calls=%d results=%v", applier.calls, results)
		}
	} else if applier.calls != 2 || results[1] != nil {
		t.Fatalf("permanent failure blocked following valid record: calls=%d results=%v", applier.calls, results)
	}
}

type incompleteApplier struct {
	write   *sink.WriteResponse
	deleted *sink.DeleteResponse
}

func (a *incompleteApplier) Write(context.Context, *sink.WriteRequest) (*sink.WriteResponse, error) {
	return a.write, nil
}
func (a *incompleteApplier) Delete(context.Context, *sink.DeleteRequest) (*sink.DeleteResponse, error) {
	return a.deleted, nil
}

func TestIncompleteOrCanceledResultsRemainRetryable(t *testing.T) {
	if _, err := NewProcessor(nil); err == nil {
		t.Fatal("missing applier accepted")
	}
	key := uri.StringKey("record")
	address := &sink.RecordAddress{Uri: testuri.Record("primary", []string{"catalog", "records"}, key)}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: []byte(`{}`)}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	action := &sink.WriteOperation_Put{Put: put}
	write := &sink.WriteOperation{Address: address, Action: action}
	deleted := &sink.DeleteOperation{Address: address}
	writeMutation, deleteMutation := queue.Mutation{Write: write}, queue.Mutation{Delete: deleted}
	for _, canceled := range []bool{false, true} {
		applier := &incompleteApplier{}
		ctx, cancel := context.WithCancel(t.Context())
		if canceled {
			// A permanent-looking result received after cancellation cannot prove rejection.
			failure := &sink.Failure{Code: sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT, Retryable: false}
			written := &sink.WriteResult{Status: sink.WriteStatus_WRITE_STATUS_FAILED, Failure: failure}
			removed := &sink.DeleteResult{Status: sink.DeleteStatus_DELETE_STATUS_FAILED, Failure: failure}
			applier.write = &sink.WriteResponse{Results: []*sink.WriteResult{written}}
			applier.deleted = &sink.DeleteResponse{Results: []*sink.DeleteResult{removed}}
			cancel()
		}
		processor, err := NewProcessor(applier)
		if err != nil {
			t.Fatal(err)
		}
		for _, mutation := range []queue.Mutation{writeMutation, deleteMutation} {
			err := processor.Handle(ctx, mutation)
			var failure *ApplyError
			if !errors.As(err, &failure) || !failure.Retryable() {
				t.Fatalf("lost unresolved source record: %v", err)
			}
		}
		cancel()
	}
}
