package gateway

import (
	"testing"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResponseBudgetComesFromTransportAndSharesOnlyReturns(t *testing.T) {
	request := config.Request{MaxReadBytes: 4096}
	server := &Server{request: request}
	grant, err := server.responseBudget(2)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Returns != 4096-2*1280 {
		t.Fatalf("unreserved response envelope: %v", grant)
	}
	used := &forward.Budget{Returns: 100}
	consume(grant, used)
	if grant.Returns != 4096-2*1280-100 {
		t.Fatalf("settlement charged unrelated Store working sets: %v", grant)
	}
	if _, err := server.responseBudget(4); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("impossible response must fail before forwarding: %v", err)
	}
	native, err := server.responseBudget(0)
	if err != nil || native.GetReturns() != 4096 {
		t.Fatalf("native encoded response: %v %v", native, err)
	}
}

func TestParallelWritesOnlySerializeReturnedDocuments(t *testing.T) {
	for _, mode := range []sink.WriteMode{sink.WriteMode_WRITE_MODE_UPSERT, sink.WriteMode_WRITE_MODE_CREATE, sink.WriteMode_WRITE_MODE_REPLACE} {
		put := &sink.PutOperation{Mode: mode}
		action := &sink.WriteOperation_Put{Put: put}
		operation := &sink.WriteOperation{Action: action}
		write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
		body := &forward.ForwardRequest_Write{Write: write}
		request := &forward.ForwardRequest{Request: body}
		if !parallelBatch(request) {
			t.Fatal("write without returned documents unnecessarily serialized")
		}
		merge := &sink.MergeOperation{}
		operation.Action = &sink.WriteOperation_Merge{Merge: merge}
		if !parallelBatch(request) {
			t.Fatal("Merge unnecessarily serialized by removed intermediate budgets")
		}
		operation.ReturnDocument = true
		if parallelBatch(request) {
			t.Fatal("returned documents lost their shared response allowance")
		}
	}
}
