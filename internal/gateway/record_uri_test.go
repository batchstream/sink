package gateway

import (
	"testing"

	sink "github.com/liran/sink/gen/sink"
)

func TestRootResourceDoesNotPoisonRecordBatch(t *testing.T) {
	backend := testEngine(t, "a", 4096)
	gateway := testGateway(t, 4096, backend)
	valid := put("a", "kept", false)
	invalid := put("a", "invalid", false)
	root := &sink.RecordAddress{Uri: "sink://a"}
	invalid.Address = root
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{valid, invalid}}
	response, err := gateway.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 2 || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || response.Results[1].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_INVALID_ARGUMENT {
		t.Fatalf("root resource affected a valid sibling: %+v", response)
	}
}
