package gateway

import (
	"strings"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/config"
)

func TestRoutesRejectDuplicateStoreNames(t *testing.T) {
	loaded, err := config.Decode(strings.NewReader("mode: gateway\ngateway:\n  routes:\n    - store: a\n      target: 127.0.0.1:1\n      tls: {insecure: true}\n    - store: a\n      target: 127.0.0.1:2\n      tls: {insecure: true}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateRoutes(loaded.Gateway.Routes); err == nil || !strings.Contains(err.Error(), "duplicate Store") {
		t.Fatalf("duplicate name accepted: %v", err)
	}
}

func TestInvalidEngineResultsCannotOverwriteKnownSuccess(t *testing.T) {
	good := &sink.WriteResult{OperationIndex: 0, Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
	cases := [][]*sink.WriteResult{
		{nil},
		{{OperationIndex: 0, Status: sink.WriteStatus_WRITE_STATUS_UNSPECIFIED}},
		{{OperationIndex: 1, Status: sink.WriteStatus_WRITE_STATUS_APPLIED}},
		{good, good},
	}
	for _, source := range cases {
		destination := []*sink.WriteResult{good, nil}
		if err := mergeResults(destination, source, []int{1}); err == nil {
			t.Fatal("invalid response accepted")
		}
		if destination[0] != good || destination[1] != nil {
			t.Fatal("validation changed a known result")
		}
	}
}
