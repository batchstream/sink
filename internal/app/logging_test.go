package app

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCDiagnosticsCaptureApplicationFailuresWithoutPayload(t *testing.T) {
	loaded, err := config.Decode(strings.NewReader("mode: engine\nstorage:\n  name: primary\n  driver: mongodb\n  mongodb:\n    uri: mongodb://localhost:27017\n"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	opts := logging.Options{Config: loaded.Logging, Role: "engine", Store: "primary", Version: "test", Stderr: &output}
	runtime, err := logging.New(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	previous := slog.Default()
	slog.SetDefault(runtime.Logger)
	defer slog.SetDefault(previous)
	failure := &sink.Failure{Code: sink.FailureCode_FAILURE_CODE_UNAVAILABLE, Message: "private document error"}
	result := &sink.WriteResult{Status: sink.WriteStatus_WRITE_STATUS_FAILED, Failure: failure}
	write := &sink.WriteResponse{Results: []*sink.WriteResult{result}}
	wrapped := &forward.ForwardResponse_Write{Write: write}
	envelope := &forward.ForwardResponse{Response: wrapped}
	native := &sink.ExecuteResponse{Success: false, Payload: []byte("private response")}
	tests := []struct {
		name     string
		response any
		err      error
		want     string
	}{
		{name: "partial failure", response: write, want: "warn"},
		{name: "forwarded failure", response: envelope, want: "warn"},
		{name: "native failure", response: native, want: "warn"},
		{name: "transport failure", err: status.Error(codes.Internal, "private error"), want: "error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output.Reset()
			info := &grpc.UnaryServerInfo{FullMethod: "/sink.v1.Sink/Write"}
			next := func(context.Context, any) (any, error) { return test.response, test.err }
			response, err := logUnary(t.Context(), nil, info, next)
			if response != test.response || err != test.err {
				t.Fatal("logging changed RPC outcome")
			}
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record["level"] != test.want || record["event"] != "rpc_completed" || strings.Contains(output.String(), "private") {
				t.Fatalf("incorrect failure diagnostic: %s", output.String())
			}
			if test.response != nil && record["failed"] != "1" {
				t.Fatal("gRPC OK hid operation failure")
			}
		})
	}
	output.Reset()
	health := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	next := func(context.Context, any) (any, error) { return nil, nil }
	_, _ = logUnary(t.Context(), nil, health, next)
	if output.Len() != 0 {
		t.Fatal("health check logged")
	}
}
