package app

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/gen/forward"
	"github.com/batchstream/sink/internal/config"
	"github.com/batchstream/sink/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCDiagnosticsCaptureApplicationFailuresWithoutPayload(t *testing.T) {
	loaded, err := config.Decode(strings.NewReader("mode: engine\n"), strings.NewReader("name: primary\nstorage:\n  driver: mongodb\n  mongodb:\n    uri: mongodb://localhost:27017\n"))
	if err != nil {
		t.Fatal(err)
	}
	loaded.Logging.Console.Format = "json"
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
	native := &sink.ExecuteResponse{Success: false, Payload: []byte("private response")}
	tests := []struct {
		name     string
		response any
		err      error
		want     string
	}{
		{name: "partial failure", response: write, want: "warn"},
		{name: "managed failure", response: write, want: "warn"},
		{name: "managed native failure", response: native, want: "warn"},
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

func TestStreamDiagnosticsPreserveOutcomeAndExcludeHealth(t *testing.T) {
	loaded, err := config.Decode(strings.NewReader("mode: gateway\nforwarding:\n  routes: [{store: primary, target: '127.0.0.1:8080', tls: {insecure: true}}]\nlogging:\n  level: debug\n"), nil)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Logging.Console.Format = "json"
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
	cases := []struct {
		method string
		err    error
		level  string
	}{
		{method: "/sink.forward.v1.Engine/Forward", level: "debug"},
		{method: "/sink.forward.v1.Engine/Forward", err: status.Error(codes.Canceled, "private request"), level: "warn"},
		{method: "/sink.forward.v1.Engine/Forward", err: status.Error(codes.DataLoss, "private response"), level: "error"},
		{method: "/grpc.health.v1.Health/Watch"},
		{method: "/sink.forward.v1.Engine/Unknown"},
	}
	for _, test := range cases {
		output.Reset()
		called := false
		next := func(server any, stream grpc.ServerStream) error {
			called = true
			if server != runtime {
				t.Fatal("interceptor changed server")
			}
			if test.level == "" {
				if stream != nil {
					t.Fatal("interceptor wrapped an unrelated stream")
				}
			} else if observed, ok := stream.(*diagnosticStream); !ok || observed.ServerStream != nil {
				t.Fatal("interceptor lost the underlying stream")
			}
			return test.err
		}
		info := &grpc.StreamServerInfo{FullMethod: test.method}
		got := logStream(runtime, nil, info, next)
		if got != test.err || !called {
			t.Fatalf("stream outcome changed: %v", got)
		}
		if test.level == "" {
			if output.Len() != 0 {
				t.Fatalf("unexpected diagnostic for %s: %s", test.method, output.String())
			}
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record["method"] != "Forward" || record["status"] != status.Code(test.err).String() || record["level"] != test.level || strings.Contains(output.String(), "private") {
			t.Fatalf("wrong stream diagnostic: %s", output.String())
		}
	}
}

func TestStreamDiagnosticsCountResultsAcrossFramesWithoutPayload(t *testing.T) {
	var output bytes.Buffer
	options := &slog.HandlerOptions{Level: slog.LevelDebug}
	logger := slog.New(slog.NewJSONHandler(&output, options))
	previous := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(previous)
	failure := &sink.Failure{Code: sink.FailureCode_FAILURE_CODE_UNAVAILABLE, Message: "private failure"}
	document := &sink.Document{Payload: []byte("private document")}
	read := &sink.ReadResponse{Results: []*sink.ReadResult{{Document: document}, {Failure: failure}}}
	write := &sink.WriteResponse{Results: []*sink.WriteResult{{Document: document}, {Failure: failure}}}
	deleted := &sink.DeleteResponse{Results: []*sink.DeleteResult{{}, {Failure: failure}}}
	executed := &sink.ExecuteResponse{Payload: []byte("private document")}
	forwardRead := &forward.ForwardResponse{Response: &forward.ForwardResponse_Read{Read: read}}
	forwardWrite := &forward.ForwardResponse{Response: &forward.ForwardResponse_Write{Write: write}}
	forwardDelete := &forward.ForwardResponse{Response: &forward.ForwardResponse_Delete{Delete: deleted}}
	forwardExecute := &forward.ForwardResponse{Response: &forward.ForwardResponse_Execute{Execute: executed}}
	cases := []struct {
		method     string
		message    any
		operations int
	}{
		{method: "Read", message: read, operations: 4},
		{method: "Write", message: write, operations: 4},
		{method: "Delete", message: deleted, operations: 4},
		{method: "Forward", message: forwardRead, operations: 4},
		{method: "Forward", message: forwardWrite, operations: 4},
		{method: "Forward", message: forwardDelete, operations: 4},
		{method: "Forward", message: forwardExecute, operations: 2},
	}
	for _, test := range cases {
		t.Run(test.method, func(t *testing.T) {
			output.Reset()
			stream := &diagnosticTestStream{}
			prefix := "/sink.v1.Sink/"
			if test.method == "Forward" {
				prefix = "/sink.forward.v1.Engine/"
			}
			info := &grpc.StreamServerInfo{FullMethod: prefix + test.method}
			next := func(_ any, observed grpc.ServerStream) error {
				for range 2 {
					if err := observed.SendMsg(test.message); err != nil {
						return err
					}
				}
				return nil
			}
			err := logStream(nil, stream, info, next)
			if err != nil || stream.sent != 2 {
				t.Fatalf("logging changed delivery: sent=%d err=%v", stream.sent, err)
			}
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record["level"] != "WARN" || record["failed"] != float64(2) || record["operations"] != float64(test.operations) || strings.Contains(output.String(), "private") {
				t.Fatalf("incorrect stream summary: %s", output.String())
			}
		})
	}
}

func TestStreamDiagnosticsPreserveSendError(t *testing.T) {
	failure := status.Error(codes.Canceled, "client disconnected")
	underlying := &diagnosticTestStream{err: failure}
	stream := &diagnosticStream{ServerStream: underlying}
	response := &sink.WriteResponse{Results: []*sink.WriteResult{{}}}
	if err := stream.SendMsg(response); err != failure || stream.summary.operations != 0 {
		t.Fatalf("failed send counted or changed: summary=%+v err=%v", stream.summary, err)
	}
}

type diagnosticTestStream struct {
	grpc.ServerStream
	sent int
	err  error
}

func (s *diagnosticTestStream) SendMsg(any) error {
	if s.err != nil {
		return s.err
	}
	s.sent++
	return nil
}
