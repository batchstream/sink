package app

import (
	"context"
	"log/slog"
	"strings"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func logUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	started := time.Now()
	response, err := next(ctx, req)
	method := diagnosticMethod(info.FullMethod)
	if method != "" {
		code := status.Code(err)
		observed := response

		logRPC(method, code, observed, time.Since(started))
	}
	return response, err
}

func logStream(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	started := time.Now()
	err := next(server, stream)
	if method := diagnosticMethod(info.FullMethod); method != "" {
		logRPC(method, status.Code(err), nil, time.Since(started))
	}
	return err
}

func diagnosticMethod(full string) string {
	if !strings.HasPrefix(full, "/sink.v1.Sink/") && !strings.HasPrefix(full, "/sink.forward.v1.Engine/") {
		return ""
	}
	method := full[strings.LastIndexByte(full, '/')+1:]
	switch method {
	case "Read", "Write", "Delete", "Execute", "Query", "Count", "Scan", "Forward":
		return method
	default:
		return ""
	}
}

func logRPC(method string, code codes.Code, response any, elapsed time.Duration) {
	failed := 0
	operations := 0
	failureCode := ""
	observe := func(failure *sink.Failure) {
		operations++
		if failure != nil {
			failed++
			if failureCode == "" {
				failureCode = failure.GetCode().String()
			}
		}
	}
	switch result := response.(type) {
	case *sink.ReadResponse:
		for _, item := range result.GetResults() {
			observe(item.GetFailure())
		}
	case *sink.WriteResponse:
		for _, item := range result.GetResults() {
			observe(item.GetFailure())
		}
	case *sink.DeleteResponse:
		for _, item := range result.GetResults() {
			observe(item.GetFailure())
		}
	case *sink.ExecuteResponse:
		operations = 1
		if !result.GetSuccess() {
			failed = 1
			failureCode = "native_execution_failed"
		}
	}
	level := slog.LevelDebug
	if code != codes.OK || failed > 0 || elapsed > 5*time.Second {
		level = slog.LevelWarn
	}
	if code == codes.Internal || code == codes.DataLoss {
		level = slog.LevelError
	}
	slog.Log(context.Background(), level, "RPC completed", "component", "rpc", "event", "rpc_completed", "method", method,
		"status", code.String(), "operations", operations, "failed", failed, "error_code", failureCode, "duration_ms", elapsed.Milliseconds())
}
