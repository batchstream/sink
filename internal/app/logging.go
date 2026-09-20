package app

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/batchstream/sink/gen/forward"
	sink "github.com/batchstream/sink/gen/sink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func logUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	started := time.Now()
	response, err := next(ctx, req)
	method := diagnosticMethod(info.FullMethod)
	if method != "" {
		var summary rpcSummary
		summary.observe(response)
		summary.log(method, status.Code(err), time.Since(started))
	}
	return response, err
}

func logStream(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	method := diagnosticMethod(info.FullMethod)
	if method == "" {
		return next(server, stream)
	}
	started := time.Now()
	observed := &diagnosticStream{ServerStream: stream}
	err := next(server, observed)
	observed.summary.log(method, status.Code(err), time.Since(started))
	return err
}

// Keep counters only; streamed documents are released after SendMsg returns.
type diagnosticStream struct {
	grpc.ServerStream
	summary rpcSummary
}

func (s *diagnosticStream) SendMsg(message any) error {
	if err := s.ServerStream.SendMsg(message); err != nil {
		return err
	}
	s.summary.observe(message)
	return nil
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

type rpcSummary struct {
	operations  int
	failed      int
	failureCode string
}

func (s *rpcSummary) observe(response any) {
	observe := func(failure *sink.Failure) {
		s.operations++
		if failure != nil {
			s.failed++
			if s.failureCode == "" {
				s.failureCode = failure.GetCode().String()
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
		s.operations++
		if !result.GetSuccess() {
			s.failed++
			if s.failureCode == "" {
				s.failureCode = "native_execution_failed"
			}
		}
	case *forward.ForwardResponse:
		s.observe(result.GetResponse())
	case *forward.ForwardResponse_Read:
		s.observe(result.Read)
	case *forward.ForwardResponse_Write:
		s.observe(result.Write)
	case *forward.ForwardResponse_Delete:
		s.observe(result.Delete)
	case *forward.ForwardResponse_Execute:
		s.observe(result.Execute)
	}
}

func (s *rpcSummary) log(method string, code codes.Code, elapsed time.Duration) {
	level := slog.LevelDebug
	if code != codes.OK || s.failed > 0 || elapsed > 5*time.Second {
		level = slog.LevelWarn
	}
	if code == codes.Internal || code == codes.DataLoss {
		level = slog.LevelError
	}
	slog.Log(context.Background(), level, "RPC completed", "component", "rpc", "event", "rpc_completed", "method", method,
		"status", code.String(), "operations", s.operations, "failed", s.failed, "error_code", s.failureCode, "duration_ms", elapsed.Milliseconds())
}
