package protocol

import (
	"context"
	"errors"
	"strings"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// MemoryStats owns the RPC reference until gRPC finishes it. Batch producers
// and encoded transport buffers keep independent references past cancellation.
type MemoryStats struct {
	Pool    *capacity.Pool
	Timeout time.Duration
}

type memoryCancelKey struct{}

func (h *MemoryStats) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if strings.HasPrefix(info.FullMethodName, "/sink.") {
		if h.Timeout > 0 {
			timed, cancel := context.WithTimeout(ctx, h.Timeout)
			ctx = context.WithValue(timed, memoryCancelKey{}, cancel)
		}
		return capacity.WithScope(ctx, h.Pool.NewScope())
	}
	return ctx
}

func (*MemoryStats) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if _, ok := event.(*stats.End); ok {
		capacity.FromContext(ctx).Release()
		if cancel, ok := ctx.Value(memoryCancelKey{}).(context.CancelFunc); ok {
			cancel()
		}
	}
}
func (*MemoryStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (*MemoryStats) HandleConn(context.Context, stats.ConnStats)                       {}

// ManagedMessage carries memory ownership only inside the local gRPC stack;
// its protobuf representation and the public API are unchanged.
type ManagedMessage struct {
	Message any
	Context context.Context
}

func MemoryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	scope := capacity.FromContext(ctx)
	if scope == nil {
		return handler(ctx, req)
	}
	if sized, ok := req.(interface{ SizeVT() int }); ok {
		if err := scope.Admit(ctx, 2*sized.SizeVT()+384*operationCount(req)+1024); err != nil {
			return nil, MemoryAdmissionError(req, err)
		}
	}
	if err := scope.AdmitCompletion(ctx, CompletionBytes(req)); err != nil {
		return nil, MemoryAdmissionError(req, err)
	}
	response, err := handler(ctx, req)
	if err != nil {
		return nil, err
	}
	if sized, ok := response.(interface{ SizeVT() int }); ok {
		if err := scope.EnsureOutput(ctx, sized.SizeVT()+1024); err != nil {
			return nil, err
		}
		wrapped := &ManagedMessage{Message: response, Context: ctx}
		return wrapped, nil
	}
	return response, nil
}

func operationCount(message any) int {
	switch req := message.(type) {
	case *sink.ReadRequest:
		return len(req.GetOperations())
	case *sink.WriteRequest:
		return len(req.GetOperations())
	case *sink.DeleteRequest:
		return len(req.GetOperations())
	case *forward.ForwardRequest:
		return len(req.GetRead().GetOperations()) + len(req.GetWrite().GetOperations()) + len(req.GetDelete().GetOperations())
	default:
		return 0
	}
}

// CompletionBytes covers bounded per-operation failures and protobuf envelopes.
// Document bytes are acquired once their actual sizes become known.
func CompletionBytes(message any) int { return 4096 + 1280*operationCount(message) }

// MemoryAdmissionError preserves the Scan SDK's safe retry signal, exclusively
// for temporary refusal before invoking the service or forwarding downstream.
func MemoryAdmissionError(message any, err error) error {
	if !errors.Is(err, capacity.ErrBusy) {
		return err
	}
	_, scan := message.(*sink.ScanRequest)
	if forwarded, ok := message.(*forward.ForwardRequest); ok {
		scan = forwarded.GetScan() != nil
	}
	if !scan {
		return err
	}
	detail := &errdetails.ErrorInfo{Domain: "sink", Reason: "SCAN_ADMISSION_REJECTED", Metadata: map[string]string{"pool": "memory", "reason": "busy"}}
	marked, detailErr := status.Convert(err).WithDetails(detail)
	if detailErr != nil {
		return err
	}
	return marked.Err()
}
