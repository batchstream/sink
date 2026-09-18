package gateway

import (
	"strings"
	"sync/atomic"
	"testing"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/forwarding"
	"google.golang.org/grpc"
)

type rejectionEngine struct {
	forward.UnimplementedEngineServer
	calls atomic.Int32
}

func (s *rejectionEngine) ForwardStream(req *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ResponseFrame]) error {
	s.calls.Add(1)
	results := make([]*sink.WriteResult, len(req.GetWrite().GetOperations()))
	for i := range results {
		result := &sink.WriteResult{OperationIndex: uint32(i), Status: sink.WriteStatus_WRITE_STATUS_APPLIED}
		results[i] = result
	}
	written := &sink.WriteResponse{Results: results}
	body := &forward.ForwardResponse_Write{Write: written}
	used := &forward.Budget{}
	response := &forward.ForwardResponse{Version: forwarding.Version, Store: req.GetStore(), Used: used, Response: body}
	data, err := response.MarshalVT()
	if err != nil {
		return err
	}
	header := &forward.ResponseFrame{Size: uint64(len(data))}
	if err := stream.Send(header); err != nil {
		return err
	}
	frame := &forward.ResponseFrame{Data: data}
	return stream.Send(frame)
}

func TestLocalScratchRejectionPreservesSafeRetry(t *testing.T) {
	backend := &rejectionEngine{}
	fixture := fixtureEngine{store: "primary", target: serveEngine(t, backend)}
	server := testGateway(t, 1<<20, fixture)
	pool := gatewayMemory(t, 256<<10)
	server.memory = pool
	busy := pool.NewOwner().NewLease()
	defer busy.Close()
	if err := busy.Grow(t.Context(), 64<<10, capacity.Request); err != nil {
		t.Fatal(err)
	}
	op := put("primary", "retry-local", false)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{op}}
	response, err := server.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	failure := response.GetResults()[0].GetFailure()
	rejectedCalls := backend.calls.Load()
	busy.Close()
	control, err := server.Write(t.Context(), request)
	if err != nil || control.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("control failed: %v %v", control, err)
	}
	t.Logf("downstreamCallsAtRejection=%d retryable=%v failure=%v successfulControlCalls=%d", rejectedCalls, failure.GetRetryable(), failure, backend.calls.Load())
	if rejectedCalls != 0 {
		t.Fatal("request unexpectedly reached Engine")
	}
	if !failure.GetRetryable() || strings.Contains(failure.GetMessage(), "unknown") {
		t.Fatalf("local pre-forward rejection was mislabeled as ambiguous mutation")
	}
}
