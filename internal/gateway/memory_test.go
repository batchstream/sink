package gateway

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/forwarding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func gatewayMemory(t *testing.T, size int64) *capacity.Pool {
	t.Helper()
	opts := capacity.Options{Bytes: size, BurstPercent: 10, WaitTimeout: time.Second}
	p, err := capacity.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDemandAdmissionDoesNotChargeMaximumResponses(t *testing.T) {
	fixture := fixtureEngine{store: "primary", target: "127.0.0.1:1"}
	server := testGateway(t, 32<<20, fixture)
	server.memory = gatewayMemory(t, 128<<20)
	server.config.MaxRequests = 1
	server.config.MaxRequestsPerStore = 1
	request := &sink.ReadRequest{}
	body := &forward.ForwardRequest_Read{Read: request}
	wrapped := &forward.ForwardRequest{Request: body}
	releases := make([]func(), 0, 128)
	for range 128 {
		_, release, err := server.begin(t.Context(), wrapped)
		if err != nil {
			t.Fatalf("tiny read was charged its 32 MiB maximum: %v", err)
		}
		releases = append(releases, release)
	}
	if used := server.memory.Used(); used >= 1<<20 {
		t.Fatalf("small requests consumed %d", used)
	}
	for _, release := range releases {
		release()
	}
	if server.memory.Used() != 0 {
		t.Fatal("admission leaked")
	}
}

func TestStreamedForwardingUsesActualSize(t *testing.T) {
	backend := testEngine(t, "primary", 32<<20)
	server := testGateway(t, 32<<20, backend)
	// Far below a single legacy 64 MiB speculative read reservation.
	server.memory = gatewayMemory(t, 8<<20)
	payload := []byte(`{"value":"` + strings.Repeat("x", 1<<20) + `"}`)
	op := put("primary", "large", true)
	op.GetPut().Document.Payload = payload
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{op}}
	result, err := server.Write(t.Context(), write)
	if err != nil || result.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("write: %v %v", result, err)
	}
	if !bytes.Equal(result.GetResults()[0].GetDocument().GetPayload(), payload) {
		t.Fatal("returned write payload changed")
	}
	readOp := &sink.ReadOperation{Address: address("primary", "large")}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{readOp}}
	response, err := server.Read(t.Context(), read)
	if err != nil || !bytes.Equal(response.GetResults()[0].GetDocument().GetPayload(), payload) {
		t.Fatalf("framed read failed: %v", err)
	}
	if server.memory.Used() != 0 {
		t.Fatalf("forwarding leaked %d", server.memory.Used())
	}
}

type badFrameEngine struct {
	forward.UnimplementedEngineServer
	mode string
}

func (s *badFrameEngine) ForwardStream(_ *forward.ForwardRequest, stream grpc.ServerStreamingServer[forward.ResponseFrame]) error {
	header := &forward.ResponseFrame{Size: 8}
	if err := stream.Send(header); err != nil {
		return err
	}
	frame := &forward.ResponseFrame{Data: make([]byte, forwarding.FrameBytes+1)}
	if s.mode == "oversized" {
		frame.Data = make([]byte, 1<<20)
	}
	return stream.Send(frame)
}

func TestInvalidStreamCannotAllocateAnnouncedMaximum(t *testing.T) {
	for _, mode := range []string{"oversized", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			backend := &badFrameEngine{mode: mode}
			target := serveEngine(t, backend)
			fixture := fixtureEngine{store: "primary", target: target}
			server := testGateway(t, 32<<20, fixture)
			server.memory = gatewayMemory(t, 2<<20)
			view := server.current
			route, err := routeFor(view, "primary")
			if err != nil {
				t.Fatal(err)
			}
			// Exercise the transport error, before public per-operation translation.
			entry, err := server.pool.acquire(route)
			if err != nil {
				t.Fatal(err)
			}
			defer server.pool.release(entry)
			scope := server.memory.NewScope()
			ctx := capacity.WithScope(context.Background(), scope)
			request := &forward.ForwardRequest{}
			_, err = server.forwardStream(ctx, entry, request)
			scope.Release()
			if mode == "oversized" && status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("oversized frame not limited by transport: %v", err)
			}
			if mode == "overflow" && status.Code(err) != codes.Internal {
				t.Fatalf("invalid frame not rejected: %v", err)
			}
			if server.memory.Used() != 0 {
				t.Fatal("invalid stream leaked capacity")
			}
		})
	}
}
