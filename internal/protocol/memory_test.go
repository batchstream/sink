package protocol_test

import (
	"bytes"
	"context"
	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"

	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/protocol"
)

func TestManagedCodecReleasesOnLastTransportReference(t *testing.T) {
	for _, size := range []int{0, 1, 1024, 1025, 65536} {
		t.Run(string(rune('a'+size%26)), func(t *testing.T) {
			opts := capacity.Options{Bytes: 1 << 20, BurstPercent: 10, WaitTimeout: time.Second}
			p, err := capacity.New(opts)
			if err != nil {
				t.Fatal(err)
			}
			scope := p.NewScope()
			ctx := capacity.WithScope(t.Context(), scope)
			if err := scope.Admit(ctx, 1024); err != nil {
				t.Fatal(err)
			}
			message := &trackedVTMessage{data: bytes.Repeat([]byte{42}, size)}
			wrapped := &protocol.ManagedMessage{Message: message, Context: ctx}
			encoded, err := protocol.NewVTProtoCodec().Marshal(wrapped)
			if err != nil {
				t.Fatal(err)
			}
			encoded.Ref()
			encoded.Free()
			scope.Release() // gRPC stats.End may precede the transport's final Free.
			if p.Used() == 0 {
				t.Fatal("transport still owns an unaccounted buffer")
			}
			if !bytes.Equal(encoded.Materialize(), message.data) {
				t.Fatal("invalid encoded message")
			}
			encoded.Free()
			if p.Used() != 0 {
				t.Fatalf("transport leaked %d bytes", p.Used())
			}
		})
	}
}

func TestMemoryAdmissionPreservesSafeScanRetryOnlyBeforeExecution(t *testing.T) {
	opts := capacity.Options{Bytes: 1 << 20, BurstPercent: 10, WaitTimeout: time.Second}
	pool, err := capacity.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	blocker := pool.NewOwner().NewLease()
	if err := blocker.Grow(t.Context(), 943000, capacity.Request); err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	scope := pool.NewScope()
	defer scope.Release()
	ctx := capacity.WithScope(t.Context(), scope)
	req := &sink.ScanRequest{}
	info := &grpc.UnaryServerInfo{FullMethod: "/sink.v1.Sink/Scan"}
	called := false
	handler := func(context.Context, any) (any, error) { called = true; return nil, nil }
	_, err = protocol.MemoryInterceptor(ctx, req, info, handler)
	if called || status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("admission result: called=%v err=%v", called, err)
	}
	details := status.Convert(err).Details()
	if len(details) != 1 {
		t.Fatalf("missing Scan retry detail: %v", details)
	}
	detail, ok := details[0].(*errdetails.ErrorInfo)
	if !ok || detail.GetReason() != "SCAN_ADMISSION_REJECTED" {
		t.Fatalf("wrong retry detail: %v", details)
	}
	oversize := status.Error(codes.ResourceExhausted, "allocation exceeds process memory capacity")
	if len(status.Convert(protocol.MemoryAdmissionError(req, oversize)).Details()) != 0 {
		t.Fatal("permanent failure advertised safe retry")
	}
	write := &sink.WriteRequest{}
	if len(status.Convert(protocol.MemoryAdmissionError(write, capacity.ErrBusy)).Details()) != 0 {
		t.Fatal("write advertised Scan retry")
	}
}
