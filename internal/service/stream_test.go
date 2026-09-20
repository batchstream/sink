package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/storage/memory"
	"google.golang.org/grpc"
)

type readSender struct {
	grpc.ServerStream
	ctx  context.Context
	send func(*sink.ReadResponse) error
}

func (s *readSender) Context() context.Context            { return s.ctx }
func (s *readSender) Send(frame *sink.ReadResponse) error { return s.send(frame) }

type writeSender struct {
	grpc.ServerStream
	ctx  context.Context
	send func(*sink.WriteResponse) error
}

func (s *writeSender) Context() context.Context             { return s.ctx }
func (s *writeSender) Send(frame *sink.WriteResponse) error { return s.send(frame) }

func TestReadStreamBoundsWorkingBatchAndWaitsForReceiver(t *testing.T) {
	backend := &readCapacityStorage{Storage: memory.New(), maximum: 32 * (33 << 10)}
	batching := completionServer(t, backend)
	batching.server.maxReadBytes = 256 << 10
	request := &sink.ReadRequest{}
	writes := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
	for i := range 96 {
		key := fmt.Sprint(i)
		operation := completionPut(key, i)
		operation.GetPut().Document.Payload = []byte(`{"padding":"` + strings.Repeat("x", 32<<10) + `"}`)
		writes.Operations = append(writes.Operations, operation)
		read := &sink.ReadOperation{Address: completionAddress(key)}
		request.Operations = append(request.Operations, read)
	}
	if _, err := batching.server.Write(t.Context(), writes); err != nil {
		t.Fatal(err)
	}
	count := 0
	stream := &readSender{ctx: t.Context()}
	stream.send = func(frame *sink.ReadResponse) error {
		if len(frame.Results) != 1 || frame.Results[0].GetStatus() != sink.ReadStatus_READ_STATUS_FOUND || frame.Results[0].OperationIndex != uint32(count) {
			t.Fatalf("invalid frame %v", frame)
		}
		if backend.reads.Load() != int64(count/32+1) {
			t.Fatal("executor read ahead of downstream consumption")
		}
		count++
		return nil
	}
	if err := batching.RPC().Read(request, stream); err != nil {
		t.Fatal(err)
	}
	if count != 96 || backend.reads.Load() != 3 {
		t.Fatalf("count=%d rounds=%d", count, backend.reads.Load())
	}
	// A send failure ends execution before another batch is fetched.
	backend.reads.Store(0)
	stop := errors.New("receiver stopped")
	stream.send = func(*sink.ReadResponse) error { return stop }
	if err := batching.RPC().Read(request, stream); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	if backend.reads.Load() != 1 {
		t.Fatal("read continued after failed send")
	}
}

func TestReturnedWriteStreamUsesPerResultBudgetAndPreservesSameRecordOrder(t *testing.T) {
	backend := &syncCapacityStorage{Storage: memory.New()}
	batching := completionServer(t, backend)
	batching.server.maxReadBytes = 4096
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
	for i := range 65 {
		operation := completionPut("same", i)
		operation.ReturnDocument = true
		operation.GetPut().Document.Payload = []byte(fmt.Sprintf(`{"value":%d,"padding":"%s"}`, i, strings.Repeat("x", 2048)))
		request.Operations = append(request.Operations, operation)
	}
	count := 0
	stream := &writeSender{ctx: t.Context()}
	stream.send = func(frame *sink.WriteResponse) error {
		result := frame.Results[0]
		expected := fmt.Sprintf(`"value":%d,`, count)
		if result.GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED || result.OperationIndex != uint32(count) || !strings.Contains(string(result.GetDocument().GetPayload()), expected) {
			t.Fatalf("order or document lost: %v", result)
		}
		count++
		return nil
	}
	if err := batching.RPC().Write(request, stream); err != nil {
		t.Fatal(err)
	}
	if count != 65 || backend.writes.Load() != 65 {
		t.Fatalf("count=%d writes=%d", count, backend.writes.Load())
	}
}
