package service

import (
	"context"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type phaseMemoryStorage struct {
	storage.Storage
	storage.NativeStorage
	core             *Server
	entered          chan storage.WriteRequest
	resume           chan bool
	readReservations []int
	writes           int
}

func (s *phaseMemoryStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.core.admissionMu.Lock()
	s.readReservations = append(s.readReservations, s.core.inFlightBytes)
	s.core.admissionMu.Unlock()
	return s.Storage.Read(ctx, req)
}

func (s *phaseMemoryStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.writes++
	s.entered <- req
	response := storage.WriteResponse{}
	select {
	case conflict := <-s.resume:
		if conflict {
			for range req.Operations {
				result := storage.WriteResult{Status: storage.WriteStatusPreconditionFailed}
				response.Results = append(response.Results, result)
			}
			return response, nil
		}
	case <-ctx.Done():
		return response, ctx.Err()
	}
	return s.Storage.Write(ctx, req)
}

func (s *phaseMemoryStorage) Scan(context.Context, storage.ScanRequest) (storage.ScanResponse, error) {
	response := storage.ScanResponse{}
	return response, nil
}

type phaseWriteResult struct {
	response *sink.WriteResponse
	err      error
}

func TestSmallVisibleWriteReturnsUnusedWorkingCapacityToScans(t *testing.T) {
	backend := &phaseMemoryStorage{Storage: memory.New(), entered: make(chan storage.WriteRequest, 1), resume: make(chan bool)}
	defer close(backend.resume)
	core := completionServer(t, backend).server
	backend.core = core
	core.maxInFlightBytes = 70 << 20
	core.maxScanBytes = 64 << 20
	operation := completionMerge("record", 1)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: []*sink.WriteOperation{operation}}
	estimate := core.estimateWriteExecution(request, 1, 0)
	finished := make(chan phaseWriteResult, 1)
	go func() {
		response, err := core.Write(t.Context(), request)
		result := phaseWriteResult{response: response, err: err}
		finished <- result
	}()
	write := <-backend.entered
	if !write.WaitUntilVisible {
		t.Fatal("visible completion was weakened")
	}
	core.admissionMu.Lock()
	retained := core.inFlightBytes
	storeRetained := core.inFlightBytes
	core.admissionMu.Unlock()
	if retained >= 1<<20 || retained <= 0 || retained != storeRetained {
		t.Fatalf("small write kept hypothetical snapshots: global=%d store=%d", retained, storeRetained)
	}
	command := bson.D{{Key: "find", Value: "products"}}
	payload, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	native := &sink.Command{Store: "primary", Namespace: "catalog", ContentType: "application/bson", Payload: payload}
	scan := &sink.ScanRequest{Command: native}
	if _, err := core.Scan(t.Context(), scan); err != nil {
		t.Fatalf("56 MiB BSON scan could not use released write capacity: %v", err)
	}
	if len(finished) != 0 {
		t.Fatal("write returned before storage visibility completed")
	}
	backend.resume <- false
	result := <-finished
	if result.err != nil || result.response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("write result: %v, %v", result.response, result.err)
	}
	if core.inFlightBytes != 0 || core.inFlightRequests != 0 {
		t.Fatal("resized reservation leaked or released its original size twice")
	}
	t.Logf("synthetic small merge reservation: %d -> %d bytes while waiting for visibility", estimate.bytes, retained)
}

func TestWriteRestoresWorkingCapacityBeforeConflictRetry(t *testing.T) {
	backend := &phaseMemoryStorage{Storage: memory.New(), entered: make(chan storage.WriteRequest, 1), resume: make(chan bool)}
	defer close(backend.resume)
	core := completionServer(t, backend).server
	backend.core = core
	operation := completionMerge("record", 1)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: []*sink.WriteOperation{operation}}
	peak := core.estimateWriteExecution(request, 1, 0).bytes
	finished := make(chan phaseWriteResult, 1)
	go func() {
		response, err := core.Write(t.Context(), request)
		result := phaseWriteResult{response: response, err: err}
		finished <- result
	}()
	<-backend.entered
	backend.resume <- true
	<-backend.entered
	if len(backend.readReservations) != 2 || backend.readReservations[0] != peak || backend.readReservations[1] != peak {
		t.Fatalf("retry read lacked peak protection: %v want %d", backend.readReservations, peak)
	}
	backend.resume <- false
	result := <-finished
	if result.err != nil || result.response.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED || core.inFlightBytes != 0 {
		t.Fatalf("retry lost result or capacity: %v, %v", result.response, result.err)
	}
}

func TestWriteCannotRegrowWithoutCapacityAndPreservesEarlierSuccess(t *testing.T) {
	backend := &phaseMemoryStorage{Storage: memory.New(), entered: make(chan storage.WriteRequest, 1), resume: make(chan bool)}
	defer close(backend.resume)
	core := completionServer(t, backend).server
	backend.core = core
	core.maxInFlightBytes = 70 << 20
	put := completionPut("successful", 1)
	conditional := completionMerge("conflict", 1)
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE, Operations: []*sink.WriteOperation{put, conditional}}
	finished := make(chan phaseWriteResult, 1)
	go func() {
		response, err := core.Write(t.Context(), request)
		result := phaseWriteResult{response: response, err: err}
		finished <- result
	}()
	<-backend.entered
	backend.resume <- false
	<-backend.entered
	pressure := admissionRequest{encodedBytes: 65 << 20}
	_, release, err := core.admitRequest(t.Context(), pressure)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	backend.resume <- true
	result := <-finished
	if result.err != nil || len(result.response.GetResults()) != 2 {
		t.Fatalf("lost partial response: %v, %v", result.response, result.err)
	}
	if result.response.Results[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatal("earlier successful put was changed")
	}
	failure := result.response.Results[1].GetFailure()
	if failure.GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED || !failure.GetRetryable() {
		t.Fatalf("unapplied conflict must report safe per-operation retry: %v", failure)
	}
	if len(backend.readReservations) != 1 || backend.writes != 2 || core.inFlightBytes != pressure.encodedBytes {
		t.Fatal("restoring capacity allocated, replayed successful work or leaked its reservation")
	}
}
