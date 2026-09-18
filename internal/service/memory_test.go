package service

import (
	"context"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/memory"
)

func TestBatchedInputMemoryIsNotReservedTwice(t *testing.T) {
	opts := capacity.Options{Bytes: 512 << 10, BurstPercent: 10, WaitTimeout: time.Second}
	pool, err := capacity.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	scope := pool.NewScope()
	defer scope.Release()
	if err := scope.Admit(t.Context(), 256<<10); err != nil {
		t.Fatal(err)
	}
	caller := capacity.WithScope(t.Context(), scope)
	budgets := contextBudgets(caller, 1)
	legacy := &admissionPool{requestTimeout: time.Second}
	core := &Server{memory: pool, admissionPool: legacy}
	request := admissionRequest{inputBytes: 256 << 10, resultBytes: 4096, sharedInput: budgets.ownsInput()}
	_, release, err := core.admitRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("batch reserved its already-owned input again: %v", err)
	}
	if pool.Used() >= 300<<10 {
		t.Fatalf("batch charged payload copies instead of bookkeeping: %d", pool.Used())
	}
	release()
	if pool.Used() != 256<<10 {
		t.Fatalf("batch released original caller ownership: %d", pool.Used())
	}
}

func TestBatchCancellationRetainsProducerMemory(t *testing.T) {
	opts := capacity.Options{Bytes: 1 << 20, BurstPercent: 10, WaitTimeout: time.Second}
	pool, err := capacity.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	scope := pool.NewScope()
	if err := scope.Admit(t.Context(), 4096); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(capacity.WithScope(t.Context(), scope))
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	execute := func(_ context.Context, calls []*batchCall[int, int]) {
		close(entered)
		<-release
		for _, call := range calls {
			completeCall(call, 1, nil)
		}
	}
	batching := requestBatcherOptions[int, int]{Unlimited: true, MaxWait: time.Millisecond, MaxOperations: 1, MaxBytes: 100, MaxQueuedOperations: 10, MaxQueuedBytes: 100, Execute: execute}
	batcher := newRequestBatcher(batching)
	done := make(chan error, 1)
	go func() { _, err := batcher.Submit(ctx, 1, 1, 1); done <- err }()
	<-entered
	cancel()
	<-done
	scope.Release()
	if pool.Used() != 4096 {
		t.Fatal("canceled caller prematurely released producer input")
	}
	close(release)
	batcher.Close()
	if pool.Used() != 0 {
		t.Fatal("producer memory leaked after completion")
	}
}

type demandReadStorage struct {
	storage.Storage
	entered chan struct{}
	resume  chan struct{}
}

func (s *demandReadStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.entered <- struct{}{}
	select {
	case <-s.resume:
	case <-ctx.Done():
		var empty storage.ReadResponse
		return empty, ctx.Err()
	}
	return s.Storage.Read(ctx, req)
}

func TestDemandReadsIgnoreMaximumResponseAndRequestCount(t *testing.T) {
	backend := &demandReadStorage{Storage: memory.New(), entered: make(chan struct{}, 16), resume: make(chan struct{})}
	core := completionServer(t, backend).server
	options := capacity.Options{Bytes: 4 << 20, BurstPercent: 10, WaitTimeout: time.Second}
	pool, err := capacity.New(options)
	if err != nil {
		t.Fatal(err)
	}
	core.memory = pool
	core.maxReadBytes = 32 << 20
	core.maxInFlightRequests = 1
	done := make(chan error, 16)
	for range 16 {
		go func() {
			operation := &sink.ReadOperation{Address: completionMerge("small", 1).Address}
			request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
			_, err := core.Read(t.Context(), request)
			done <- err
		}()
	}
	for range 16 {
		select {
		case <-backend.entered:
		case <-time.After(time.Second):
			close(backend.resume)
			t.Fatal("maximum response or count limit blocked small reads")
		}
	}
	close(backend.resume)
	for range 16 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if pool.Used() != 0 {
		t.Fatalf("read leaked %d bytes", pool.Used())
	}
}

func TestDemandReturnedWriteRejectsBeforeCommit(t *testing.T) {
	backend := memory.New()
	core := completionServer(t, backend).server
	options := capacity.Options{Bytes: 128 << 10, BurstPercent: 10, WaitTimeout: time.Second}
	pool, err := capacity.New(options)
	if err != nil {
		t.Fatal(err)
	}
	core.memory = pool
	payload := []byte(`{"value":"` + strings.Repeat("x", 40<<10) + `"}`)
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_JSON, Payload: payload}
	put := &sink.PutOperation{Document: document, Mode: sink.WriteMode_WRITE_MODE_UPSERT}
	action := &sink.WriteOperation_Put{Put: put}
	address := completionMerge("no-commit", 1).Address
	operation := &sink.WriteOperation{Address: address, Action: action, ReturnDocument: true}
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
	result, err := core.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetResults()[0].GetStatus() != sink.WriteStatus_WRITE_STATUS_FAILED || result.GetResults()[0].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
		t.Fatalf("unexpected failure: %v", result)
	}
	readOperation := &sink.ReadOperation{Address: address}
	readRequest := &sink.ReadRequest{Operations: []*sink.ReadOperation{readOperation}}
	read, err := core.Read(t.Context(), readRequest)
	if err != nil || read.GetResults()[0].GetStatus() != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("write committed before reserving its return: %v %v", read, err)
	}
	if pool.Used() != 0 {
		t.Fatal("failed write leaked")
	}
}
