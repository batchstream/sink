package service

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/backpressure"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type orderedAdmissionStore struct {
	*nativeBoundaryStore
	events chan string
}

func (s *orderedAdmissionStore) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.events <- "Read"
	return s.Store.Read(ctx, req)
}

func (s *orderedAdmissionStore) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.events <- "Write"
	return s.Store.Write(ctx, req)
}

func (s *orderedAdmissionStore) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	s.events <- "Delete"
	return s.Store.Delete(ctx, req)
}

func (s *orderedAdmissionStore) Execute(ctx context.Context, req storage.NativeRequest) (storage.NativeResponse, error) {
	s.events <- "Execute"
	return s.nativeBoundaryStore.Execute(ctx, req)
}

func (s *orderedAdmissionStore) Query(ctx context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	s.events <- "Query"
	return s.nativeBoundaryStore.Query(ctx, req)
}

func (s *orderedAdmissionStore) Count(ctx context.Context, req storage.CountRequest) (storage.CountResponse, error) {
	s.events <- "Count"
	return s.nativeBoundaryStore.Count(ctx, req)
}

func (s *orderedAdmissionStore) Scan(ctx context.Context, req storage.ScanRequest) (storage.ScanResponse, error) {
	s.events <- "Scan"
	return s.nativeBoundaryStore.Scan(ctx, req)
}

func TestBatchAndNativeMethodsShareOneDispatchFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		native := &nativeBoundaryStore{Store: memory.New()}
		backend := &orderedAdmissionStore{nativeBoundaryStore: native, events: make(chan string, 16)}
		batching, controller := admissionServer(t, backend, 1)
		defer batching.Close()
		held, err := controller.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		command := &sink.Command{Uri: "sink://primary", Method: "GET", Path: "/_search"}
		methods := []string{"Write", "Count", "Read", "Execute", "Delete", "Query", "Scan"}
		results := make(chan error, len(methods))
		for _, method := range methods {
			go func() {
				var err error
				switch method {
				case "Write":
					request := &sink.WriteRequest{Operations: []*sink.WriteOperation{completionPut("record", 1)}, CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
					_, err = batching.Write(t.Context(), request)
				case "Read":
					operation := &sink.ReadOperation{Address: completionAddress("record")}
					request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
					_, err = batching.Read(t.Context(), request)
				case "Delete":
					operation := &sink.DeleteOperation{Address: completionAddress("record")}
					request := &sink.DeleteRequest{Operations: []*sink.DeleteOperation{operation}, CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
					_, err = batching.Delete(t.Context(), request)
				case "Count":
					request := &sink.CountRequest{Command: command}
					_, err = batching.Count(t.Context(), request)
				case "Execute":
					request := &sink.ExecuteRequest{Command: command}
					_, err = batching.Execute(t.Context(), request)
				case "Query":
					request := &sink.QueryRequest{Command: command}
					_, err = batching.server.Query(t.Context(), request)
				case "Scan":
					request := &sink.ScanRequest{Command: command}
					_, err = batching.server.Scan(t.Context(), request)
				}
				results <- err
			}()
			synctest.Wait()
			time.Sleep(2 * time.Millisecond)
			synctest.Wait()
		}
		if len(backend.events) != 0 {
			t.Fatal("a method bypassed the shared admission window")
		}
		held.Release()
		for range methods {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		var got []string
		for range methods {
			got = append(got, <-backend.events)
		}
		if !slices.Equal(got, methods) {
			t.Fatalf("Store dispatch was not FIFO: got %v, want %v", got, methods)
		}
		permit, _, _ := controller.TryAcquire()
		if permit == nil {
			t.Fatal("mixed dispatch leaked a permit")
		}
		permit.Release()
	})
}

func TestBatchProducerWaitsForAdmissionSpaceWithoutBlockingCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := backpressure.Options{MaxConcurrent: 1, MaxQueuedTasks: 1, MaxQueuedBytes: 64}
		controller, err := backpressure.New(opts)
		if err != nil {
			t.Fatal(err)
		}
		held, err := controller.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		nativeContext, cancelNative := context.WithCancel(t.Context())
		defer cancelNative()
		nativeDone := make(chan error, 1)
		go func() {
			_, permit, err := controller.Admit(nativeContext, 16)
			permit.Release()
			nativeDone <- err
		}()
		synctest.Wait()
		executed := make(chan int, 4)
		execute := func(_ context.Context, calls []*batchCall[int, int]) {
			for _, call := range calls {
				executed <- call.request
				completeCall(call, call.request, nil)
			}
		}
		batchOptions := requestBatcherOptions[int, int]{Admission: controller, Unlimited: true, MaxWait: time.Millisecond, MaxOperations: 1, MaxBytes: 16, MaxQueuedOperations: 4, MaxQueuedBytes: 64, Execute: execute}
		batcher := newRequestBatcher(batchOptions)
		defer batcher.Close()
		canceledContext, cancel := context.WithCancel(t.Context())
		defer cancel()
		canceled := make(chan error, 1)
		go func() { _, err := batcher.Submit(canceledContext, 1, 1, 16); canceled <- err }()
		synctest.Wait()
		live := make(chan error, 1)
		go func() { _, err := batcher.Submit(t.Context(), 2, 1, 16); live <- err }()
		synctest.Wait()
		cancel()
		if err := <-canceled; status.Code(err) != codes.Canceled {
			t.Fatalf("full downstream queue blocked cancellation: %v", err)
		}
		synctest.Wait()
		if batcher.queuedCallCount() != 1 || len(executed) != 0 {
			t.Fatal("cancellation lost live upstream work or executed canceled work")
		}
		cancelNative()
		if err := <-nativeDone; status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
		held.Release()
		if err := <-live; err != nil {
			t.Fatalf("accepted batch was rejected by downstream queue: %v", err)
		}
		if got := <-executed; got != 2 || len(executed) != 0 {
			t.Fatalf("canceled batch reached execution: %d", got)
		}
	})
}

func TestCanceledMemberOfQueuedBatchDoesNotCancelOtherCallers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		controller := admissionController(t, 1)
		held, err := controller.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		executed := make(chan int, 2)
		execute := func(_ context.Context, calls []*batchCall[int, int]) {
			for _, call := range calls {
				executed <- call.request
				completeCall(call, call.request, nil)
			}
		}
		opts := requestBatcherOptions[int, int]{Admission: controller, Unlimited: true, MaxWait: time.Second, MaxOperations: 2, MaxBytes: 32, MaxQueuedOperations: 4, MaxQueuedBytes: 64, Execute: execute}
		batcher := newRequestBatcher(opts)
		defer batcher.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		first := make(chan error, 1)
		second := make(chan error, 1)
		go func() { _, err := batcher.Submit(ctx, 1, 1, 16); first <- err }()
		synctest.Wait()
		go func() { _, err := batcher.Submit(t.Context(), 2, 1, 16); second <- err }()
		synctest.Wait()
		cancel()
		if err := <-first; status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
		synctest.Wait()
		if batcher.queuedBytes != 16 || batcher.queuedCallCount() != 1 {
			t.Fatal("queued batch member cancellation lost or duplicated queue charges")
		}
		held.Release()
		if err := <-second; err != nil {
			t.Fatal(err)
		}
		if got := <-executed; got != 2 || len(executed) != 0 {
			t.Fatalf("batch cancellation reached the wrong calls: %d", got)
		}
	})
}
