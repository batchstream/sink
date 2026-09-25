package service

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/backpressure"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func admissionController(t *testing.T, maximum int) *backpressure.Controller {
	t.Helper()
	opts := backpressure.Options{Store: "primary", Role: "engine", MaxConcurrent: maximum}
	c, err := backpressure.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestStoreAdmissionRetainsBoundedQueueAndCancelsWithoutDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		controller := admissionController(t, 1)
		permit, err := controller.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer permit.Release()
		executions := 0
		execute := func(ctx context.Context, calls []*batchCall[int, int]) {
			executions++
			<-ctx.Done()
			for _, call := range calls {
				completeCall(call, 0, ctx.Err())
			}
		}
		opts := requestBatcherOptions[int, int]{Admission: controller, Unlimited: true, MaxWait: time.Millisecond, MaxOperations: 1, MaxBytes: 16, MaxQueuedOperations: 64, MaxQueuedBytes: 1024, Execute: execute}
		batcher := newRequestBatcher(opts)
		defer batcher.Close()
		results := make(chan error, 64)
		cancels := make([]context.CancelFunc, 64)
		for index := range 64 {
			ctx, cancel := context.WithCancel(t.Context())
			cancels[index] = cancel
			go func() { _, err := batcher.Submit(ctx, index, 1, 16); results <- err }()
		}
		synctest.Wait()
		goroutines := runtime.NumGoroutine()
		for range 1000 {
			if _, err := batcher.Submit(t.Context(), 999, 1, 16); status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("full queue did not reject: %v", err)
			}
		}
		synctest.Wait()
		if batcher.queuedCallCount() != 64 || batcher.queuedBytes != 1024 || executions != 0 {
			t.Fatal("work escaped bounded queue without a permit")
		}
		if runtime.NumGoroutine() > goroutines+2 {
			t.Fatal("rejected work created waiting goroutines")
		}
		for _, cancel := range cancels[:32] {
			cancel()
		}
		synctest.Wait()
		if batcher.queuedCallCount() != 32 || executions != 0 {
			t.Fatal("canceled waiting work retained capacity or reached Store")
		}
		permit.Release()
		synctest.Wait()
		if executions != 1 || batcher.queuedCallCount() != 31 {
			t.Fatalf("dispatch ignored shared window: executing=%d queued=%d", executions, batcher.queuedCallCount())
		}
		batcher.Close()
		for range 64 {
			if err := <-results; err == nil {
				t.Fatal("canceled/shutdown work unexpectedly succeeded")
			}
		}
		for _, cancel := range cancels {
			cancel()
		}
		if batcher.queuedCallCount() != 0 {
			t.Fatal("shutdown leaked queued work")
		}
		next, _, _ := controller.TryAcquire()
		if next == nil {
			t.Fatal("shutdown leaked execution permit")
		}
		next.Release()
	})
}

func TestFinishedBatchRetainsPermitUntilDispatcherReceivesCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		controller := admissionController(t, 1)
		entered := make(chan struct{})
		finish := make(chan struct{})
		execute := func(_ context.Context, calls []*batchCall[int, int]) {
			close(entered)
			<-finish
			completeCall(calls[0], 1, nil)
		}
		opts := requestBatcherOptions[int, int]{Admission: controller, Unlimited: true, MaxWait: time.Millisecond, MaxOperations: 1, MaxBytes: 1, MaxQueuedOperations: 2, MaxQueuedBytes: 2, Execute: execute}
		batcher := newRequestBatcher(opts)
		defer batcher.Close()
		result := make(chan error, 1)
		go func() { _, err := batcher.Submit(t.Context(), 1, 1, 1); result <- err }()
		<-entered

		// Hold the dispatcher while it settles a canceled queued call. Finished
		// executions must not free slots before their completion is received.
		blocked := make(chan struct{})
		resume := make(chan struct{})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		pending := &batchCall[int, int]{ctx: ctx, request: 2, operationCount: 1, encodedBytes: 1, enqueuedAt: time.Now(), result: make(chan batchResult[int], 1)}
		pending.stopWake = func() bool { close(blocked); <-resume; return true }
		if err := batcher.reserve(pending); err != nil {
			t.Fatal(err)
		}
		cancel()
		batcher.input <- pending
		<-blocked
		close(finish)
		synctest.Wait()
		permit, _, _ := controller.TryAcquire()
		retained := permit == nil
		permit.Release()
		close(resume)
		batcher.Close()
		if !retained {
			t.Fatal("finished work freed admission before the dispatcher received it")
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		permit, _, _ = controller.TryAcquire()
		if permit == nil {
			t.Fatal("completion settlement leaked its permit")
		}
		permit.Release()
	})
}

func admissionServer(t *testing.T, backend storage.Storage, maximum int) (*BatchingServer, *backpressure.Controller) {
	t.Helper()
	controller := admissionController(t, maximum)
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Admission: controller, Storage: backend, BoundStore: "primary", Lua: lua}
	core, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	batchOptions := BatchingOptions{MaxWait: time.Millisecond, MaxOperations: 1}
	batching, err := NewBatchingServer(core, batchOptions)
	if err != nil {
		t.Fatal(err)
	}
	return batching, controller
}

type heldAdmissionStore struct {
	*nativeBoundaryStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *heldAdmissionStore) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.once.Do(func() { close(s.entered); <-s.release })
	// A backend can commit even after the caller canceled. Its permit and
	// document dependency must survive until that outcome is actually returned.
	return s.Store.Write(context.WithoutCancel(ctx), req)
}

func TestSharedStoreAdmissionPreservesOrderingAndNativeCapability(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		native := &nativeBoundaryStore{Store: memory.New()}
		backend := &heldAdmissionStore{nativeBoundaryStore: native, entered: make(chan struct{}), release: make(chan struct{})}
		batching, controller := admissionServer(t, backend, 1)
		defer batching.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		first := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{completionPut("hot", 1)}}
		firstResult := make(chan error, 1)
		go func() { _, err := batching.Write(ctx, first); firstResult <- err }()
		awaitCompletion(t, backend.entered)
		cancel()
		if err := <-firstResult; status.Code(err) != codes.Canceled {
			t.Fatalf("canceled caller: %v", err)
		}
		second := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{completionPut("hot", 2)}}
		readOp := &sink.ReadOperation{Address: completionAddress("other")}
		readRequest := &sink.ReadRequest{Operations: []*sink.ReadOperation{readOp}}
		deleteOp := &sink.DeleteOperation{Address: completionAddress("other")}
		deleteRequest := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.DeleteOperation{deleteOp}}
		results := make(chan error, 3)
		go func() { _, err := batching.Write(t.Context(), second); results <- err }()
		go func() { _, err := batching.Read(t.Context(), readRequest); results <- err }()
		go func() { _, err := batching.Delete(t.Context(), deleteRequest); results <- err }()
		synctest.Wait()
		if batching.writes.queuedCallCount() != 1 || batching.reads.queuedCallCount() != 1 || batching.deletes.queuedCallCount() != 1 {
			t.Fatal("Read/Write/Delete did not share Store execution budget")
		}
		command := &sink.Command{Uri: "sink://primary", Method: "GET", Path: "/_search"}
		countRequest := &sink.CountRequest{Command: command}
		countContext, cancelCount := context.WithCancel(t.Context())
		countResult := make(chan error, 1)
		go func() { _, err := batching.Count(countContext, countRequest); countResult <- err }()
		synctest.Wait()
		select {
		case err := <-countResult:
			t.Fatalf("Count did not wait for the shared Store budget: %v", err)
		default:
		}
		cancelCount()
		if err := <-countResult; status.Code(err) != codes.Canceled || native.calls != 0 {
			t.Fatal("canceled queued Count reached Store")
		}
		if permit, _, _ := controller.TryAcquire(); permit != nil {
			permit.Release()
			t.Fatal("caller cancellation released an executing mutation")
		}
		close(backend.release)
		for range 3 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		readOp.Address = completionAddress("hot")
		response, err := batching.Read(t.Context(), readRequest)
		if err != nil || string(response.Results[0].Document.Payload) != `{"value":2}` {
			t.Fatalf("record ordering changed: %v, %v", response, err)
		}
		synctest.Wait()
		count, err := batching.Count(t.Context(), countRequest)
		if err != nil || count.Count != 123 || native.calls != 1 {
			t.Fatal("Native capability was not retained after observation wrapping")
		}
	})
}

func TestAdmittedMergeRetainsItsPermitAcrossConflictRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := &heldReadStorage{Storage: memory.New(), entered: make(chan struct{}), release: make(chan struct{}), writes: make(map[string]int), conflict: true}
		close(backend.release)
		batching, controller := admissionServer(t, backend, 1)
		defer batching.Close()
		request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{completionMerge("slow", 1)}}
		response, err := batching.Write(t.Context(), request)
		if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || backend.reads != 2 || backend.writes["slow"] != 2 {
			t.Fatalf("admission changed conflict retry: response=%v err=%v reads=%d writes=%d", response, err, backend.reads, backend.writes["slow"])
		}
		synctest.Wait()
		permit, _, _ := controller.TryAcquire()
		if permit == nil {
			t.Fatal("semantic conflict caused cooldown or leaked permit")
		}
		permit.Release()
	})
}

type concurrentAdmissionStore struct {
	storage.Storage
	mu     sync.Mutex
	active int
	peak   int
}

func (s *concurrentAdmissionStore) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	s.mu.Lock()
	s.active++
	s.peak = max(s.peak, s.active)
	s.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	defer func() {
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
	}()
	return s.Storage.Read(ctx, req)
}

func TestBatchDispatchGrowsFromRealStoreFeedback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := &concurrentAdmissionStore{Storage: memory.New()}
		batching, _ := admissionServer(t, backend, 8)
		defer batching.Close()
		results := make(chan error, 512)
		for range 512 {
			operation := &sink.ReadOperation{Address: completionAddress("record")}
			request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
			go func() { _, err := batching.Read(t.Context(), request); results <- err }()
		}
		for range 512 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if backend.peak < 3 || backend.peak > 8 || backend.active != 0 {
			t.Fatalf("real feedback failed to grow bounded dispatch: peak=%d active=%d", backend.peak, backend.active)
		}
	})
}
