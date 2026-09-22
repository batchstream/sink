package backpressure

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorClassification(t *testing.T) {
	cause := errors.New("injected")
	for _, code := range []storage.ErrorCode{storage.ErrorCodeInvalidArgument, storage.ErrorCodeConflict, storage.ErrorCodePreconditionFailed, storage.ErrorCodeInternal} {
		for _, retryable := range []bool{false, true} {
			err := storage.NewOperationError(code, retryable, cause)
			if classify(err) != ignored {
				t.Fatalf("semantic error treated as congestion: %v", code)
			}
		}
	}
	for _, code := range []storage.ErrorCode{storage.ErrorCodeResourceExhausted, storage.ErrorCodeUnavailable, storage.ErrorCodeDeadlineExceeded} {
		for _, retryable := range []bool{false, true} {
			err := storage.NewOperationError(code, retryable, cause)
			want := ignored
			if retryable {
				want = congested
			}
			if classify(fmt.Errorf("wrapped: %w", err)) != want {
				t.Fatalf("incorrect storage classification: %v, retryable=%t", code, retryable)
			}
		}
	}
	for _, code := range []codes.Code{codes.InvalidArgument, codes.Aborted, codes.FailedPrecondition, codes.Canceled, codes.Internal, codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded} {
		want := ignored
		if code == codes.ResourceExhausted || code == codes.Unavailable || code == codes.DeadlineExceeded {
			want = congested
		}
		if classify(status.Error(code, "injected")) != want {
			t.Fatalf("incorrect gRPC classification: %v", code)
		}
	}
	if classify(context.DeadlineExceeded) != congested || classify(context.Canceled) != ignored || classify(cause) != ignored || classify(nil) != healthy {
		t.Fatal("incorrect context/unknown classification")
	}
	var cursor storage.ScanCursor
	_, err := cursor.Page(nil, make([]byte, storage.MaxScanCursorBytes))
	if err == nil || classify(err) != ignored {
		t.Fatal("local cursor size limit was treated as backend congestion")
	}
}

type feedbackStore struct {
	*memory.Store
	writeCalls    int
	failure       error
	nativeFailure error
	precondition  bool
	afterEmit     error
}

func (s *feedbackStore) Write(_ context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.writeCalls++
	response := storage.WriteResponse{Results: make([]storage.WriteResult, len(req.Operations))}
	for index := range response.Results {
		response.Results[index].Status = storage.WriteStatusApplied
	}
	if len(response.Results) != 0 && s.failure != nil {
		response.Results[0].Status = storage.WriteStatusFailed
		response.Results[0].Err = s.failure
	}
	if s.precondition {
		response.Results[0].Status = storage.WriteStatusPreconditionFailed
	}
	return response, nil
}

func (s *feedbackStore) Execute(context.Context, storage.NativeRequest) (storage.NativeResponse, error) {
	response := storage.NativeResponse{Success: s.nativeFailure == nil, Payload: []byte("original response"), Failure: s.nativeFailure}
	return response, s.failure
}

func (s *feedbackStore) Count(context.Context, storage.CountRequest) (storage.CountResponse, error) {
	response := storage.CountResponse{Count: 42}
	return response, s.failure
}

func (s *feedbackStore) Query(_ context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	var response storage.QueryResponse
	time.Sleep(10 * time.Millisecond)
	if req.Emit != nil {
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{}`)}
		if err := req.Emit(document); err != nil {
			return response, err
		}
	}
	return response, s.afterEmit
}

func (s *feedbackStore) Scan(ctx context.Context, req storage.ScanRequest) (storage.ScanResponse, error) {
	query := storage.QueryRequest{Emit: req.Emit}
	_, err := s.Query(ctx, query)
	var response storage.ScanResponse
	return response, err
}

func TestDecoratorPreservesCapabilitiesAndResultsWithoutRetries(t *testing.T) {
	c := testController(t, 8)
	c.limit = 8
	plain := memory.New()
	if _, ok := Observe(plain, c).(storage.NativeStorage); ok {
		t.Fatal("decorator invented NativeStorage support")
	}
	if Observe(plain, nil) != plain {
		t.Fatal("nil observer replaced backend")
	}
	backend := &feedbackStore{Store: plain}
	wrapped := Observe(backend, c)
	native, ok := wrapped.(storage.NativeStorage)
	if !ok {
		t.Fatal("NativeStorage capability lost")
	}
	var request storage.NativeRequest
	response, err := native.Execute(t.Context(), request)
	if err != nil || !response.Success || string(response.Payload) != "original response" {
		t.Fatal("native response changed")
	}
	var countRequest storage.CountRequest
	total, err := native.Count(t.Context(), countRequest)
	if err != nil || total.Count != 42 {
		t.Fatal("native count changed")
	}
	backend.failure = storage.ResourceExhaustedError(errors.New("overloaded"))
	writeRequest := storage.WriteRequest{Operations: make([]storage.WriteOperation, 32)}
	written, err := wrapped.Write(t.Context(), writeRequest)
	if err != nil || backend.writeCalls != 1 || written.Results[0].Err != backend.failure || written.Results[1].Status != storage.WriteStatusApplied {
		t.Fatal("partial/unknown write results or retry count changed")
	}
	if c.limit != 0 || c.observed.overloads != 1 || c.observed.samples[write][congested] != 1 {
		t.Fatal("partial batch overload was not counted once")
	}
	// Health bypasses feedback and admission even with a zero execution window.
	if err := wrapped.Ping(t.Context()); err != nil || c.observed.overloads != 1 {
		t.Fatal("backpressure affected health")
	}
	var readRequest storage.ReadRequest
	if _, err := wrapped.Read(t.Context(), readRequest); err != nil {
		t.Fatal(err)
	}
	var deleteRequest storage.DeleteRequest
	if _, err := wrapped.Delete(t.Context(), deleteRequest); err != nil {
		t.Fatal(err)
	}
}

func TestNativeFailureAndSemanticFeedback(t *testing.T) {
	for _, failure := range []error{
		storage.InvalidArgumentError(errors.New("bad command")),
		storage.NewOperationError(storage.ErrorCodeConflict, true, errors.New("conflict")),
		storage.NewOperationError(storage.ErrorCodePreconditionFailed, false, errors.New("condition")),
		storage.ResourceExhaustedError(errors.New("overload")),
	} {
		c := testController(t, 8)
		c.limit = 8
		backend := &feedbackStore{Store: memory.New(), nativeFailure: failure}
		native := Observe(backend, c).(storage.NativeStorage)
		var request storage.NativeRequest
		response, err := native.Execute(t.Context(), request)
		if err != nil || response.Failure != failure || response.Success {
			t.Fatal("native backend response changed")
		}
		want := 8
		if classify(failure) == congested {
			want = 0
		}
		if c.limit != want {
			t.Fatalf("failure %v produced window %d, want %d", failure, c.limit, want)
		}
	}
	c := testController(t, 8)
	c.limit = 8
	backend := &feedbackStore{Store: memory.New(), precondition: true}
	wrapped := Observe(backend, c)
	request := storage.WriteRequest{Operations: make([]storage.WriteOperation, 1), WaitUntilVisible: true}
	if _, err := wrapped.Write(t.Context(), request); err != nil || c.observed.samples[writeVisible][ignored] != 1 {
		t.Fatal("precondition was treated as congestion or success")
	}
	backend.precondition = false
	backend.failure = storage.BackendError(context.DeadlineExceeded)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := wrapped.Write(ctx, request); err != nil || c.limit != 8 {
		t.Fatal("caller cancellation affected the window")
	}
}

func TestStreamFeedbackExcludesSlowClientAndCallbackFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, scanCall := range []bool{false, true} {
			c := testController(t, 8)
			c.limit = 8
			backend := &feedbackStore{Store: memory.New()}
			native := Observe(backend, c).(storage.NativeStorage)
			kind := query
			if scanCall {
				kind = scan
			}
			for range 8 {
				emit := func(storage.Document) error { time.Sleep(time.Second); return nil }
				queryRequest := storage.QueryRequest{PageSize: 1, Emit: emit}
				scanRequest := storage.ScanRequest{BatchSize: 1, Emit: emit}
				var err error
				if scanCall {
					_, err = native.Scan(t.Context(), scanRequest)
				} else {
					_, err = native.Query(t.Context(), queryRequest)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if c.limit != 8 || c.observed.seconds[kind] > 0.081 || c.latency[kind][0].baseline > float64(11*time.Millisecond) {
				t.Fatal("slow client time leaked into backend latency")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			emit := func(storage.Document) error {
				<-ctx.Done()
				return status.Error(codes.ResourceExhausted, "downstream quota")
			}
			request := storage.QueryRequest{PageSize: 1, Emit: emit}
			_, err := native.Query(ctx, request)
			cancel()
			if status.Code(err) != codes.ResourceExhausted || c.limit != 8 {
				t.Fatal("callback timeout/failure was treated as backend congestion")
			}
			backend.afterEmit = context.DeadlineExceeded
			request.Emit = func(storage.Document) error { time.Sleep(time.Second); return nil }
			_, err = native.Query(t.Context(), request)
			if !errors.Is(err, context.DeadlineExceeded) || c.limit != 8 {
				t.Fatal("driver timeout dominated by Emit was treated as backend congestion")
			}
			request.Emit = nil
			_, err = native.Query(t.Context(), request)
			if !errors.Is(err, context.DeadlineExceeded) || c.limit != 0 {
				t.Fatal("real backend timeout was ignored")
			}
		}
	})
}

func TestControllerMetricsAndValidation(t *testing.T) {
	for _, maximum := range []int{-1, 4097} {
		opts := Options{MaxConcurrent: maximum}
		if _, err := New(opts); err == nil {
			t.Fatal("invalid ceiling accepted")
		}
	}
	c := testController(t, 0)
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(c); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil || len(families) != 8 {
		t.Fatalf("invalid metrics: %d families, %v", len(families), err)
	}
}
