package backpressure

import (
	"context"
	"errors"
	"time"

	"github.com/batchstream/sink/internal/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type method uint8

const (
	read method = iota
	write
	writeVisible
	deleteRecords
	deleteVisible
	execute
	query
	count
	scan
	methodCount
	sizeClasses = 4
)

var methodNames = [methodCount]string{"read", "write", "write_visible", "delete", "delete_visible", "execute", "query", "count", "scan"}

type feedback uint8

const (
	ignored feedback = iota
	healthy
	congested
	feedbackCount
)

func sizeClass(operations int) int {
	switch {
	case operations <= 1:
		return 0
	case operations <= 32:
		return 1
	case operations <= 128:
		return 2
	default:
		return 3
	}
}

// Classification is independent of retry decisions. Semantic/local quota
// failures are neutral even if a caller could retry them. Only explicit backend
// overloads and timeouts are congestion; unknown errors provide no evidence.
func classify(err error) feedback {
	if err == nil {
		return healthy
	}
	if errors.Is(err, context.Canceled) {
		return ignored
	}
	var operation *storage.OperationError
	if errors.As(err, &operation) {
		code, retryable := storage.ErrorDetails(err)
		if retryable {
			switch code {
			case storage.ErrorCodeResourceExhausted, storage.ErrorCodeUnavailable, storage.ErrorCodeDeadlineExceeded:
				return congested
			}
		}
		return ignored
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return congested
	}
	switch status.Code(err) {
	case codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded:
		return congested
	default:
		return ignored
	}
}

func combine(current feedback, next feedback) feedback {
	if current == congested || next == congested {
		return congested
	}
	if current == ignored || next == ignored {
		return ignored
	}
	return healthy
}

type observedStorage struct {
	storage.Storage
	controller *Controller
}

type observedNative struct {
	*observedStorage
	native storage.NativeStorage
}

// Observe never waits for admission, changes results or retries a call. Keep the
// original Storage separately for ownership/Close and health checks.
func Observe(backend storage.Storage, controller *Controller) storage.Storage {
	if controller == nil {
		return backend
	}
	observed := &observedStorage{Storage: backend, controller: controller}
	if native, ok := backend.(storage.NativeStorage); ok {
		wrapped := &observedNative{observedStorage: observed, native: native}
		return wrapped
	}
	return observed
}

func (s *observedStorage) finish(ctx context.Context, started sample, at time.Time, result feedback) {
	if errors.Is(ctx.Err(), context.Canceled) {
		result = ignored
	}
	s.controller.observe(started, time.Since(at), result)
}

func (s *observedStorage) Read(ctx context.Context, req storage.ReadRequest) (storage.ReadResponse, error) {
	started := s.controller.begin(read, len(req.Operations))
	at := time.Now()
	response, err := s.Storage.Read(ctx, req)
	result := classify(err)
	for _, item := range response.Results {
		if item.Status != storage.ReadStatusFound && item.Status != storage.ReadStatusNotFound {
			result = combine(result, combine(ignored, classify(item.Err)))
		}
	}
	if len(response.Results) != len(req.Operations) || len(req.Operations) == 0 {
		result = combine(result, ignored)
	}
	s.finish(ctx, started, at, result)
	return response, err
}

func (s *observedStorage) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	kind := write
	if req.WaitUntilVisible {
		kind = writeVisible
	}
	started := s.controller.begin(kind, len(req.Operations))
	at := time.Now()
	response, err := s.Storage.Write(ctx, req)
	result := classify(err)
	for _, item := range response.Results {
		if item.Status != storage.WriteStatusApplied {
			result = combine(result, combine(ignored, classify(item.Err)))
		}
	}
	if len(response.Results) != len(req.Operations) || len(req.Operations) == 0 {
		result = combine(result, ignored)
	}
	s.finish(ctx, started, at, result)
	return response, err
}

func (s *observedStorage) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	kind := deleteRecords
	if req.WaitUntilVisible {
		kind = deleteVisible
	}
	started := s.controller.begin(kind, len(req.Operations))
	at := time.Now()
	response, err := s.Storage.Delete(ctx, req)
	result := classify(err)
	for _, item := range response.Results {
		if item.Status != storage.DeleteStatusApplied {
			result = combine(result, combine(ignored, classify(item.Err)))
		}
	}
	if len(response.Results) != len(req.Operations) || len(req.Operations) == 0 {
		result = combine(result, ignored)
	}
	s.finish(ctx, started, at, result)
	return response, err
}

func (s *observedNative) Execute(ctx context.Context, req storage.NativeRequest) (storage.NativeResponse, error) {
	started := s.controller.begin(execute, 1)
	at := time.Now()
	response, err := s.native.Execute(ctx, req)
	result := combine(classify(err), classify(response.Failure))
	if !response.Success {
		result = combine(result, ignored)
	}
	s.finish(ctx, started, at, result)
	return response, err
}

func (s *observedNative) Count(ctx context.Context, req storage.CountRequest) (storage.CountResponse, error) {
	started := s.controller.begin(count, 1)
	at := time.Now()
	response, err := s.native.Count(ctx, req)
	s.finish(ctx, started, at, classify(err))
	return response, err
}

// Emit is synchronous under the NativeStorage contract. Exclude client sends
// and local response validation from latency and error feedback, including a
// deadline that expires inside Emit. Keep the permit while a cursor is open.
type streamObservation struct {
	ctx    context.Context
	wait   time.Duration
	failed bool
	emit   func(storage.Document) error
}

func (o *streamObservation) send(document storage.Document) error {
	at := time.Now()
	err := o.emit(document)
	o.wait += time.Since(at)
	o.failed = o.failed || err != nil || o.ctx.Err() != nil
	return err
}

func (s *observedNative) finishStream(started sample, at time.Time, stream *streamObservation, err error) {
	result := classify(err)
	duration := max(0, time.Since(at)-stream.wait)
	code, _ := storage.ErrorDetails(err)
	timeout := errors.Is(err, context.DeadlineExceeded) || code == storage.ErrorCodeDeadlineExceeded || status.Code(err) == codes.DeadlineExceeded
	// Driver-wide timeouts (e.g. HTTP Client.Timeout) can expire while Emit
	// waits even if the RPC has no deadline. Do not blame the backend when
	// downstream waiting dominates that timeout.
	if stream.failed || errors.Is(stream.ctx.Err(), context.Canceled) || (timeout && stream.wait > duration) {
		result = ignored
	}
	s.controller.observe(started, duration, result)
}

func (s *observedNative) Query(ctx context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	started := s.controller.begin(query, req.PageSize)
	stream := streamObservation{ctx: ctx, emit: req.Emit}
	if req.Emit != nil {
		req.Emit = stream.send
	}
	at := time.Now()
	response, err := s.native.Query(ctx, req)
	s.finishStream(started, at, &stream, err)
	return response, err
}

func (s *observedNative) Scan(ctx context.Context, req storage.ScanRequest) (storage.ScanResponse, error) {
	started := s.controller.begin(scan, req.BatchSize)
	stream := streamObservation{ctx: ctx, emit: req.Emit}
	if req.Emit != nil {
		req.Emit = stream.send
	}
	at := time.Now()
	response, err := s.native.Scan(ctx, req)
	s.finishStream(started, at, &stream, err)
	return response, err
}
