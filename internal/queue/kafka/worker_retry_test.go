package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	"github.com/batchstream/sink/internal/queue"
)

type scriptedRetryHandler struct {
	t       *testing.T
	batches [][]queue.Mutation
	results [][]error
	calls   int
	cancel  context.CancelFunc
}

func (h *scriptedRetryHandler) HandleBatch(_ context.Context, batch []queue.Mutation) []error {
	h.t.Helper()
	if h.calls >= len(h.batches) {
		h.t.Fatalf("unexpected retry batch: %+v", batch)
	}
	if !reflect.DeepEqual(batch, h.batches[h.calls]) {
		h.t.Fatalf("batch %d = %+v, want %+v", h.calls, batch, h.batches[h.calls])
	}
	result := h.results[h.calls]
	h.calls++
	if h.calls == len(h.batches) && h.cancel != nil {
		h.cancel()
	}
	return result
}

func TestHandleWithRetryPreservesOriginalResults(t *testing.T) {
	mutations := make([]queue.Mutation, 5)
	for index := range mutations {
		address := &sink.RecordAddress{Uri: fmt.Sprintf("record-%d", index)}
		if index%2 == 0 {
			mutations[index].Write = &sink.WriteOperation{Address: address}
		} else {
			mutations[index].Delete = &sink.DeleteOperation{Address: address}
		}
	}
	original := slices.Clone(mutations)
	temporary := retryHandlerError{retryable: true}
	permanent := errors.New("permanent failure")
	cases := []struct {
		name    string
		results [][]error
		want    []error
		cancel  bool
		invalid bool
	}{
		{
			name:    "mixed completion and exhausted retry",
			results: [][]error{{nil, temporary, permanent, temporary, temporary}, {nil, temporary, permanent}, {temporary}},
			want:    []error{nil, nil, permanent, temporary, permanent},
		},
		{
			name:    "cancel compacted retry batch",
			results: [][]error{{nil, temporary, permanent, temporary, temporary}, {nil, temporary, permanent}},
			want:    []error{nil, nil, permanent, context.Canceled, permanent},
			cancel:  true,
		},
		{
			name:    "invalid compacted result count",
			results: [][]error{{nil, temporary, permanent, temporary, temporary}, {nil}},
			invalid: true,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				batches := [][]queue.Mutation{original, {original[1], original[3], original[4]}, {original[3]}}
				handler := &scriptedRetryHandler{t: t, batches: batches[:len(test.results)], results: test.results}
				if test.cancel {
					handler.cancel = cancel
				}
				worker := &Worker{handler: handler, maxRetryAttempts: 3, retryBackoff: time.Nanosecond, maxRetryBackoff: time.Nanosecond}
				results := worker.handleWithRetry(ctx, mutations)
				if !slices.Equal(mutations, original) {
					t.Fatal("worker changed the caller's mutation slice")
				}
				if handler.calls != len(test.results) {
					t.Fatalf("calls = %d, want %d", handler.calls, len(test.results))
				}
				if test.invalid {
					if len(results) != len(mutations) || results[0] != nil || !errors.Is(results[2], permanent) {
						t.Fatalf("completed results changed after malformed reply: %v", results)
					}
					for _, index := range []int{1, 3, 4} {
						if !isRetryable(results[index]) {
							t.Fatalf("result[%d] = %v, want retained retryable failure", index, results[index])
						}
					}
					return
				}
				if !reflect.DeepEqual(results, test.want) {
					t.Fatalf("results = %v, want %v", results, test.want)
				}
			})
		})
	}
}

type benchmarkRetryHandler struct {
	results []error
}

func (h *benchmarkRetryHandler) HandleBatch(_ context.Context, mutations []queue.Mutation) []error {
	return h.results[:len(mutations)]
}

func BenchmarkWorkerRetryBuffers(b *testing.B) {
	for _, count := range []int{1, 128, 500} {
		for _, retry := range []bool{false, true} {
			b.Run(fmt.Sprintf("operations=%d/retry=%t", count, retry), func(b *testing.B) {
				handler := &benchmarkRetryHandler{results: make([]error, count)}
				if retry {
					failure := retryHandlerError{retryable: true}
					for index := range handler.results {
						handler.results[index] = failure
					}
				}
				worker := &Worker{handler: handler, maxRetryAttempts: 3, retryBackoff: time.Nanosecond, maxRetryBackoff: time.Nanosecond}
				mutations := make([]queue.Mutation, count)
				ctx := b.Context()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					results := worker.handleWithRetry(ctx, mutations)
					if len(results) != count {
						b.Fatal("invalid result count")
					}
				}
			})
		}
	}
}
