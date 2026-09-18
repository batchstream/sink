package service

import (
	"context"
	"fmt"
	"testing"
)

// Measure the dispatcher, record ownership and completion path without backend
// I/O. Reusing record names also exercises their release before the next call.
func BenchmarkBatcherRecordTracking(b *testing.B) {
	for _, count := range []int{1, 128, 1024} {
		for _, partial := range []bool{false, true} {
			b.Run(fmt.Sprintf("records=%d/partial=%v", count, partial), func(b *testing.B) {
				keys := make([]recordIdentity, count)
				for index := range keys {
					keys[index] = recordIdentity(fmt.Sprintf("record-%d", index))
				}
				records := func(int) []recordIdentity { return keys }
				execute := func(_ context.Context, calls []*batchCall[int, int]) {
					for _, call := range calls {
						if partial {
							for index := range call.records {
								call.finishRecords(call.records[index : index+1])
							}
						}
						completeCall(call, call.request, nil)
					}
				}
				opts := requestBatcherOptions[int, int]{
					MaxConcurrent:       1,
					MaxOperations:       count,
					MaxBytes:            1 << 20,
					MaxQueuedOperations: count,
					MaxQueuedBytes:      1 << 20,
					Records:             records,
					Execute:             execute,
				}
				batcher := newRequestBatcher(opts)
				b.Cleanup(batcher.Close)
				b.ReportAllocs()
				for b.Loop() {
					response, err := batcher.Submit(b.Context(), count, count, count)
					if err != nil || response != count {
						b.Fatalf("Submit = %d, %v", response, err)
					}
				}
			})
		}
	}
}
