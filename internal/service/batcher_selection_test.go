package service

import (
	"fmt"
	"testing"
)

func TestBatchSelectionRetainsBlockedQueueAndIndependentProgress(t *testing.T) {
	hot := recordIdentity{keyData: "hot"}
	cold := recordIdentity{keyData: "cold"}
	active := map[recordIdentity]bool{hot: true}
	batcher := &requestBatcher[int, int]{maxOperations: 1000, maxBytes: 16 << 20}
	pending := make([]*batchCall[int, int], 10000)
	for index := range pending {
		call := &batchCall[int, int]{request: index, records: []recordIdentity{hot}, operationCount: 1, encodedBytes: 128}
		pending[index] = call
	}
	selected, remaining, _ := batcher.selectReady(pending, active)
	if len(selected) != 0 || len(remaining) != len(pending) {
		t.Fatal("blocked queue changed")
	}
	independent := &batchCall[int, int]{records: []recordIdentity{cold}, operationCount: 1, encodedBytes: 128}
	pending = append(remaining, independent)
	selected, remaining, _ = batcher.selectReady(pending, active)
	if len(selected) != 1 || selected[0] != independent || len(remaining) != 10000 {
		t.Fatal("independent request cannot pass blocked backlog")
	}
	if pending[len(pending)-1] != independent {
		t.Fatal("selection modified the queue before dispatch")
	}
	for index, call := range remaining {
		if call.request != index || pending[index] != call {
			t.Fatal("selection reordered pending requests")
		}
	}
	delete(active, hot)
	selected, remaining, reason := batcher.selectReady(remaining, active)
	if len(selected) != 1000 || len(remaining) != 9000 || reason != "max_operations" {
		t.Fatal("unblocked queue did not respect batch limits")
	}
	for index, call := range selected {
		if call.request != index {
			t.Fatal("unblocked queue lost request order")
		}
	}
}

func BenchmarkBlockedQueueSelection(b *testing.B) {
	for _, count := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			key := recordIdentity{keyData: "hot-record"}
			active := map[recordIdentity]bool{key: true}
			pending := make([]*batchCall[int, int], count)
			for index := range pending {
				call := &batchCall[int, int]{records: []recordIdentity{key}, operationCount: 1, encodedBytes: 128}
				pending[index] = call
			}
			batcher := &requestBatcher[int, int]{maxOperations: 1000, maxBytes: 16 << 20}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				selected, remaining, _ := batcher.selectReady(pending, active)
				if len(selected) != 0 || len(remaining) != count {
					b.Fatal("invalid selection")
				}
			}
		})
	}
}

func TestBatchSelectionPreservesTransitiveDependencies(t *testing.T) {
	hot := recordIdentity{keyData: "hot"}
	linked := recordIdentity{keyData: "linked"}
	last := recordIdentity{keyData: "last"}
	cold := recordIdentity{keyData: "cold"}
	active := map[recordIdentity]bool{hot: true}
	batcher := &requestBatcher[int, int]{maxOperations: 100, maxBytes: 1000}
	first := &batchCall[int, int]{records: []recordIdentity{hot, linked}, operationCount: 2}
	second := &batchCall[int, int]{records: []recordIdentity{linked, last}, operationCount: 2}
	third := &batchCall[int, int]{records: []recordIdentity{last}, operationCount: 1}
	independent := &batchCall[int, int]{records: []recordIdentity{cold}, operationCount: 1}
	pending := []*batchCall[int, int]{first, second, third, independent}
	selected, remaining, _ := batcher.selectReady(pending, active)
	if len(selected) != 1 || selected[0] != independent || len(remaining) != 3 {
		t.Fatal("a dependent request overtook its blocked predecessor")
	}
	for index, call := range remaining {
		if call != pending[index] {
			t.Fatal("transitive dependency chain was reordered")
		}
	}
}
