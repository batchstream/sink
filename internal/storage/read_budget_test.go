package storage

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestReadResponseBudgetIsSharedAcrossConcurrentReaders(t *testing.T) {
	working := NewReadBudget(8 * 256)
	var accepted atomic.Int64
	var readers sync.WaitGroup
	for range 64 {
		readers.Go(func() {
			budget := working
			err := budget.Reserve(128)
			if err == nil {
				accepted.Add(1)
			}
		})
	}
	readers.Wait()
	if accepted.Load() != 8 {
		t.Fatalf("admitted %d records, want 8", accepted.Load())
	}
}
