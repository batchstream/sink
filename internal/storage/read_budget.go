package storage

import (
	"errors"
	"sync"
)

const DefaultMaxReadBytes = 32 << 20

// ReadBudget is shared across every store and repeated key in one read. Reserve
// before copying a result so small key batches cannot allocate unbounded output.
type ReadBudget struct {
	mu        sync.Mutex
	remaining int
	maximum   int
	observe   func(int)
	unlimited bool
}

func NewReadBudget(maxBytes int) *ReadBudget {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxReadBytes
	}
	budget := &ReadBudget{remaining: maxBytes, maximum: maxBytes}
	return budget
}

// NewTrackedReadBudget accepts an explicit zero grant and reports successful charges.
// observe must not call back into this budget.
func NewTrackedReadBudget(maxBytes int, observe func(int)) *ReadBudget {
	budget := &ReadBudget{remaining: max(0, maxBytes), maximum: max(0, maxBytes), observe: observe}
	return budget
}

// NewUnboundedReadBudget leaves intermediate documents to process admission.
// Public response allowances continue to use NewReadBudget.
func NewUnboundedReadBudget() *ReadBudget {
	budget := &ReadBudget{unlimited: true}
	return budget
}

func (b *ReadBudget) Reserve(size int) error {
	if b.unlimited {
		if size < 0 {
			return ResourceExhaustedError(errors.New("invalid document size"))
		}
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Include room for the per-operation protobuf envelope and revision.
	const overhead = 128
	if size < 0 || b.remaining < overhead || size > b.remaining-overhead {
		cause := errors.New("read response exceeds its byte budget; request fewer or smaller records")
		return NewOperationError(ErrorCodeResourceExhausted, size >= 0 && size <= b.maximum-overhead, cause)
	}
	b.remaining -= size + overhead
	if b.observe != nil {
		b.observe(b.maximum - b.remaining)
	}
	return nil
}
