package storage

import (
	"errors"
	"sync"
)

const DefaultMaxReadBytes = 32 << 20

// ReadBudget bounds document copies within a local result, batch or scan page.
// It does not cross the Gateway-to-Engine transport boundary.
type ReadBudget struct {
	mu        sync.Mutex
	remaining int
	maximum   int
	unlimited bool
}

func NewReadBudget(maxBytes int) *ReadBudget {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxReadBytes
	}
	budget := &ReadBudget{remaining: maxBytes, maximum: maxBytes}
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
	return nil
}
