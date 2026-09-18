package storage

import (
	"context"
	"errors"
	"github.com/liran/sink/internal/capacity"
	"sync"
)

const DefaultMaxReadBytes = 32 << 20

// ErrReadWorkingSetFull asks the synchronous executor to read this record in a
// later chunk. It is never a failure of the original RPC's document budget.
var ErrReadWorkingSetFull = errors.New("read working set is full")

// ReadBudget is shared across every store and repeated key in one read. Reserve
// before copying a result so small key batches cannot allocate unbounded output.
type ReadBudget struct {
	mu        sync.Mutex
	remaining int
	maximum   int
	shared    []*ReadBudget
	caller    *ReadBudget
	working   *ReadBudget
	observe   func(int)
	memory    *capacity.Lease
	context   context.Context
	output    *capacity.Scope
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

// NewSharedReadBudget charges each interested caller once for a shared snapshot.
// A caller that exhausted its own quota cannot prevent another caller's read.
func NewSharedReadBudget(budgets []*ReadBudget) *ReadBudget {
	if len(budgets) == 1 {
		return budgets[0]
	}
	budget := &ReadBudget{shared: budgets}
	return budget
}

// NewWorkingSetReadBudget limits retained snapshots across independent RPCs.
// A deferred snapshot does not consume its caller's quota. The working budget
// must be an ordinary NewReadBudget, not another composite budget.
func NewWorkingSetReadBudget(caller, working *ReadBudget) *ReadBudget {
	budget := &ReadBudget{caller: caller, working: working}
	return budget
}

func WithMemoryBudget(ctx context.Context, budget *ReadBudget, lease *capacity.Lease) *ReadBudget {
	if lease == nil {
		return budget
	}
	managed := &ReadBudget{caller: budget, memory: lease, context: ctx}
	return managed
}

func NewResponseBudget(ctx context.Context, maximum int) *ReadBudget {
	budget := NewReadBudget(maximum)
	if scope := capacity.FromContext(ctx); scope != nil {
		managed := &ReadBudget{caller: budget, context: ctx, output: scope}
		return managed
	}
	return budget
}

func (b *ReadBudget) Reserve(size int) error {
	if b.output != nil {
		if err := b.caller.Reserve(size); err != nil {
			return err
		}
		if err := b.output.Output(b.context, size+128); err != nil {
			return ResourceExhaustedError(err)
		}
		return nil
	}
	if b.memory != nil {
		if err := b.caller.Reserve(size); err != nil {
			return err
		}
		if err := capacity.Grow(b.context, b.memory, size+128); err != nil {
			return ResourceExhaustedError(err)
		}
		return nil
	}
	if b.working != nil {
		if err := b.working.Reserve(size); err != nil {
			if size >= 0 && size <= b.working.maximum-128 {
				return ErrReadWorkingSetFull
			}
			return err
		}
		if err := b.caller.Reserve(size); err != nil {
			b.working.mu.Lock()
			b.working.remaining += size + 128
			b.working.mu.Unlock()
			return err
		}
		return nil
	}
	if len(b.shared) > 0 {
		accepted := false
		var failure error
		for _, budget := range b.shared {
			if err := budget.Reserve(size); err != nil {
				failure = err
			} else {
				accepted = true
			}
		}
		if accepted {
			return nil
		}
		return failure
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
