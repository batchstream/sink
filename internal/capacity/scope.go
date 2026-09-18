package capacity

import (
	"context"
	"sync"
)

type contextKey struct{}

// Scope follows an RPC through batching and transport ownership. Cancellation
// stops new acquisitions; only the last owner releases already retained bytes.
type Scope struct {
	mu      sync.Mutex
	owner   *Owner
	refs    int
	leases  []*Lease
	input   *Lease
	output  *Lease
	outputs map[*Owner]*Lease
}

func (p *Pool) NewScope() *Scope {
	s := &Scope{owner: p.NewOwner(), refs: 1}
	s.input = s.NewLease()
	s.output = s.NewLease()
	return s
}

func WithScope(ctx context.Context, s *Scope) context.Context {
	return context.WithValue(ctx, contextKey{}, s)
}

func FromContext(ctx context.Context) *Scope {
	s, _ := ctx.Value(contextKey{}).(*Scope)
	return s
}

func (s *Scope) NewLease() *Lease {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.owner.NewLease()
	if s.refs == 0 {
		l.Close()
		return l
	}
	s.leases = append(s.leases, l)
	return l
}

// Retain is called before handing work to another goroutine or transport.
func (s *Scope) Retain() func() {
	if s == nil {
		return func() {}
	}
	s.mu.Lock()
	if s.refs == 0 {
		s.mu.Unlock()
		return func() {}
	}
	s.refs++
	s.mu.Unlock()
	return sync.OnceFunc(s.Release)
}

func (s *Scope) Release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refs == 0 {
		return
	}
	s.refs--
	if s.refs == 0 {
		for _, l := range s.leases {
			l.Close()
		}
		s.leases = nil
	}
}

func (s *Scope) Admit(ctx context.Context, bytes int) error {
	if s == nil {
		return nil
	}
	return s.input.Grow(ctx, int64(bytes), Request)
}

// Output acquires a known document and its eventual encoded copy together.
// Reserving the actual encoded size before commit protects returned writes.
func (s *Scope) Output(ctx context.Context, bytes int) error {
	return s.OutputFrom(ctx, s, bytes)
}

// OutputFrom treats batched work as one completion owner; each caller retains its
// own output leases. This avoids a batch blocking on its own borrowed reserve.
func (s *Scope) OutputFrom(ctx context.Context, producer *Scope, bytes int) error {
	if s == nil {
		return nil
	}
	if producer == nil || producer.owner == s.owner {
		return s.output.Grow(ctx, 2*int64(bytes), Response)
	}
	s.mu.Lock()
	if s.outputs == nil {
		s.outputs = make(map[*Owner]*Lease)
	}
	lease := s.outputs[producer.owner]
	if lease == nil {
		lease = producer.owner.NewLease()
		if s.refs == 0 {
			lease.Close()
		} else {
			s.leases = append(s.leases, lease)
		}
		s.outputs[producer.owner] = lease
	}
	s.mu.Unlock()
	return lease.Grow(ctx, 2*int64(bytes), Response)
}

// EnsureOutput reuses document and encoding capacity already acquired by the
// producer. Allocation growth remains atomic in the process pool.
func (s *Scope) EnsureOutput(ctx context.Context, bytes int) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	used := s.output.Bytes()
	for _, lease := range s.outputs {
		used += lease.Bytes()
	}
	s.mu.Unlock()
	return s.output.Grow(ctx, max(0, 2*int64(bytes)-used), Response)
}

func Grow(ctx context.Context, lease *Lease, bytes int) error {
	if lease == nil {
		return nil
	}
	return lease.Grow(ctx, int64(bytes), Response)
}

func Close(lease *Lease) {
	if lease != nil {
		lease.Close()
	}
}

// AdmitCompletion acquires small, known acknowledgement envelopes from ordinary
// capacity before starting work. Only later document growth can use the reserve.
func (s *Scope) AdmitCompletion(ctx context.Context, bytes int) error {
	if s == nil {
		return nil
	}
	return s.output.Grow(ctx, 2*int64(bytes), Request)
}

// ReleaseOutputFrom returns unused pre-commit document capacity after a final
// failure or after a successful candidate shrank. Retained results keep theirs.
func (s *Scope) ReleaseOutputFrom(producer *Scope, bytes int) {
	if s == nil || bytes <= 0 {
		return
	}
	s.mu.Lock()
	lease := s.output
	if producer != nil && producer.owner != s.owner {
		lease = s.outputs[producer.owner]
	}
	s.mu.Unlock()
	if lease != nil {
		lease.Shrink(2 * int64(bytes))
	}
}
