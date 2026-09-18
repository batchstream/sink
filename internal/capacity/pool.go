// Package capacity manages process-local ownership of request and response bytes.
package capacity

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrBusy = status.Error(codes.ResourceExhausted, "process memory capacity is occupied")

type Phase string

const (
	Request  Phase = "request"
	Response Phase = "response"
)

type Options struct {
	Bytes        int64
	BurstPercent int
	WaitTimeout  time.Duration
	MaxWaiters   int
	Source       string
	Role         string
	Store        string
}

type Pool struct {
	mu          sync.Mutex
	total       int64
	normal      int64
	used        int64
	opaque      int64
	burstOwner  *Owner
	waiters     []*waiter
	waitTimeout time.Duration
	maxWaiters  int
	observed    *observations
}

// Owner identifies one operation, including all of its retained buffers.
type Owner struct {
	pool  *Pool
	bytes int64
}

// Lease is independently released when the corresponding buffer stops being owned.
type Lease struct {
	owner  *Owner
	growMu sync.Mutex
	bytes  int64
	closed bool
	opaque bool
}

type waiter struct {
	lease   *Lease
	bytes   int64
	phase   Phase
	started time.Time
	ready   chan struct{}
	granted bool
}

func New(opts Options) (*Pool, error) {
	if opts.Bytes < 1024 || opts.BurstPercent < 1 || opts.BurstPercent > 99 || opts.WaitTimeout <= 0 {
		return nil, errors.New("memory capacity requires at least 1KiB, burst_percent between 1 and 99, and a positive wait timeout")
	}
	burst := opts.Bytes / 100 * int64(opts.BurstPercent)
	if opts.MaxWaiters == 0 {
		opts.MaxWaiters = 1024
	}
	if opts.MaxWaiters < 0 {
		return nil, errors.New("memory max_waiters must be positive")
	}
	pool := &Pool{total: opts.Bytes, normal: opts.Bytes - burst, waitTimeout: opts.WaitTimeout, maxWaiters: opts.MaxWaiters}
	pool.observed = newObservations(opts)
	return pool, nil
}

func (p *Pool) NewOwner() *Owner {
	owner := &Owner{pool: p}
	return owner
}

func (o *Owner) NewLease() *Lease {
	lease := &Lease{owner: o}
	return lease
}

func (p *Pool) Limit() int64 { return p.total }

func (l *Lease) Bytes() int64 {
	p := l.owner.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	return l.bytes
}

// Grow acquires the entire allocation before allowing its caller to allocate.
// Response growth has priority over new requests. Only one owner may borrow
// the completion reserve, so it is not fragmented among unrelated operations.
func (l *Lease) Grow(ctx context.Context, bytes int64, phase Phase) error {
	l.growMu.Lock()
	defer l.growMu.Unlock()
	if bytes < 0 || (phase != Request && phase != Response) {
		return errors.New("invalid memory capacity acquisition")
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	p := l.owner.pool
	p.mu.Lock()
	if l.closed {
		p.mu.Unlock()
		return errors.New("memory lease is closed")
	}
	limit := p.total
	if phase == Request {
		limit = p.normal
	}
	if bytes > limit-l.owner.bytes {
		p.observed.rejected.WithLabelValues(string(phase), "oversize").Inc()
		p.mu.Unlock()
		return status.Error(codes.ResourceExhausted, "allocation exceeds process memory capacity")
	}
	if bytes == 0 {
		p.mu.Unlock()
		return nil
	}
	w := &waiter{lease: l, bytes: bytes, phase: phase, started: time.Now(), ready: make(chan struct{})}
	p.waiters = append(p.waiters, w)
	p.dispatch()
	if w.granted {
		p.observed.admitted.WithLabelValues(string(phase)).Inc()
		p.mu.Unlock()
		return nil
	}
	// Decoded arrivals must not accumulate outside the managed capacity. The
	// batch queue waits only after acquiring ordinary bytes. Responses may wait
	// because they already own an admitted request and need to finish it.
	if phase == Request || len(p.waiters) > p.maxWaiters {
		p.remove(w)
		p.observed.rejected.WithLabelValues(string(phase), "busy").Inc()
		p.mu.Unlock()
		return ErrBusy
	}
	p.mu.Unlock()
	waitCtx, cancel := context.WithTimeout(ctx, p.waitTimeout)
	defer cancel()
	select {
	case <-w.ready:
	case <-waitCtx.Done():
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if l.closed {
		return status.Error(codes.Canceled, "memory lease closed while waiting")
	}
	if err := waitCtx.Err(); err != nil {
		if w.granted {
			l.releaseLocked(min(bytes, l.bytes))
		} else {
			p.remove(w)
		}
		p.dispatch()
		p.observed.wait.WithLabelValues(string(phase), "canceled").Observe(time.Since(w.started).Seconds())
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		p.observed.rejected.WithLabelValues(string(phase), "wait_timeout").Inc()
		return status.Error(codes.ResourceExhausted, "timed out waiting for process memory capacity")
	}
	if !w.granted {
		p.remove(w)
		p.dispatch()
		return status.Error(codes.Canceled, "memory lease closed while waiting")
	}
	p.observed.admitted.WithLabelValues(string(phase)).Inc()
	p.observed.wait.WithLabelValues(string(phase), "admitted").Observe(time.Since(w.started).Seconds())
	return nil
}

func (p *Pool) remove(target *waiter) {
	for index, w := range p.waiters {
		if w == target {
			copy(p.waiters[index:], p.waiters[index+1:])
			p.waiters[len(p.waiters)-1] = nil
			p.waiters = p.waiters[:len(p.waiters)-1]
			return
		}
	}
}

func (p *Pool) dispatch() {
	for _, phase := range []Phase{Response, Request} {
		pendingResponse := false
		for index := 0; index < len(p.waiters); {
			w := p.waiters[index]
			if w.phase != phase {
				index++
				continue
			}
			limit := p.normal
			if phase == Response && (p.burstOwner == nil || p.burstOwner == w.lease.owner) {
				limit = p.total
			}
			if w.bytes > limit-p.used {
				pendingResponse = phase == Response
				index++
				continue
			}
			if p.used+w.bytes > p.normal {
				p.burstOwner = w.lease.owner
			}
			p.used += w.bytes
			if w.lease.opaque {
				p.opaque += w.bytes
			}
			w.lease.bytes += w.bytes
			w.lease.owner.bytes += w.bytes
			w.granted = true
			p.remove(w)
			close(w.ready)
		}
		if pendingResponse {
			return
		}
	}
}

func (l *Lease) releaseLocked(bytes int64) {
	p := l.owner.pool
	l.bytes -= bytes
	l.owner.bytes -= bytes
	p.used -= bytes
	if l.opaque {
		p.opaque -= bytes
	}
	if p.used <= p.normal || (p.burstOwner == l.owner && l.owner.bytes == 0) {
		p.burstOwner = nil
	}
}

func (l *Lease) Shrink(bytes int64) {
	p := l.owner.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if l.closed || bytes <= 0 {
		return
	}
	l.releaseLocked(min(bytes, l.bytes))
	p.dispatch()
}

func (l *Lease) Close() {
	p := l.owner.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	l.releaseLocked(l.bytes)
	for index := 0; index < len(p.waiters); {
		w := p.waiters[index]
		if w.lease != l {
			index++
			continue
		}
		p.remove(w)
		close(w.ready)
	}
	p.dispatch()
}

func (p *Pool) Used() int64 { p.mu.Lock(); defer p.mu.Unlock(); return p.used }

// MarkOpaque distinguishes capacity protecting an uninstrumented driver from
// document and transport allocations. Call it before growing a new lease.
func (l *Lease) MarkOpaque() {
	if l == nil {
		return
	}
	p := l.owner.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if !l.opaque {
		l.opaque = true
		p.opaque += l.bytes
	}
}
