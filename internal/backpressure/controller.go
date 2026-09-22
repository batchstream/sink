// Package backpressure controls Store dispatch using feedback from real work.
// It owns no queue, background goroutine, dependency probe or retry policy.
package backpressure

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const DefaultMaxConcurrent = 64

const (
	startupWindow   = time.Second
	controlInterval = 250 * time.Millisecond
	minimumCooldown = 200 * time.Millisecond
	maximumCooldown = 10 * time.Second
)

// ErrBusy proves that this admission did not start backend work.
var ErrBusy = status.Error(codes.ResourceExhausted, "Store admission is temporarily full or cooling down")

type Options struct {
	Store string
	Role  string
	// MaxConcurrent is a local resource ceiling, not an estimate of DB capacity.
	MaxConcurrent int
}

type Controller struct {
	mu          sync.Mutex
	limit       int
	inFlight    int
	maximum     int
	threshold   int
	resumeLimit int
	epoch       uint64
	clean       int
	recovered   int
	failures    int
	demand      bool
	started     bool
	resumeAt    time.Time
	adjustAt    time.Time
	changed     chan struct{}
	random      *rand.Rand
	latency     [methodCount][sizeClasses]latency
	observed    observations
}

type latency struct {
	baseline float64
	short    float64
	samples  int
	slow     int
}

// Permit reserves one sequential execution, including its existing conflict
// retries. Reducing the window never revokes already admitted executions.
type Permit struct {
	controller *Controller
	once       sync.Once
}

type permitKey struct{}

func New(opts Options) (*Controller, error) {
	if opts.MaxConcurrent == 0 {
		opts.MaxConcurrent = DefaultMaxConcurrent
	}
	if opts.MaxConcurrent < 1 || opts.MaxConcurrent > 4096 {
		return nil, errors.New("store max_concurrent must be between 1 and 4096")
	}
	c := &Controller{
		maximum: opts.MaxConcurrent, threshold: min(16, opts.MaxConcurrent), resumeLimit: 1,
		changed: make(chan struct{}), random: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		observed: newObservations(opts),
	}
	return c, nil
}

// TryAcquire is used by bounded dispatchers before removing queued work or
// starting a goroutine. On failure, changed and delay describe when to retry;
// delay == 0 means only a completion or another state change can free capacity.
func (c *Controller) TryAcquire() (*Permit, <-chan struct{}, time.Duration) {
	return c.tryAcquire(time.Now())
}

func (c *Controller) tryAcquire(now time.Time) (*Permit, <-chan struct{}, time.Duration) {
	if c == nil {
		permit := &Permit{}
		return permit, nil, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ready, changed, delay := c.availability(now)
	if !ready {
		return nil, changed, delay
	}
	c.inFlight++
	c.observed.admitted++
	permit := &Permit{controller: c}
	return permit, nil, 0
}

// Ready lets a dispatcher avoid rebuilding a batch while capacity is occupied,
// and lets Worker avoid pausing prefetch on every healthy poll. It reserves no
// slot; callers must still use TryAcquire/Acquire before dispatch.
func (c *Controller) Ready() (bool, <-chan struct{}, time.Duration) {
	if c == nil {
		return true, nil, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.availability(time.Now())
}

func (c *Controller) availability(now time.Time) (bool, <-chan struct{}, time.Duration) {
	c.resume(now)
	ready := c.limit > c.inFlight
	if !ready {
		c.demand = true
	}
	return ready, c.changed, max(0, c.resumeAt.Sub(now))
}

// Acquire may wait only at an already bounded caller (the Kafka processing
// loop). Engine RPCs use the existing batch queue or fail-fast Admit instead.
func (c *Controller) Acquire(ctx context.Context) (*Permit, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		permit, changed, delay := c.TryAcquire()
		if permit != nil {
			return permit, nil
		}
		if err := wait(ctx, changed, delay); err != nil {
			return nil, err
		}
	}
}

// Wait leaves the next poll in Kafka. It reserves nothing while polling and
// cannot prevent consumer group heartbeats or rebalance callbacks.
func (c *Controller) Wait(ctx context.Context) error {
	if c == nil {
		return ctx.Err()
	}
	for {
		ready, changed, delay := c.Ready()
		if ready {
			return ctx.Err()
		}
		if err := wait(ctx, changed, delay); err != nil {
			return err
		}
	}
}

func wait(ctx context.Context, changed <-chan struct{}, delay time.Duration) error {
	var timer *time.Timer
	var deadline <-chan time.Time
	if delay > 0 {
		timer = time.NewTimer(delay)
		deadline = timer.C
		defer timer.Stop()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
	case <-deadline:
	}
	return nil
}

func (p *Permit) Context(ctx context.Context) context.Context {
	if p == nil || p.controller == nil {
		return ctx
	}
	return context.WithValue(ctx, permitKey{}, p)
}

func (p *Permit) Release() {
	if p == nil || p.controller == nil {
		return
	}
	p.once.Do(func() {
		c := p.controller
		c.mu.Lock()
		defer c.mu.Unlock()
		c.inFlight--
		c.signal()
	})
}

// Admit reuses the dispatch permit within a sequential execution. A direct
// request without a queue is rejected before execution if capacity is absent.
func (c *Controller) Admit(ctx context.Context) (context.Context, *Permit, error) {
	if err := ctx.Err(); err != nil {
		return ctx, nil, status.FromContextError(err).Err()
	}
	if c == nil {
		return ctx, nil, nil
	}
	if permit, ok := ctx.Value(permitKey{}).(*Permit); ok && permit.controller == c {
		return ctx, nil, nil
	}
	permit, _, _ := c.TryAcquire()
	if permit == nil {
		c.mu.Lock()
		c.observed.rejected++
		c.mu.Unlock()
		return ctx, nil, ErrBusy
	}
	return permit.Context(ctx), permit, nil
}

// All state transitions below run with mu held. No timers mutate the state.
func (c *Controller) resume(now time.Time) {
	if !c.started {
		c.started = true
		c.resumeAt = now.Add(c.jitter(startupWindow))
	}
	if c.limit == 0 && !now.Before(c.resumeAt) {
		c.limit = c.resumeLimit
		c.resumeAt = time.Time{}
		c.adjustAt = now.Add(c.jitter(controlInterval))
		c.signal()
	}
}

func (c *Controller) signal() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *Controller) jitter(base time.Duration) time.Duration {
	return base/2 + time.Duration(c.random.Int64N(int64(base)))
}

type sample struct {
	method    method
	size      int
	epoch     uint64
	saturated bool
}

func (c *Controller) begin(method method, operations int) sample {
	c.mu.Lock()
	defer c.mu.Unlock()
	started := sample{method: method, size: sizeClass(operations), epoch: c.epoch, saturated: c.inFlight >= c.limit}
	return started
}

func (c *Controller) observe(started sample, duration time.Duration, result feedback) {
	c.observeAt(started, duration, result, time.Now())
}

func (c *Controller) observeAt(started sample, duration time.Duration, result feedback, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observed.samples[started.method][result]++
	c.observed.seconds[started.method] += max(0, duration.Seconds())
	// Replies from the same pre-decrease flight cannot repeatedly collapse the
	// window, or undo the decrease with old successes.
	if started.epoch != c.epoch || c.limit == 0 {
		return
	}
	if result == congested {
		c.decrease(now, true)
		return
	}
	if result != healthy {
		return
	}
	// Opaque Execute commands have incomparable costs, even at the same URI.
	// Successful calls still drive growth, but only errors can reduce the window.
	if started.method != execute {
		latency := &c.latency[started.method][started.size]
		value := float64(max(duration, time.Microsecond))
		if latency.samples == 0 {
			latency.baseline, latency.short = value, value
		}
		latency.samples = min(latency.samples+1, 1000)
		latency.short += (value - latency.short) / 4
		// Slow upward aging also allows a permanent workload/latency change to
		// establish a new baseline. Fast downward adaptation follows recovery.
		weight := 0.01
		if value < latency.baseline {
			weight = 0.25
		}
		latency.baseline += weight * (value - latency.baseline)
		if latency.samples >= 4 && latency.short > max(1.5*latency.baseline, latency.baseline+float64(5*time.Millisecond)) {
			latency.slow++
		} else {
			latency.slow = 0
		}
		if latency.slow >= 2 {
			latency.slow = 0
			latency.short = latency.baseline
			c.decrease(now, false)
			return
		}
		if latency.slow != 0 {
			return
		}
	}
	c.recovered = min(4, c.recovered+1)
	if c.recovered == 4 {
		c.failures = 0
	}
	if !started.saturated && !c.demand {
		return
	}
	c.clean++
	if c.clean < max(4, c.limit) || now.Before(c.adjustAt) {
		return
	}
	c.failures = 0
	c.clean = 0
	c.demand = false
	step := 1
	if c.limit < c.threshold {
		step = max(1, c.limit/2)
	}
	next := min(c.maximum, c.limit+step)
	c.adjustAt = now.Add(c.jitter(controlInterval))
	if next == c.limit {
		return
	}
	c.limit = next
	c.observed.increases++
	c.signal()
}

func (c *Controller) decrease(now time.Time, overload bool) {
	c.threshold = max(1, c.limit/2)
	c.resumeLimit = c.threshold
	c.limit /= 2
	c.clean = 0
	c.recovered = 0
	c.demand = false
	c.epoch++
	if overload {
		c.limit = 0
		c.observed.overloads++
	} else {
		c.observed.slowdowns++
	}
	delay := c.jitter(controlInterval)
	if c.limit == 0 {
		c.failures = min(c.failures+1, 7)
		delay = c.jitter(min(maximumCooldown, minimumCooldown<<(c.failures-1)))
		c.resumeAt = now.Add(delay)
	}
	c.adjustAt = now.Add(delay)
	c.signal()
}
