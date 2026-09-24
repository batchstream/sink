// Package backpressure controls Store dispatch using feedback from real work.
// All ready Store executions share one bounded FIFO. The controller owns no
// background goroutine, dependency probe or retry policy.
package backpressure

import (
	"container/list"
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
	initialConcurrent     = 4
	defaultMaxQueuedTasks = 10_000
	defaultMaxQueuedBytes = 128 << 20
	startupWindow         = time.Second
	controlInterval       = 250 * time.Millisecond
	minimumCooldown       = 200 * time.Millisecond
	maximumCooldown       = 10 * time.Second
)

// ErrQueueFull proves that admission did not start backend work.
var ErrQueueFull = status.Error(codes.ResourceExhausted, "Store waiting capacity is full")

type Options struct {
	Store string
	Role  string
	// MaxConcurrent is a local resource ceiling, not an estimate of DB capacity.
	MaxConcurrent  int
	MaxQueuedTasks int
	MaxQueuedBytes int
}

type Controller struct {
	mu             sync.Mutex
	limit          int
	inFlight       int
	maximum        int
	threshold      int
	resumeLimit    int
	epoch          uint64
	clean          int
	recovered      int
	failures       int
	demand         bool
	started        bool
	resumeAt       time.Time
	adjustAt       time.Time
	changed        chan struct{}
	random         *rand.Rand
	latency        [methodCount][sizeClasses]latency
	observed       observations
	waiters        list.List
	queuedBytes    int
	bufferedBytes  int
	maxQueuedTasks int
	maxQueuedBytes int
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
	started    time.Time
}

type permitKey struct{}

func New(opts Options) (*Controller, error) {
	if opts.MaxConcurrent == 0 {
		opts.MaxConcurrent = DefaultMaxConcurrent
	}
	if opts.MaxConcurrent < 1 || opts.MaxConcurrent > 4096 {
		return nil, errors.New("store max_concurrent must be between 1 and 4096")
	}
	if opts.MaxQueuedTasks < 0 || opts.MaxQueuedBytes < 0 {
		return nil, errors.New("store admission queue limits cannot be negative")
	}
	if opts.MaxQueuedTasks == 0 {
		opts.MaxQueuedTasks = defaultMaxQueuedTasks
	}
	if opts.MaxQueuedBytes == 0 {
		opts.MaxQueuedBytes = defaultMaxQueuedBytes
	}
	c := &Controller{
		maximum: opts.MaxConcurrent, threshold: min(16, opts.MaxConcurrent), resumeLimit: min(initialConcurrent, opts.MaxConcurrent),
		changed: make(chan struct{}), random: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		observed:       newObservations(opts),
		maxQueuedTasks: opts.MaxQueuedTasks, maxQueuedBytes: opts.MaxQueuedBytes,
	}
	return c, nil
}

// TryAcquire is used by the bounded Worker loop; it cannot bypass queued tasks.
// Engine dispatchers enqueue a Ticket instead. On failure, changed and delay describe when to retry;
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
	if c.waiters.Len() != 0 {
		c.demand = true
		return nil, c.changed, 0
	}
	return c.tryAcquireLocked(now)
}

func (c *Controller) tryAcquireLocked(now time.Time) (*Permit, <-chan struct{}, time.Duration) {
	ready, changed, delay := c.availability(now)
	if !ready {
		c.demand = true
		return nil, changed, delay
	}
	c.inFlight++
	c.observed.admitted++
	permit := &Permit{controller: c, started: now}
	return permit, nil, 0
}

// Ready lets startup and Worker prefetch observe capacity without reserving a
// slot or inventing demand. Engine execution tickets are polled separately.
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
	return ready, c.changed, max(0, c.resumeAt.Sub(now))
}

// Acquire waits only at the already bounded Kafka processing loop. Engine
// requests use Admit or enqueue a batch Ticket, never this unqueued wait path.
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

// Wait observes available capacity without reserving a permit. Engine uses it
// before serving; Worker uses it before polling so backlog stays in Kafka while
// consumer group heartbeats and rebalance callbacks continue.
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
		c.observed.permitDuration.Observe(max(0, time.Since(p.started).Seconds()))
		c.signal()
	})
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
	emitWait  time.Duration
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
	c.observed.emitSeconds[started.method] += max(0, started.emitWait.Seconds())
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
		// Compare recent latency with its long-term mean. Aging both directions
		// equally prevents ordinary fast replies from pulling the baseline
		// toward a minimum and making a stable, variable workload look congested.
		latency.baseline += 0.01 * (value - latency.baseline)
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
	if !started.saturated && !c.demand && c.waiters.Len() == 0 {
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
	// Latency growth still represents successful work. Keep one execution
	// available so a slower backend can establish its new baseline; only an
	// explicit overload or timeout should pause dispatch and grow cooldown.
	c.limit = c.threshold
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
