package backpressure

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func testController(t *testing.T, maximum int) *Controller {
	t.Helper()
	opts := Options{Store: "primary", Role: "engine", MaxConcurrent: maximum}
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	c.resumeAt = time.Time{}
	c.started = true
	c.random = rand.New(rand.NewPCG(1, 2))
	return c
}

func TestGrowthLatencyDecreaseAndRecovery(t *testing.T) {
	c := testController(t, 32)
	now := time.Now()
	for range 100 {
		now = now.Add(controlInterval)
		permit, _, _ := c.tryAcquire(now)
		if permit == nil {
			t.Fatal("healthy execution was blocked")
		}
		started := c.begin(write, 32)
		started.saturated = true // Model a saturated bounded dispatcher.
		c.observeAt(started, 10*time.Millisecond, healthy, now)
		permit.Release()
	}
	before := c.limit
	if before < 16 || before > 32 {
		t.Fatalf("healthy saturated load did not grow within its ceiling: %d", before)
	}
	for range 3 {
		now = now.Add(controlInterval)
		started := c.begin(write, 32)
		c.observeAt(started, 100*time.Millisecond, healthy, now)
	}
	if c.limit >= before || c.observed.slowdowns == 0 {
		t.Fatalf("latency growth did not decrease window: %d -> %d", before, c.limit)
	}
	low := c.limit
	for range 300 {
		now = now.Add(controlInterval)
		permit, _, _ := c.tryAcquire(now)
		if permit == nil {
			continue
		}
		started := c.begin(write, 32)
		started.saturated = true
		c.observeAt(started, 10*time.Millisecond, healthy, now)
		permit.Release()
	}
	if c.limit <= low {
		t.Fatalf("recovered Store did not regain concurrency: %d -> %d", low, c.limit)
	}
}

func TestOverloadPausesAndOldFlightCannotUndoDecrease(t *testing.T) {
	c := testController(t, 16)
	now := time.Now()
	c.limit = 8
	first, _, _ := c.tryAcquire(now)
	second, _, _ := c.tryAcquire(now)
	started := c.begin(read, 1)
	c.observeAt(started, time.Second, congested, now)
	pausedUntil := c.resumeAt
	if c.limit != 0 || c.inFlight != 2 {
		t.Fatalf("decrease revoked active work: window=%d active=%d", c.limit, c.inFlight)
	}
	c.observeAt(started, time.Second, congested, now)
	c.observeAt(started, time.Millisecond, healthy, now)
	if c.observed.overloads != 1 || c.resumeAt != pausedUntil || c.limit != 0 {
		t.Fatal("old flight changed new congestion epoch")
	}
	if permit, _, delay := c.tryAcquire(now); permit != nil || delay <= 0 {
		t.Fatal("paused Store admitted work")
	}
	first.Release()
	second.Release()
	second.Release()
	permit, _, _ := c.tryAcquire(pausedUntil)
	if permit == nil || c.limit != 4 || c.inFlight != 1 {
		t.Fatal("cooldown did not resume at the reduced window")
	}
	permit.Release()
}

func TestSlowerSuccessfulWorkKeepsMakingProgress(t *testing.T) {
	for _, maximum := range []int{1, 8} {
		t.Run(strconv.Itoa(maximum), func(t *testing.T) {
			c := testController(t, maximum)
			now := time.Now()
			for iteration := range 500 {
				now = now.Add(controlInterval)
				permit, _, delay := c.tryAcquire(now)
				if permit == nil {
					t.Fatalf("successful work stopped at iteration %d: limit=%d cooldown=%s", iteration, c.limit, delay)
				}
				started := c.begin(write, 1)
				started.saturated = true
				duration := 10 * time.Millisecond
				if iteration >= 100 {
					duration = 100 * time.Millisecond
				}
				c.observeAt(started, duration, healthy, now)
				permit.Release()
				if c.limit < 1 || !c.resumeAt.IsZero() || c.failures != 0 {
					t.Fatalf("latency alone paused successful work: limit=%d resume=%v failures=%d", c.limit, c.resumeAt, c.failures)
				}
			}
			if c.limit != maximum {
				t.Fatalf("permanently slower healthy work did not recover concurrency: %d, want %d", c.limit, maximum)
			}
			if maximum > 1 && c.observed.slowdowns == 0 {
				t.Fatal("slower work did not reduce concurrency")
			}
			// Explicit overload must still stop dispatch, even at the floor.
			c.limit = 1
			started := c.begin(write, 1)
			c.observeAt(started, time.Millisecond, congested, now)
			if permit, _, delay := c.tryAcquire(now); permit != nil || delay <= 0 {
				t.Fatal("overload at the minimum window did not enter cooldown")
			}
		})
	}
}

func TestStationaryLatencyVariationDoesNotKeepShrinkingWindow(t *testing.T) {
	c := testController(t, 8)
	now := time.Now()
	// A stable workload alternates fast and slow replies without any overload.
	// Comparing its recent mean against a baseline biased toward fast replies
	// would repeatedly misclassify the ordinary slow replies as congestion.
	durations := []time.Duration{20 * time.Millisecond, 20 * time.Millisecond, 80 * time.Millisecond, 80 * time.Millisecond}
	var trainedSlowdowns uint64
	for iteration := range 800 {
		duration := durations[iteration%len(durations)]
		now = now.Add(duration)
		permit, _, _ := c.tryAcquire(now)
		if permit == nil {
			t.Fatal("successful variable-latency work stopped making progress")
		}
		started := c.begin(write, 1)
		started.saturated = true
		c.observeAt(started, duration, healthy, now)
		permit.Release()
		if iteration == 399 {
			trainedSlowdowns = c.observed.slowdowns
		}
	}
	if c.limit != c.maximum || c.observed.slowdowns != trainedSlowdowns {
		t.Fatalf("stationary latency kept reducing concurrency: limit=%d, trained decreases=%d, final decreases=%d", c.limit, trainedSlowdowns, c.observed.slowdowns)
	}
}

func TestOperationAndBatchClassesDoNotCompareAbsoluteLatency(t *testing.T) {
	c := testController(t, 16)
	c.limit = 8
	now := time.Now()
	for range 100 {
		for _, kind := range []method{read, write, writeVisible, deleteRecords, query, count, scan} {
			for size := range sizeClasses {
				started := sample{method: kind, size: size, epoch: c.epoch}
				duration := time.Duration((int(kind)+1)*(size+1)) * 100 * time.Millisecond
				c.observeAt(started, duration, healthy, now)
			}
		}
	}
	if c.limit != 8 || c.observed.slowdowns != 0 {
		t.Fatal("independent normal operation costs caused congestion")
	}
}

func TestLowDemandDoesNotInflateWindow(t *testing.T) {
	c := testController(t, 64)
	c.limit = 4
	c.failures = 5
	now := time.Now()
	for range 100 {
		now = now.Add(time.Second)
		permit, _, _ := c.tryAcquire(now)
		started := c.begin(write, 1)
		c.observeAt(started, time.Millisecond, healthy, now)
		permit.Release()
	}
	if c.limit != 4 || c.failures != 0 {
		t.Fatalf("serial underloaded traffic grew window to %d", c.limit)
	}
}

func TestGrowthDoesNotHideLateOverload(t *testing.T) {
	c := testController(t, 16)
	c.limit = 4
	c.inFlight = 4
	now := time.Now()
	slow := c.begin(write, 1)
	for range 4 {
		fast := c.begin(read, 1)
		c.observeAt(fast, time.Millisecond, healthy, now)
	}
	if c.limit <= 4 {
		t.Fatal("test did not grow the window")
	}
	c.observeAt(slow, time.Second, congested, now.Add(time.Second))
	if c.limit != 0 {
		t.Fatal("a growth decision suppressed a late real overload")
	}
}

func TestAdmissionCancellationAndNoLostWakeup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := testController(t, 1)
		permit, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, err := c.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiting admission error: %v", err)
		}
		if c.observed.overloads != 0 || c.inFlight != 1 {
			t.Fatal("admission wait was counted as backend congestion")
		}
		done := make(chan *Permit, 1)
		go func() {
			next, _ := c.Acquire(t.Context())
			done <- next
		}()
		synctest.Wait()
		permit.Release()
		next := <-done
		admitted := next.Context(t.Context())
		_, nested, err := c.Admit(admitted, 0)
		if err != nil || nested != nil {
			t.Fatal("sequential execution tried to reacquire its permit")
		}
		next.Release()
		if c.inFlight != 0 {
			t.Fatal("permit leaked")
		}
	})
}

func TestMultiInstanceSharedBackendSimulation(t *testing.T) {
	for _, instances := range []int{1, 8, 100} {
		t.Run(strconv.Itoa(instances), func(t *testing.T) {
			const ticks = 60_000
			const latencyTicks = 20
			origin := time.Now()
			controllers := make([]*Controller, instances)
			for index := range controllers {
				c := testController(t, 64)
				c.random = rand.New(rand.NewPCG(uint64(index+1), 42))
				c.resumeAt = origin.Add(c.jitter(startupWindow))
				controllers[index] = c
			}
			type flight struct {
				permit  *Permit
				started sample
				due     int
				result  feedback
			}
			var active []flight
			occupancy := 0
			var applied [3]int
			var rejected [3]int
			startupPeak := 0
			served := make([]bool, instances)
			// Saturated clients share a backend whose capacity falls and recovers.
			for tick := range ticks {
				now := origin.Add(time.Duration(tick) * time.Millisecond)
				phase := tick / 20_000
				capacity := 8
				if phase == 1 {
					capacity = 2
				}
				remaining := active[:0]
				for _, call := range active {
					if call.due > tick {
						remaining = append(remaining, call)
						continue
					}
					c := call.permit.controller
					c.observeAt(call.started, latencyTicks*time.Millisecond, call.result, now)
					call.permit.Release()
					if call.result == healthy {
						occupancy--
						applied[phase]++
					} else {
						rejected[phase]++
					}
				}
				active = remaining
				for offset := range instances {
					index := (tick + offset) % instances
					c := controllers[index]
					for {
						permit, _, _ := c.tryAcquire(now)
						if permit == nil {
							break
						}
						result := congested
						if occupancy < capacity {
							result = healthy
							occupancy++
							served[index] = true
						}
						call := flight{permit: permit, started: c.begin(write, 32), due: tick + latencyTicks, result: result}
						active = append(active, call)
					}
				}
				if tick < 1500 {
					startupPeak = max(startupPeak, len(active))
				}
			}
			for _, call := range active {
				call.permit.Release()
			}
			t.Logf("instances=%d applied=%v overloads=%v startup_peak=%d", instances, applied, rejected, startupPeak)
			if applied[2] < 3*applied[1] || applied[2] < 4_000 {
				t.Fatal("shared backend failed to recover useful throughput")
			}
			if rejected[2] > applied[2]/2 {
				t.Fatal("instances persistently flooded recovered backend")
			}
			if instances == 100 && startupPeak >= instances/2 {
				t.Fatal("cold starts synchronized into a backend stampede")
			}
			progressing := 0
			for _, received := range served {
				if received {
					progressing++
				}
			}
			t.Logf("instances making progress within 60 simulated seconds: %d/%d", progressing, instances)
			// Local feedback has no strict fairness guarantee, particularly when
			// there are more instances than slots. Avoid persistent monopolization.
			if progressing < instances/2 {
				t.Fatal("too few instances made progress")
			}
		})
	}
}
