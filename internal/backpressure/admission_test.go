package backpressure

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readAdmissionResult struct {
	index  int
	ctx    context.Context
	permit *Permit
	err    error
}

func readController(t *testing.T, maximum, requests, bytes int) *Controller {
	t.Helper()
	opts := Options{Store: "primary", Role: "engine", MaxConcurrent: maximum, MaxQueuedTasks: requests, MaxQueuedBytes: bytes}
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAdmissionFIFOAndCancellationReleaseEveryQueuePosition(t *testing.T) {
	for _, canceled := range []int{0, 1, 2} {
		t.Run(strconv.Itoa(canceled), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := readController(t, 1, 3, 48)
				held, err := c.Acquire(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer held.Release()
				results := make(chan readAdmissionResult, 3)
				var cancels []context.CancelFunc
				for index := range 3 {
					ctx, cancel := context.WithCancel(t.Context())
					cancels = append(cancels, cancel)
					defer cancel()
					go func() {
						admitted, permit, err := c.Admit(ctx, 16)
						result := readAdmissionResult{index: index, ctx: admitted, permit: permit, err: err}
						results <- result
					}()
					synctest.Wait()
				}
				if c.waiters.Len() != 3 || c.queuedBytes != 48 {
					t.Fatalf("queue is not bounded/accounted: requests=%d bytes=%d", c.waiters.Len(), c.queuedBytes)
				}
				goroutines := runtime.NumGoroutine()
				for range 1000 {
					_, permit, err := c.Admit(t.Context(), 1)
					if permit != nil || err != ErrQueueFull {
						t.Fatalf("full queue: permit=%v error=%v", permit, err)
					}
				}
				synctest.Wait()
				if runtime.NumGoroutine() > goroutines {
					t.Fatal("rejected requests created waiting goroutines")
				}
				cancels[canceled]()
				result := <-results
				if result.index != canceled || status.Code(result.err) != codes.Canceled || result.permit != nil {
					t.Fatalf("cancellation changed: %+v", result)
				}
				if c.waiters.Len() != 2 || c.queuedBytes != 32 || c.inFlight != 1 {
					t.Fatal("cancellation leaked queue capacity or revoked active work")
				}
				held.Release()
				for index := range 3 {
					if index == canceled {
						continue
					}
					result := <-results
					if result.err != nil || result.index != index || result.permit == nil {
						t.Fatalf("FIFO admission changed: %+v, want %d", result, index)
					}
					result.permit.Release()
				}
				synctest.Wait()
				if c.waiters.Len() != 0 || c.queuedBytes != 0 || c.inFlight != 0 || c.observed.queueCanceled != 1 || c.observed.queueAdmitted != 2 {
					t.Fatal("completed queue leaked state")
				}
			})
		})
	}
}

func TestAdmissionByteLimitAndCapacityReuse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 1, 10, 32)
		held, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { _, permit, err := c.Admit(ctx, 32); permit.Release(); result <- err }()
		synctest.Wait()
		for _, bytes := range []int{1, 33, int(^uint(0) >> 1)} {
			_, permit, err := c.Admit(t.Context(), bytes)
			if err != ErrQueueFull || permit != nil || c.queuedBytes != 32 || c.waiters.Len() != 1 {
				t.Fatalf("encoded-byte bound failed for %d: %v", bytes, err)
			}
		}
		cancel()
		if err := <-result; status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
		held.Release()
		_, permit, err := c.Admit(t.Context(), 32)
		if err != nil {
			t.Fatal(err)
		}
		permit.Release()
		permit.Release()
		if c.queuedBytes != 0 || c.waiters.Len() != 0 || c.inFlight != 0 {
			t.Fatal("byte reservations or permits leaked")
		}
	})
}

func TestAdmissionPreservesContextAndOnlyCallerEndsWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 1, 4, 64)
		held, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		type contextKey struct{}
		ctx := context.WithValue(t.Context(), contextKey{}, "retained")
		ctx, cancel := context.WithDeadline(ctx, time.Now().Add(2*time.Hour))
		defer cancel()
		wantDeadline, _ := ctx.Deadline()
		result := make(chan readAdmissionResult, 1)
		go func() {
			admitted, permit, err := c.Admit(ctx, 16)
			value := readAdmissionResult{ctx: admitted, permit: permit, err: err}
			result <- value
		}()
		synctest.Wait()
		time.Sleep(time.Hour)
		synctest.Wait()
		select {
		case value := <-result:
			t.Fatalf("an internal timeout ended waiting: %v", value.err)
		default:
		}
		held.Release()
		value := <-result
		deadline, _ := value.ctx.Deadline()
		if value.err != nil || deadline != wantDeadline || value.ctx.Value(contextKey{}) != "retained" || c.queuedBytes != 0 {
			t.Fatalf("caller context or queue lifetime changed: %+v", value)
		}
		value.permit.Release()

		held, err = c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		short, shortCancel := context.WithDeadline(t.Context(), time.Now().Add(time.Minute))
		defer shortCancel()
		_, permit, err := c.Admit(short, 16)
		if status.Code(err) != codes.DeadlineExceeded || permit != nil || c.waiters.Len() != 0 || c.queuedBytes != 0 {
			t.Fatalf("caller deadline did not release queue: %v", err)
		}
	})
}

func TestQueuedHealthyReadsGrowWithoutRejectingRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 16, 512, 8192)
		if err := c.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		c.limit = 1
		results := make(chan error, 512)
		var mu sync.Mutex
		active, peak := 0, 0
		for range 512 {
			go func() {
				_, permit, err := c.Admit(t.Context(), 16)
				if err != nil {
					results <- err
					return
				}
				mu.Lock()
				active++
				peak = max(peak, active)
				mu.Unlock()
				started := c.begin(query, 1)
				time.Sleep(20 * time.Millisecond)
				c.observe(started, 20*time.Millisecond, healthy)
				mu.Lock()
				active--
				mu.Unlock()
				permit.Release()
				results <- nil
			}()
		}
		for range 512 {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if peak < 3 || peak > 16 || active != 0 || c.observed.increases == 0 || c.observed.rejected != 0 || c.waiters.Len() != 0 || c.queuedBytes != 0 || c.inFlight != 0 {
			t.Fatalf("healthy demand did not grow safely: peak=%d increases=%d rejected=%d", peak, c.observed.increases, c.observed.rejected)
		}
	})
}

func TestAdmissionWaitsThroughCooldownWithoutRevokingActiveWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 4, 2, 32)
		held, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		started := c.begin(query, 1)
		c.observe(started, time.Millisecond, congested)
		resume := c.resumeAt
		result := make(chan readAdmissionResult, 1)
		go func() {
			ctx, permit, err := c.Admit(t.Context(), 16)
			value := readAdmissionResult{ctx: ctx, permit: permit, err: err}
			result <- value
		}()
		synctest.Wait()
		if c.limit != 0 || c.inFlight != 1 || c.waiters.Len() != 1 {
			t.Fatal("cooldown revoked admitted work or let waiting work bypass")
		}
		time.Sleep(time.Until(resume) / 2)
		select {
		case value := <-result:
			t.Fatalf("read did not wait for overload recovery: %v", value.err)
		default:
		}
		held.Release()
		value := <-result
		if value.err != nil || time.Now().Before(resume) || c.limit != 2 || c.observed.rejected != 0 {
			t.Fatal("cooldown recovery was bypassed or rejected healthy waiting work")
		}
		value.permit.Release()
	})
}

func TestAdmissionValidationNilAndReusedPermit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, opts := range []Options{{MaxQueuedTasks: -1}, {MaxQueuedBytes: -1}} {
			if _, err := New(opts); err == nil {
				t.Fatal("negative queue limit accepted")
			}
		}
		c := readController(t, 1, 1, 16)
		_, permit, err := c.Admit(t.Context(), -1)
		if status.Code(err) != codes.InvalidArgument || permit != nil {
			t.Fatal("negative encoded size accepted")
		}
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		_, permit, err = c.Admit(canceled, 1)
		if status.Code(err) != codes.Canceled || permit != nil || c.observed.admitted != 0 {
			t.Fatal("already canceled request was admitted")
		}
		var absent *Controller
		admitted, permit, err := absent.Admit(t.Context(), 1)
		if err != nil || permit != nil || admitted != t.Context() {
			t.Fatal("nil admission changed component behavior")
		}
		held, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		ctx := held.Context(t.Context())
		admitted, permit, err = c.Admit(ctx, 1)
		if err != nil || permit != nil || admitted != ctx || c.inFlight != 1 || c.waiters.Len() != 0 {
			t.Fatal("nested admission deadlocked or double-counted an existing permit")
		}
	})
}

func TestAdmissionMetricsSeparateWaitingAndPermitHold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 1, 1, 16)
		held, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		registry := prometheus.NewPedanticRegistry()
		if err := registry.Register(c); err != nil {
			t.Fatal(err)
		}
		result := make(chan *Permit, 1)
		go func() {
			_, permit, err := c.Admit(t.Context(), 16)
			if err != nil {
				t.Error(err)
			}
			result <- permit
		}()
		synctest.Wait()
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			switch family.GetName() {
			case "sink_store_admission_queued_tasks":
				if family.Metric[0].GetGauge().GetValue() != 1 {
					t.Fatal("missing waiting request gauge")
				}
			case "sink_store_admission_queued_bytes":
				if family.Metric[0].GetGauge().GetValue() != 16 {
					t.Fatal("missing waiting bytes gauge")
				}
			}
		}
		time.Sleep(time.Minute)
		held.Release()
		permit := <-result
		time.Sleep(time.Second)
		permit.Release()
		permit.Release()
		families, err = registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			switch family.GetName() {
			case "sink_store_admission_queue_duration_seconds":
				for _, metric := range family.Metric {
					for _, label := range metric.Label {
						if label.GetName() == "outcome" && label.GetValue() == "admitted" && (metric.GetHistogram().GetSampleCount() != 1 || metric.GetHistogram().GetSampleSum() != 60) {
							t.Fatal("queue time includes execution or was lost")
						}
					}
				}
			case "sink_store_permit_hold_duration_seconds":
				if family.Metric[0].GetHistogram().GetSampleCount() != 2 || family.Metric[0].GetHistogram().GetSampleSum() != 61 {
					t.Fatal("permit hold time includes queue time or double release")
				}
			}
		}
	})
}

func TestAdmissionCancelReleaseContentionDoesNotLeak(t *testing.T) {
	c := readController(t, 4, 256, 4096)
	if err := c.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for index := range 256 {
		workers.Go(func() {
			for iteration := range 16 {
				ctx, cancel := context.WithCancel(t.Context())
				if (index+iteration)%3 == 0 {
					cancel()
				} else if (index+iteration)%3 == 1 {
					go cancel()
				}
				_, permit, err := c.Admit(ctx, 16)
				if err != nil && status.Code(err) != codes.Canceled {
					t.Errorf("bounded contention lost a request: %v", err)
				}
				permit.Release()
				cancel()
			}
		})
	}
	workers.Wait()
	if c.inFlight != 0 || c.waiters.Len() != 0 || c.queuedBytes != 0 {
		t.Fatalf("race leaked admission: active=%d queued=%d bytes=%d", c.inFlight, c.waiters.Len(), c.queuedBytes)
	}
}
