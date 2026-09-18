package capacity

import (
	"context"
	"math"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testPool(t testing.TB, percent int) *Pool {
	t.Helper()
	opts := Options{Bytes: 10000, BurstPercent: percent, WaitTimeout: time.Second}
	p, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func grow(t testing.TB, l *Lease, bytes int64, phase Phase) {
	t.Helper()
	if err := l.Grow(context.Background(), bytes, phase); err != nil {
		t.Fatal(err)
	}
}
func awaitWaiters(t *testing.T, p *Pool, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		n := len(p.waiters)
		p.mu.Unlock()
		if n == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("did not get %d waiters", count)
}
func assertEmpty(t testing.TB, p *Pool) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.used != 0 || len(p.waiters) != 0 || p.burstOwner != nil {
		t.Fatalf("leaked capacity: used=%d waiting=%d borrower=%v", p.used, len(p.waiters), p.burstOwner != nil)
	}
}

func TestResponsePriorityAndExclusiveReserve(t *testing.T) {
	p := testPool(t, 10)
	first, second := p.NewOwner().NewLease(), p.NewOwner().NewLease()
	grow(t, first, 5000, Request)
	grow(t, second, 4000, Request)
	newcomer := p.NewOwner().NewLease()
	if status.Code(newcomer.Grow(t.Context(), 1, Request)) != codes.ResourceExhausted {
		t.Fatal("request borrowed completion reserve")
	}
	grow(t, first, 500, Response)
	done := make(chan error, 1)
	go func() { done <- second.Grow(t.Context(), 700, Response) }()
	awaitWaiters(t, p, 1)
	// Even when some ordinary capacity becomes free, a queued response wins.
	first.Shrink(1000)
	awaitWaiters(t, p, 0)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if status.Code(newcomer.Grow(t.Context(), 1, Request)) != codes.ResourceExhausted {
		t.Fatal("new request overtook reserve borrower")
	}
	first.Close()
	second.Close()
	newcomer.Close()
	assertEmpty(t, p)
}

func TestBoundedWaitCancellationAndClose(t *testing.T) {
	for _, action := range []string{"cancel", "close", "timeout", "grant-cancel"} {
		t.Run(action, func(t *testing.T) {
			for range 30 {
				p := testPool(t, 10)
				p.waitTimeout = 10 * time.Millisecond
				held := p.NewOwner().NewLease()
				grow(t, held, 9000, Request)
				blocked := p.NewOwner().NewLease()
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- blocked.Grow(ctx, 2000, Response) }()
				awaitWaiters(t, p, 1)
				switch action {
				case "cancel":
					cancel()
				case "close":
					blocked.Close()
				case "grant-cancel":
					held.Close()
					cancel()
					blocked.Close()
				}
				err := <-done
				if action != "grant-cancel" && err == nil {
					t.Fatal("wait unexpectedly succeeded")
				}
				cancel()
				held.Close()
				blocked.Close()
				assertEmpty(t, p)
			}
		})
	}
}

func TestReserveIsNotFragmented(t *testing.T) {
	p := testPool(t, 10)
	first, second := p.NewOwner().NewLease(), p.NewOwner().NewLease()
	grow(t, first, 4500, Request)
	grow(t, second, 4500, Request)
	grow(t, first, 400, Response)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- second.Grow(ctx, 100, Response) }()
	awaitWaiters(t, p, 1)
	// Remaining reserve belongs to the first owner until it can finish.
	grow(t, first, 600, Response)
	first.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	second.Close()
	assertEmpty(t, p)
}

func TestImpossibleGrowthAndQueueBound(t *testing.T) {
	p := testPool(t, 10)
	p.maxWaiters = 1
	held := p.NewOwner().NewLease()
	grow(t, held, 9000, Request)
	if status.Code(held.Grow(t.Context(), 1001, Response)) != codes.ResourceExhausted {
		t.Fatal("impossible request waited")
	}
	pending := p.NewOwner().NewLease()
	done := make(chan error, 1)
	go func() { done <- pending.Grow(t.Context(), 2000, Response) }()
	awaitWaiters(t, p, 1)
	other := p.NewOwner().NewLease()
	if status.Code(other.Grow(t.Context(), 2000, Response)) != codes.ResourceExhausted {
		t.Fatal("wait queue unbounded")
	}
	held.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	pending.Close()
	other.Close()
	assertEmpty(t, p)
}

func TestScopeRetainsCanceledProducerAndCopies(t *testing.T) {
	p := testPool(t, 10)
	s := p.NewScope()
	if err := s.Admit(t.Context(), 1000); err != nil {
		t.Fatal(err)
	}
	if err := s.Output(t.Context(), 1500); err != nil {
		t.Fatal(err)
	}
	release := s.Retain()
	s.Release()
	p.mu.Lock()
	used := p.used
	p.mu.Unlock()
	if used != 4000 {
		t.Fatalf("premature release or wrong copy charge: %d", used)
	}
	release()
	release()
	assertEmpty(t, p)
}

func TestConcurrentAcquisitionsStayWithinLimit(t *testing.T) {
	p := testPool(t, 20)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 100 {
				l := p.NewOwner().NewLease()
				if l.Grow(t.Context(), 100, Request) == nil {
					_ = l.Grow(t.Context(), 300, Response)
				}
				p.mu.Lock()
				if p.used < 0 || p.used > p.total {
					t.Errorf("invalid use %d", p.used)
				}
				p.mu.Unlock()
				l.Close()
			}
		})
	}
	wg.Wait()
	assertEmpty(t, p)
}

func TestMetricsIncludeIdleZeroes(t *testing.T) {
	p := testPool(t, 10)
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(p); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "sink_memory_waiting_bytes" {
			if len(family.GetMetric()) != 2 {
				t.Fatal("missing idle phases")
			}
			for _, metric := range family.GetMetric() {
				if metric.GetGauge().GetValue() != 0 {
					t.Fatal("nonzero idle wait")
				}
			}
			return
		}
	}
	t.Fatal("missing waiting metric")
}

func TestDetectCapacity(t *testing.T) {
	for _, tc := range []struct {
		name, membership, mount, filename, value string
		soft, want                               int64
		source                                   string
	}{
		{"v2-parent", "0::/pod/child", "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw", "sys/fs/cgroup/pod/memory.max", "104857600", math.MaxInt64, 50 << 20, "cgroup"},
		{"v1", "1:cpu,memory:/pod", "1 0 0:1 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory", "sys/fs/cgroup/memory/pod/memory.limit_in_bytes", "209715200", math.MaxInt64, 100 << 20, "cgroup"},
		{"namespaced", "0::/", "1 0 0:1 /pod /sys/fs/cgroup rw - cgroup2 cgroup rw", "sys/fs/cgroup/memory.max", "104857600", math.MaxInt64, 50 << 20, "cgroup"},
		{"soft", "0::/", "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw", "sys/fs/cgroup/memory.max", "104857600", 40 << 20, 20 << 20, "gomemlimit"},
		{"unlimited", "0::/", "1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw", "sys/fs/cgroup/memory.max", "max", math.MaxInt64, 256 << 20, "fallback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := fstest.MapFS{}
			files["proc/self/cgroup"] = &fstest.MapFile{Data: []byte(tc.membership)}
			files["proc/self/mountinfo"] = &fstest.MapFile{Data: []byte(tc.mount)}
			files[tc.filename] = &fstest.MapFile{Data: []byte(tc.value)}
			got, source := detect(files, tc.soft)
			if got != tc.want || source != tc.source {
				t.Fatalf("got %d %s, want %d %s", got, source, tc.want, tc.source)
			}
		})
	}
}

func TestBatchOwnerTransfersOutputLifetimeAndUnusedCredit(t *testing.T) {
	p := testPool(t, 10)
	producer, caller := p.NewScope(), p.NewScope()
	if err := producer.Admit(t.Context(), 4000); err != nil {
		t.Fatal(err)
	}
	if err := caller.Admit(t.Context(), 5000); err != nil {
		t.Fatal(err)
	}
	working := producer.NewLease()
	grow(t, working, 500, Response)
	if err := caller.OutputFrom(t.Context(), producer, 200); err != nil {
		t.Fatal(err)
	}
	if p.Used() != 9900 {
		t.Fatal("batch output did not share the completion owner")
	}
	producer.Release()
	if p.Used() != 5400 {
		t.Fatal("producer released another caller's output")
	}
	if err := caller.EnsureOutput(t.Context(), 200); err != nil {
		t.Fatal(err)
	}
	if p.Used() != 5400 {
		t.Fatal("encoding charged transferred credit twice")
	}
	caller.ReleaseOutputFrom(producer, 100)
	if p.Used() != 5200 {
		t.Fatal("unused return credit was retained")
	}
	caller.Release()
	assertEmpty(t, p)
}
