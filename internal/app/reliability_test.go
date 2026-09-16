package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liran/sink/internal/config"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type stalledHealthProbe struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (p *stalledHealthProbe) Ping(context.Context) error {
	if p.calls.Add(1) == 1 {
		close(p.started)
	}
	<-p.release
	return nil
}

func TestReadinessHonorsDeadlineWhenDependencyIgnoresCancellation(t *testing.T) {
	probe := &stalledHealthProbe{started: make(chan struct{}), release: make(chan struct{})}
	check := &configuredHealthCheck{service: "blocked", pinger: probe}
	app := &Application{healthChecks: []*configuredHealthCheck{check}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { app.serveReadiness(response, request); close(done) }()
	defer func() { close(probe.release); <-done }()
	<-probe.started
	select {
	case <-done:
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("stalled dependency readiness = %d", response.Code)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("readiness waited for dependency beyond request deadline")
	}
}

func TestReadinessSharesOutstandingProbeAndRecovers(t *testing.T) {
	probe := &stalledHealthProbe{started: make(chan struct{}), release: make(chan struct{})}
	check := &configuredHealthCheck{service: "blocked", pinger: probe}
	var callers sync.WaitGroup
	for range 32 {
		callers.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			if err := check.check(ctx); err != context.DeadlineExceeded {
				t.Errorf("stalled probe result: %v", err)
			}
		})
	}
	callers.Wait()
	if calls := probe.calls.Load(); calls != 1 {
		t.Errorf("32 timed-out callers started %d probes, want one outstanding probe", calls)
	}
	check.mu.Lock()
	attempt := check.active
	check.mu.Unlock()
	close(probe.release)
	if attempt == nil {
		t.Fatal("stalled probe was discarded before its completion")
	}
	<-attempt.done
	if err := check.check(t.Context()); err != nil {
		t.Fatalf("readiness did not recover: %v", err)
	}
	if calls := probe.calls.Load(); calls != 2 {
		t.Fatalf("completed probe was cached: calls=%d", calls)
	}
}

func TestUnavailableStoreDoesNotBlockStartup(t *testing.T) {
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer unavailable.Close()
	contents := fmt.Sprintf(`mode: engine
grpc:
  address: "127.0.0.1:0"
storage:
  name: failed
  database_id: failed-database
  driver: opensearch
  search:
    endpoints: [%q]
`, unavailable.URL)
	loaded, err := config.Decode(strings.NewReader(contents))
	if err != nil {
		t.Fatal(err)
	}
	options := Options{Config: loaded, Version: "test"}
	app, err := New(t.Context(), options)
	if err != nil {
		t.Fatalf("dependency outage prevented startup: %v", err)
	}
	defer app.Close()
	app.health = health.NewServer()
	app.updateHealth(t.Context())
	assertHealthStatus(t, app.health, storageHealthService("failed"), healthpb.HealthCheckResponse_NOT_SERVING)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()
	app.serveReadiness(response, request)
	if response.Code != http.StatusOK {
		t.Fatal("dependency outage prevented process readiness")
	}
}

func TestMongoClientStartsWithoutRequiringAvailability(t *testing.T) {
	mongoConfig := config.MongoDB{URI: "mongodb://127.0.0.1:1/?w=1&journal=false"}
	configured := config.Storage{Name: "primary", Driver: config.DriverMongoDB, MongoDB: mongoConfig}
	opened, err := openMongoStorage(t.Context(), configured, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.mongoClient.Disconnect(t.Context())

}
