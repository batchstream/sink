package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/batchstream/sink/internal/config"
)

func TestHTTPReadinessRejectsClosedRoles(t *testing.T) {
	for _, mode := range []config.Mode{config.ModeGateway, config.ModeEngine, config.ModeWorker} {
		t.Run(string(mode), func(t *testing.T) {
			settings := config.Config{Mode: mode}
			app := &Application{config: settings}
			app.Close()
			for _, endpoint := range []string{"/readyz", "/readyz?service=sink.storage.primary"} {
				request := httptest.NewRequest(http.MethodGet, endpoint, nil)
				response := httptest.NewRecorder()
				app.serveReadiness(response, request)
				if response.Code != http.StatusServiceUnavailable {
					t.Errorf("%s after Close: got %d, want 503", endpoint, response.Code)
				}
			}
		})
	}
}

func TestReadinessProbeCannotRestoreClosedRole(t *testing.T) {
	probe := &stalledHealthProbe{started: make(chan struct{}), release: make(chan struct{})}
	check := &configuredHealthCheck{service: "blocked", pinger: probe}
	settings := config.Config{Mode: config.ModeEngine}
	app := &Application{config: settings, healthChecks: []*configuredHealthCheck{check}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz?service=blocked", nil)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { app.serveReadiness(response, request); close(done) }()
	defer func() { close(probe.release); <-done }()
	select {
	case <-probe.started:
	case <-ctx.Done():
		t.Fatal("readiness did not start its dependency probe")
	}
	app.Close()
	// Release a successful dependency result after shutdown has begun. A late
	// result must not advertise that the closed process can accept new work.
	probe.release <- struct{}{}
	select {
	case <-done:
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("late successful probe restored readiness: %d", response.Code)
		}
	case <-ctx.Done():
		t.Fatal("readiness did not finish after the dependency recovered")
	}
}
