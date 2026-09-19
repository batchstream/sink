package app

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liran/sink/internal/config"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	healthCheckInterval = 5 * time.Second
	healthCheckTimeout  = 3 * time.Second
)

type healthPinger interface {
	Ping(context.Context) error
}

type configuredHealthCheck struct {
	lastState atomic.Int32
	service   string
	pinger    healthPinger
	mu        sync.Mutex
	active    *healthAttempt
}

type configuredHealthResult struct {
	service string
	status  healthpb.HealthCheckResponse_ServingStatus
}

func storageHealthService(store string) string {
	return "sink.storage." + store
}

func kafkaHealthService(store string) string {
	return "sink.kafka." + store
}

func (app *Application) runHealthChecks(ctx context.Context) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	for {
		app.updateHealth(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (app *Application) updateHealth(parent context.Context) {
	results := make(chan configuredHealthResult, len(app.healthChecks))
	for _, configured := range app.healthChecks {
		go func() {
			ctx, cancel := context.WithTimeout(parent, healthCheckTimeout)
			defer cancel()
			status := healthpb.HealthCheckResponse_SERVING
			if err := configured.check(ctx); err != nil {
				status = healthpb.HealthCheckResponse_NOT_SERVING
			}
			result := configuredHealthResult{service: configured.service, status: status}
			results <- result
		}()
	}
	for range app.healthChecks {
		result := <-results
		app.health.SetServingStatus(result.service, result.status)
	}
}

func (app *Application) serveReadiness(w http.ResponseWriter, r *http.Request) {
	if app.draining.Load() {
		http.Error(w, "process is shutting down", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
	defer cancel()
	selected := r.URL.Query().Get("service")
	if selected == "" && (app.config.Mode == config.ModeEngine || app.config.Mode == config.ModeGateway) {
		w.WriteHeader(http.StatusOK)
		return
	}
	checks := 0
	failures := make(chan string, len(app.healthChecks))
	var work sync.WaitGroup
	for _, configured := range app.healthChecks {
		if selected != "" && selected != configured.service {
			continue
		}
		checks++
		work.Go(func() {
			if err := configured.check(ctx); err != nil {
				failures <- configured.service
			}
		})
	}
	work.Wait()
	close(failures)
	if app.draining.Load() {
		http.Error(w, "process is shutting down", http.StatusServiceUnavailable)
		return
	}
	if checks == 0 && selected != "" {
		http.Error(w, "unknown health service", http.StatusNotFound)
		return
	}
	if len(failures) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		for service := range failures {
			_, _ = fmt.Fprintln(w, service)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
}
