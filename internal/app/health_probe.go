package app

import (
	"context"
	"log/slog"
	"strings"

	"github.com/liran/sink/internal/logging"
)

type healthAttempt struct {
	done chan struct{}
	err  error
}

// Some dependency clients can wait behind an earlier network request even
// after a probe context expires. Return to callers on time while retaining at
// most one outstanding probe per dependency. Never cache a completed result.
func (h *configuredHealthCheck) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.mu.Lock()
	attempt := h.active
	if attempt == nil {
		attempt = &healthAttempt{done: make(chan struct{})}
		h.active = attempt
		go func() {
			// One caller cancelling must not invalidate a shared probe for
			// other readiness requests. The dependency still gets its deadline.
			probeContext, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
			defer cancel()
			attempt.err = h.pinger.Ping(probeContext)
			state := int32(1)
			if attempt.err != nil {
				state = 2
			}
			previous := h.lastState.Swap(state)
			if previous != state {
				dependency := "storage"
				if strings.HasPrefix(h.service, "sink.kafka.") {
					dependency = "kafka"
				}
				if strings.HasPrefix(h.service, "sink.worker.") {
					dependency = "worker"
				}
				if state == 2 {
					slog.Warn("Dependency unavailable", "component", "health", "event", "dependency_unavailable", "reason", dependency, "error_type", logging.ErrorType(attempt.err))
				} else if previous == 2 {
					slog.Info("Dependency recovered", "component", "health", "event", "dependency_recovered", "reason", dependency)
				}
			}
			h.mu.Lock()
			h.active = nil
			close(attempt.done)
			h.mu.Unlock()
		}()
	}
	h.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-attempt.done:
		return attempt.err
	}
}
