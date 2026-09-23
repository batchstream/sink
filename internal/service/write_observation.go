package service

import (
	"context"
	"log/slog"
	"time"

	sink "github.com/batchstream/sink-protocol/sink/v1"
	sinkmetrics "github.com/batchstream/sink/internal/metrics"
)

type writeObservation struct {
	metrics    *sinkmetrics.Metrics
	store      string
	completion sink.CompletionMode
	reads      int
	writes     int
}

func (s *Server) newWriteObservation(req *sink.WriteRequest) *writeObservation {
	if req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		return nil
	}
	store := s.boundStore
	if s.metrics != nil {
		store = s.metrics.RequestStore(req)
	}
	observation := &writeObservation{metrics: s.metrics, store: store, completion: req.GetCompletionMode()}
	return observation
}

func (o *writeObservation) phase(phase string, started time.Time) {
	if o == nil {
		return
	}
	switch phase {
	case "storage_read":
		o.reads++
	case "storage_write":
		o.writes++
	}
	observation := sinkmetrics.WritePhaseObservation{Store: o.store, Completion: o.completion, Phase: phase, Duration: time.Since(started)}
	o.metrics.ObserveWritePhase(observation)
	level := slog.LevelDebug
	if observation.Duration > 5*time.Second {
		level = slog.LevelWarn
	}
	slog.Log(context.Background(), level, "Write phase completed", "component", "execution", "event", "write_phase_completed",
		"store", o.store, "phase", phase, "duration_ms", observation.Duration.Milliseconds())
}

func (o *writeObservation) finish() {
	if o == nil {
		return
	}
	o.metrics.ObserveWriteRounds(o.store, o.reads, o.writes)
}
