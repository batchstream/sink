package gateway

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type metrics struct {
	registry   *prometheus.Registry
	config     *prometheus.GaugeVec
	requests   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	inFlight   prometheus.Gauge
	routes     prometheus.Gauge
	downstream prometheus.Histogram
}

func newMetrics() *metrics {
	registry := prometheus.NewRegistry()
	inFlightOpts := prometheus.GaugeOpts{Name: "sink_gateway_in_flight_requests", Help: "Admitted public requests currently forwarding."}
	routesOpts := prometheus.GaugeOpts{Name: "sink_gateway_routes", Help: "Number of configured Store routes."}
	downstreamOpts := prometheus.HistogramOpts{Name: "sink_gateway_engine_duration_seconds", Help: "Engine forwarding latency, including transport failures.", Buckets: prometheus.DefBuckets}
	observed := &metrics{registry: registry, inFlight: prometheus.NewGauge(inFlightOpts), routes: prometheus.NewGauge(routesOpts), downstream: prometheus.NewHistogram(downstreamOpts)}
	configOpts := prometheus.GaugeOpts{Name: "sink_gateway_config_info", Help: "SHA-256 of the normalized routes loaded at startup."}
	observed.config = prometheus.NewGaugeVec(configOpts, []string{"sha256"})
	requestOpts := prometheus.CounterOpts{Name: "sink_gateway_requests_total", Help: "Completed public RPCs."}
	observed.requests = prometheus.NewCounterVec(requestOpts, []string{"method", "code"})
	durationOpts := prometheus.HistogramOpts{Name: "sink_gateway_request_duration_seconds", Help: "Public RPC latency.", Buckets: prometheus.DefBuckets}
	observed.duration = prometheus.NewHistogramVec(durationOpts, []string{"method"})
	processOpts := collectors.ProcessCollectorOpts{}
	registry.MustRegister(observed.config, observed.requests, observed.duration, observed.inFlight, observed.routes, observed.downstream, collectors.NewGoCollector(), collectors.NewProcessCollector(processOpts))
	return observed
}
func (s *Server) MetricsHandler() http.Handler {
	opts := promhttp.HandlerOpts{}
	return promhttp.HandlerFor(s.metrics.registry, opts)
}

func (s *Server) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, req)
		method := strings.TrimPrefix(info.FullMethod, "/sink.v1.Sink/")
		switch method {
		case "Read", "Write", "Delete", "Execute", "Query", "Count", "Scan":
			s.metrics.requests.WithLabelValues(method, status.Code(err).String()).Inc()
			s.metrics.duration.WithLabelValues(method).Observe(time.Since(started).Seconds())
		}
		return response, err
	}
}
