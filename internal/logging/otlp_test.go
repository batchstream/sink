package logging

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type captureCollector struct {
	collector.UnimplementedLogsServiceServer
	requests chan *collector.ExportLogsServiceRequest
}

func (c *captureCollector) Export(_ context.Context, request *collector.ExportLogsServiceRequest) (*collector.ExportLogsServiceResponse, error) {
	c.requests <- request
	response := &collector.ExportLogsServiceResponse{}
	return response, nil
}

func TestOTLPWireSurvivesIngestionProjection(t *testing.T) {
	for _, protocol := range []string{"grpc", "http/protobuf"} {
		t.Run(protocol, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://127.0.0.1:1/ignored")
			t.Setenv("OTEL_EXPORTER_OTLP_LOGS_HEADERS", "authorization=ignored-test-value")
			requests := make(chan *collector.ExportLogsServiceRequest, 8)
			endpoint := ""
			if protocol == "grpc" {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				server := grpc.NewServer()
				service := &captureCollector{requests: requests}
				collector.RegisterLogsServiceServer(server, service)
				go func() { _ = server.Serve(listener) }()
				t.Cleanup(server.Stop)
				endpoint = listener.Addr().String()
			} else {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Authorization") != "" {
						t.Error("inherited unspecified exporter headers")
					}
					if r.URL.Path != "/v1/logs" || r.Header.Get("Content-Type") != "application/x-protobuf" {
						t.Errorf("invalid HTTP OTLP request: %s", r.URL.Path)
					}
					raw, _ := io.ReadAll(r.Body)
					request := &collector.ExportLogsServiceRequest{}
					if err := proto.Unmarshal(raw, request); err != nil {
						t.Error(err)
					}
					requests <- request
					w.Header().Set("Content-Type", "application/x-protobuf")
				}))
				t.Cleanup(server.Close)
				endpoint = strings.TrimPrefix(server.URL, "http://")
			}
			cfg := testConfig(t)
			cfg.OTLP.Enabled = true
			cfg.OTLP.TLS = false
			cfg.OTLP.Endpoint = endpoint
			cfg.OTLP.Protocol = protocol
			cfg.FailureBody = true
			cfg.Labels = map[string]string{"cluster": "eks", "environment": "production"}
			output := &lockedBuffer{}
			runtime := newTestRuntime(t, cfg, output)
			tid, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
			sid, _ := trace.SpanIDFromHex("0123456789abcdef")
			spanOptions := trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}
			ctx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(spanOptions))
			body := &FailureBody{Encoding: "json", Payload: []byte(`{"failed_document":true}`)}
			runtime.Logger.Info("hidden")
			runtime.Logger.ErrorContext(ctx, "quarantined", "component", "kafka", "event", "kafka_quarantined", "operations", 3, "failure_body", body)
			runtime.Close()
			select {
			case request := <-requests:
				var records []*logs.LogRecord
				for _, resource := range request.ResourceLogs {
					for _, scope := range resource.ScopeLogs {
						records = append(records, scope.LogRecords...)
					}
				}
				if len(records) != 1 {
					t.Fatalf("expected one log, got %d", len(records))
				}
				record := records[0]
				// The production consumer ignores Resource and severity and converts
				// only LogRecord.Attributes to map[string]string.
				labels := map[string]string{}
				for _, attribute := range record.Attributes {
					labels[attribute.Key] = attribute.Value.GetStringValue()
				}
				for key, want := range map[string]string{"service_name": "sink", "service_version": "test", "role": "engine", "store": "primary", "level": "error", "component": "kafka", "event": "kafka_quarantined", "operations": "3", "cluster": "eks"} {
					if labels[key] != want {
						t.Errorf("ingestion lost %s: %q", key, labels[key])
					}
				}
				if labels["instance_id"] == "" || record.TimeUnixNano == 0 {
					t.Fatal("missing identity/timestamp")
				}
				if len(record.TraceId) != 0 || len(record.SpanId) != 0 {
					t.Fatal("trace context leaked")
				}
				if !strings.Contains(record.Body.GetStringValue(), `{"failed_document":true}`) || labels["failure_body"] != "" {
					t.Fatal("failure body missing or indexed as labels")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("no OTLP request")
			}
		})
	}
}

func TestCollectorOutageDoesNotBlockLoggingAndReportsQueueLoss(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	defer close(release)
	cfg := testConfig(t)
	cfg.Level = "debug"
	cfg.Console.Enabled = false
	cfg.OTLP.Enabled = true
	cfg.OTLP.Protocol = "http/protobuf"
	cfg.OTLP.TLS = false
	cfg.OTLP.Endpoint = strings.TrimPrefix(server.URL, "http://")
	cfg.OTLP.QueueSize = 8
	cfg.OTLP.BatchSize = 1
	cfg.OTLP.ExportTimeout = 200 * time.Millisecond
	cfg.OTLP.ShutdownTimeout = time.Second
	output := &lockedBuffer{}
	runtime := newTestRuntime(t, cfg, output)
	previousLogger := slog.Default()
	previousErrorHandler := otel.GetErrorHandler()
	runtime.Install()
	defer func() {
		slog.SetDefault(previousLogger)
		otel.SetErrorHandler(previousErrorHandler)
		otel.SetLogger(logr.Discard())
	}()
	runtime.Logger.Debug("first")
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("export never started")
	}
	started := time.Now()
	for i := 0; i < 200; i++ {
		runtime.Logger.Debug("queued", "operations", i)
	}
	if time.Since(started) > time.Second {
		t.Fatal("producer waited on OTLP network")
	}
	runtime.Close()
	if runtime.dropped.Load() == 0 {
		t.Fatal("SDK queue saturation was not counted")
	}
	if runtime.failed.Load() == 0 {
		t.Fatal("failed export batches were not counted")
	}
	if !strings.Contains(output.String(), "log_queue_dropped") {
		t.Fatalf("missing console-only loss diagnostic: %s", output.String())
	}
}

func TestShutdownDeadlineWhileCollectorIsUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := listener.Addr().String()
	_ = listener.Close()
	cfg := testConfig(t)
	cfg.OTLP.Enabled = true
	cfg.OTLP.TLS = false
	cfg.OTLP.Endpoint = endpoint
	cfg.OTLP.ExportTimeout = 100 * time.Millisecond
	cfg.OTLP.ShutdownTimeout = 30 * time.Millisecond
	output := &lockedBuffer{}
	started := time.Now()
	runtime := newTestRuntime(t, cfg, output)
	if time.Since(started) > time.Second {
		t.Fatal("startup waited for Collector")
	}
	runtime.Logger.Warn("retained")
	started = time.Now()
	runtime.Close()
	if time.Since(started) > time.Second {
		t.Fatal("shutdown exceeded bounded grace")
	}
	if !strings.Contains(output.String(), "log_shutdown_incomplete") {
		t.Fatal("missing shutdown diagnostic")
	}
}

func TestTLSNeverFallsBackToPlaintext(t *testing.T) {
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	cfg := testConfig(t)
	cfg.OTLP.Enabled = true
	cfg.OTLP.Protocol = "http/protobuf"
	cfg.OTLP.Endpoint = strings.TrimPrefix(server.URL, "http://")
	cfg.OTLP.ExportTimeout = 100 * time.Millisecond
	output := &lockedBuffer{}
	runtime := newTestRuntime(t, cfg, output)
	runtime.Logger.Error("must remain encrypted")
	runtime.Close()
	if received.Load() != 0 || runtime.failed.Load() == 0 {
		t.Fatal("TLS selection silently fell back to plaintext")
	}
}

func TestOTLPRecoversWithoutRestart(t *testing.T) {
	var ready atomic.Bool
	var accepted atomic.Int64
	failed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			select {
			case failed <- struct{}{}:
			default:
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		request := &collector.ExportLogsServiceRequest{}
		if err := proto.Unmarshal(raw, request); err != nil {
			t.Error(err)
		}
		for _, resource := range request.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				accepted.Add(int64(len(scope.LogRecords)))
			}
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	defer server.Close()
	cfg := testConfig(t)
	cfg.OTLP.Enabled = true
	cfg.OTLP.TLS = false
	cfg.OTLP.Protocol = "http/protobuf"
	cfg.OTLP.Endpoint = strings.TrimPrefix(server.URL, "http://")
	cfg.OTLP.BatchSize = 1
	output := &lockedBuffer{}
	runtime := newTestRuntime(t, cfg, output)
	runtime.Logger.Warn("before recovery")
	select {
	case <-failed:
	case <-time.After(2 * time.Second):
		t.Fatal("no initial export")
	}
	ready.Store(true)
	runtime.Logger.Warn("after recovery")
	runtime.Close()
	if accepted.Load() != 2 {
		t.Fatalf("export did not recover: accepted %d records", accepted.Load())
	}
}
