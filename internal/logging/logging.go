package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/batchstream/sink/internal/config"
	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
)

type Options struct {
	Config  config.Logging
	Role    string
	Store   string
	Version string
	Stderr  io.Writer
}

type Runtime struct {
	Logger          *slog.Logger
	provider        *sdklog.LoggerProvider
	console         *slog.Logger
	limiter         *eventLimiter
	shutdownTimeout time.Duration
	dropped         atomic.Uint64
	failed          atomic.Uint64
}

func New(ctx context.Context, opts Options) (*Runtime, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(opts.Config.Level)); err != nil {
		return nil, err
	}
	output := opts.Stderr
	if output == nil {
		output = os.Stderr
	}
	consoleOptions := &slog.HandlerOptions{Level: slog.LevelDebug, ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
		// The lowercase string level below is shared with OTLP record attributes.
		if attr.Key == slog.LevelKey && attr.Value.Kind() == slog.KindAny {
			var empty slog.Attr
			return empty
		}
		return attr
	}}
	var console slog.Handler = slog.NewTextHandler(output, consoleOptions)
	if opts.Config.Console.Format == "json" {
		console = slog.NewJSONHandler(output, consoleOptions)
	}
	identity := identityAttrs(opts)
	diagnostics := &handler{level: slog.LevelDebug, outputs: []slog.Handler{console}, identity: identity, limiter: &eventLimiter{}}
	runtime := &Runtime{console: slog.New(diagnostics), limiter: &eventLimiter{}, shutdownTimeout: opts.Config.OTLP.ShutdownTimeout}
	outputs := make([]slog.Handler, 0, 2)
	if opts.Config.Console.Enabled {
		outputs = append(outputs, console)
	}
	if opts.Config.OTLP.Enabled {
		exporter, err := newExporter(ctx, opts.Config.OTLP)
		if err != nil {
			return nil, err
		}
		tracked := &trackedExporter{Exporter: exporter, runtime: runtime}
		processor := sdklog.NewBatchProcessor(tracked,
			sdklog.WithMaxQueueSize(opts.Config.OTLP.QueueSize),
			sdklog.WithExportMaxBatchSize(opts.Config.OTLP.BatchSize),
			sdklog.WithExportInterval(opts.Config.OTLP.FlushInterval),
			sdklog.WithExportTimeout(opts.Config.OTLP.ExportTimeout))
		runtime.provider = sdklog.NewLoggerProvider(sdklog.WithProcessor(processor), sdklog.WithResource(resource.Empty()),
			sdklog.WithAttributeCountLimit(40), sdklog.WithAttributeValueLengthLimit(maxValueBytes))
		outputs = append(outputs, otelslog.NewHandler("sink", otelslog.WithLoggerProvider(runtime.provider)))
	}
	h := &handler{level: level, outputs: outputs, identity: identity, limiter: runtime.limiter, failureBody: opts.Config.FailureBody, maxBodyBytes: opts.Config.MaxBodyBytes}
	runtime.Logger = slog.New(h)
	return runtime, nil
}

// Install is process-level setup owned by the CLI, never by imported packages.
func (r *Runtime) Install() {
	slog.SetDefault(r.Logger)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		r.console.Warn("OTLP export failed", "component", "logging", "event", "log_export_failed", "error_type", ErrorType(err), "failed", r.failed.Load())
	}))
	diagnostics := &sdkDiagnostics{runtime: r}
	otel.SetLogger(logr.New(diagnostics))
}

func (r *Runtime) Close() {
	if suppressed := r.limiter.pending(); suppressed > 0 {
		r.console.Warn("Repeated diagnostic events suppressed", "component", "logging", "event", "log_suppressed", "suppressed", suppressed)
	}
	if r.provider == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.shutdownTimeout)
	defer cancel()
	if err := r.provider.Shutdown(ctx); err != nil {
		r.console.Warn("OTLP shutdown flush incomplete", "component", "logging", "event", "log_shutdown_incomplete", "error_type", ErrorType(err), "failed", r.failed.Load())
	}
}

func identityAttrs(opts Options) []slog.Attr {
	hostname, _ := os.Hostname()
	var random [8]byte
	_, _ = rand.Read(random[:])
	instance := os.Getenv("POD_UID")
	if instance == "" {
		instance = hostname + "-" + hex.EncodeToString(random[:])
	}
	attrs := []slog.Attr{slog.String("service_name", "sink"), slog.String("service_version", bounded(opts.Version, maxValueBytes)),
		slog.String("role", opts.Role), slog.String("instance_id", bounded(instance, maxValueBytes))}
	if opts.Store != "" {
		attrs = append(attrs, slog.String("store", bounded(opts.Store, maxValueBytes)))
	}
	for _, field := range []struct{ key, env string }{{"environment", ""}, {"cluster", ""}, {"namespace", "POD_NAMESPACE"}, {"pod", "POD_NAME"}, {"node", "NODE_NAME"}} {
		value := opts.Config.Labels[field.key]
		if value == "" && field.env != "" {
			value = os.Getenv(field.env)
		}
		if value != "" {
			attrs = append(attrs, slog.String(field.key, bounded(value, maxValueBytes)))
		}
	}
	return attrs
}

type trackedExporter struct {
	sdklog.Exporter
	runtime *Runtime
}

func (e *trackedExporter) Export(ctx context.Context, records []sdklog.Record) error {
	err := e.Exporter.Export(ctx, records)
	if err != nil {
		e.runtime.failed.Add(uint64(len(records)))
	}
	return err
}

// SDK diagnostics use their own console-only path, even when console is disabled.
type sdkDiagnostics struct{ runtime *Runtime }

func (*sdkDiagnostics) Init(logr.RuntimeInfo)            {}
func (*sdkDiagnostics) Enabled(level int) bool           { return level <= 1 }
func (d *sdkDiagnostics) WithValues(...any) logr.LogSink { return d }
func (d *sdkDiagnostics) WithName(string) logr.LogSink   { return d }
func (d *sdkDiagnostics) Error(err error, _ string, _ ...any) {
	d.runtime.console.Warn("OTLP SDK error", "component", "logging", "event", "log_sdk_error", "error_type", ErrorType(err))
}
func (d *sdkDiagnostics) Info(_ int, message string, fields ...any) {
	if message != "dropped log records" {
		return
	}
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == "dropped" {
			if count, ok := fields[i+1].(uint64); ok {
				d.runtime.dropped.Add(count)
			}
		}
	}
	d.runtime.console.Warn("OTLP queue discarded older records", "component", "logging", "event", "log_queue_dropped", "dropped", d.runtime.dropped.Load())
}
