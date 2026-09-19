package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liran/sink/internal/config"
)

type lockedBuffer struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(p)
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.data.String() }

func testConfig(t *testing.T) config.Logging {
	t.Helper()
	loaded, err := config.Decode(strings.NewReader("mode: engine\nstorage:\n  name: primary\n  driver: mongodb\n  mongodb:\n    uri: mongodb://localhost:27017\n"))
	if err != nil {
		t.Fatal(err)
	}
	return loaded.Logging
}

func newTestRuntime(t *testing.T, cfg config.Logging, output io.Writer) *Runtime {
	t.Helper()
	opts := Options{Config: cfg, Role: "engine", Store: "primary", Version: "test", Stderr: output}
	runtime, err := New(t.Context(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	return runtime
}

func TestConsoleDefaultsLabelsAndComponentLevel(t *testing.T) {
	t.Setenv("POD_UID", "pod-uid")
	t.Setenv("POD_NAME", "engine-0")
	cfg := testConfig(t)
	cfg.Components = map[string]string{"kafka": "debug"}
	output := &lockedBuffer{}
	runtime := newTestRuntime(t, cfg, output)
	runtime.Logger.Info("hidden")
	runtime.Logger.Warn("visible", "component", "rpc", "event", "rpc_completed", "service_name", "spoof", "document", "private-data", "duration_ms", 10)
	runtime.Logger.Debug("kafka detail", "component", "kafka")
	runtime.Logger.Debug("hidden batch", "component", "batcher")
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("filtering: %s", output.String())
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"service_name": "sink", "service_version": "test", "role": "engine", "store": "primary", "instance_id": "pod-uid", "pod": "engine-0", "level": "warn", "duration_ms": "10"} {
		if record[key] != want {
			t.Errorf("%s = %v, want %s", key, record[key], want)
		}
	}
	if strings.Contains(output.String(), "private-data") || strings.Count(lines[0], `"level"`) != 1 {
		t.Fatal("leaked payload or duplicate level")
	}
}

type countedBody struct{ calls *int }

func (b countedBody) LogValue() slog.Value {
	*b.calls++
	body := &FailureBody{Encoding: "json", Payload: []byte(strings.Repeat("中", 1000))}
	return slog.AnyValue(body)
}

func TestFailureBodyRequiresOptInErrorAndRemainsBounded(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		cfg := testConfig(t)
		cfg.FailureBody = enabled
		cfg.MaxBodyBytes = 1024
		output := &lockedBuffer{}
		runtime := newTestRuntime(t, cfg, output)
		calls := 0
		body := countedBody{calls: &calls}
		runtime.Logger.Warn("retry", "failure_body", body)
		runtime.Logger.Error(strings.Repeat("x", 1024), "failure_body", body)
		for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatal(err)
			}
			message := record["msg"].(string)
			if len(message) > 1024 {
				t.Fatalf("body exceeds byte limit: %d", len(message))
			}
			if _, ok := record["failure_body"]; ok {
				t.Fatal("payload became a label")
			}
			if record["level"] == "error" && enabled && !strings.Contains(message, "[truncated]") {
				t.Fatal("missing bounded failure body")
			}
		}
		want := 0
		if enabled {
			want = 1
		}
		if calls != want {
			t.Fatalf("payload evaluated %d times, want %d", calls, want)
		}
	}
}

func TestRateLimitBoundsStateAndReportsSuppression(t *testing.T) {
	limiter := &eventLimiter{}
	now := time.Now()
	for i := 0; i < 25; i++ {
		allowed, _ := limiter.allow("kafka:retry", now)
		if allowed != (i < 10) {
			t.Fatal("rate limit failed")
		}
	}
	allowed, suppressed := limiter.allow("kafka:retry", now.Add(31*time.Second))
	if !allowed || suppressed != 15 {
		t.Fatalf("lost suppression: %v, %d", allowed, suppressed)
	}
	for i := 0; i < 1000; i++ {
		limiter.allow(string(rune(i)), now)
	}
	if len(limiter.windows) > 257 {
		t.Fatal("unbounded rate limiter")
	}
}

func TestShutdownReportsSuppressedErrorsRegardlessOfLevel(t *testing.T) {
	cfg := testConfig(t)
	cfg.Level = "error"
	output := &lockedBuffer{}
	runtime := newTestRuntime(t, cfg, output)
	for range 25 {
		runtime.Logger.Error("failed", "component", "kafka", "event", "kafka_quarantined")
	}
	runtime.Close()
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 11 {
		t.Fatalf("expected 10 errors and one suppression summary, got %d", len(lines))
	}
	var summary map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &summary); err != nil {
		t.Fatal(err)
	}
	if summary["event"] != "log_suppressed" || summary["suppressed"] != "15" {
		t.Fatalf("missing shutdown suppression count: %v", summary)
	}
}

func TestHandlerBoundsAndDiscardsNestedFields(t *testing.T) {
	cfg := testConfig(t)
	output := &lockedBuffer{}
	runtime := newTestRuntime(t, cfg, output)
	runtime.Logger.With("reason", strings.Repeat("x", 10000)).Warn(strings.Repeat("y", 10000), "password", "secret")
	runtime.Logger.WithGroup("request").Warn("group", "reason", "private")
	var record map[string]any
	decoder := json.NewDecoder(strings.NewReader(output.String()))
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	if len(record["reason"].(string)) != maxValueBytes || len(record["msg"].(string)) != 1024 {
		t.Fatal("unbounded record")
	}
	if strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "private") {
		t.Fatal("unapproved field escaped")
	}
}

func BenchmarkDisabledDebug(b *testing.B) {
	h := &handler{level: slog.LevelWarn, minimum: slog.LevelWarn}
	logger := slog.New(h)
	b.ReportAllocs()
	for b.Loop() {
		logger.DebugContext(context.Background(), "batch", "operations", 100)
	}
}

func TestSuppressedFailureBodiesAreNotEvaluated(t *testing.T) {
	cfg := testConfig(t)
	cfg.FailureBody = true
	output := &lockedBuffer{}
	runtime := newTestRuntime(t, cfg, output)
	calls := 0
	body := countedBody{calls: &calls}
	for range 100 {
		runtime.Logger.Error("failed", "component", "kafka", "event", "kafka_quarantined", "failure_body", body)
	}
	if calls != 10 {
		t.Fatalf("suppressed bodies still consumed formatting work: %d evaluations", calls)
	}
	runtime.Close()
	if !strings.Contains(output.String(), `"suppressed":"90"`) {
		t.Fatal("lost suppression count")
	}
}
