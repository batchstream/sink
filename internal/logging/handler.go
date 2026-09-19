// Package logging configures bounded, structured internal diagnostics.
package logging

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const maxValueBytes = 256

type handler struct {
	components   map[string]slog.Level
	minimum      slog.Level
	failureBody  bool
	maxBodyBytes int
	level        slog.Level
	outputs      []slog.Handler
	identity     []slog.Attr
	attrs        []slog.Attr
	grouped      bool
	limiter      *eventLimiter
}

func (h *handler) Enabled(_ context.Context, level slog.Level) bool { return level >= h.minimum }

func (h *handler) Handle(_ context.Context, record slog.Record) error {
	if !h.Enabled(context.Background(), record.Level) {
		return nil
	}
	fields := make(map[string]string, 32)
	var failure slog.Value
	for _, attr := range h.attrs {
		addField(fields, attr)
	}
	if !h.grouped {
		record.Attrs(func(attr slog.Attr) bool {
			if h.failureBody && record.Level >= slog.LevelError && attr.Key == "failure_body" {
				failure = attr.Value
			}
			addField(fields, attr)
			return true
		})
	}
	for _, attr := range h.identity {
		fields[attr.Key] = attr.Value.String()
	}
	fields["level"] = strings.ToLower(record.Level.String())
	if fields["component"] == "" {
		fields["component"] = "runtime"
	}
	if fields["event"] == "" {
		fields["event"] = "diagnostic"
	}
	threshold := h.level
	if override, ok := h.components[fields["component"]]; ok {
		threshold = override
	}
	if record.Level < threshold {
		return nil
	}
	if record.Level >= slog.LevelWarn {
		allowed, suppressed := h.limiter.allow(fields["component"]+":"+fields["event"]+":"+fields["level"], record.Time)
		if !allowed {
			return nil
		}
		if suppressed > 0 {
			fields["suppressed"] = strconv.FormatUint(suppressed, 10)
		}
	}
	body := bounded(record.Message, 1024)
	if detail, ok := failure.Resolve().Any().(*FailureBody); ok && detail != nil {
		body = detail.appendTo(body, h.maxBodyBytes)
	}
	clean := slog.NewRecord(record.Time, record.Level, body, 0)
	for key, value := range fields {
		clean.AddAttrs(slog.String(key, value))
	}
	var first error
	for _, output := range h.outputs {
		// Logs deliberately have no request/trace context. The SDK export worker
		// owns its timeout independently of any request cancellation.
		if err := output.Handle(context.Background(), clean); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// FailureBody is accepted only on ERROR records when explicitly configured.
// The handler formats it after rate limiting and never adds it to attributes.
type FailureBody struct {
	Encoding string
	Payload  []byte
}

func (b *FailureBody) appendTo(message string, limit int) string {
	if limit < 1024 {
		limit = 16 << 10
	}
	metadata := "\ndocument_encoding=" + bounded(b.Encoding, 16) + "\ndocument="
	if b.Encoding != "json" {
		metadata += "base64:"
	}
	maximumMessage := max(0, limit-len(metadata)-len(" [truncated]"))
	truncated := len(message) > maximumMessage
	prefix := bounded(message, maximumMessage) + metadata
	available := max(0, limit-len(prefix)-len(" [truncated]"))
	var text string
	if b.Encoding == "json" {
		truncated = truncated || len(b.Payload) > available
		text = bounded(string(b.Payload[:min(len(b.Payload), available)]), available)
	} else {
		size := min(len(b.Payload), available/4*3)
		truncated = truncated || size < len(b.Payload)
		text = base64.StdEncoding.EncodeToString(b.Payload[:size])
	}
	if truncated {
		text += " [truncated]"
	}
	return prefix + text
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copy := *h
	fields := make(map[string]string)
	for _, attr := range h.attrs {
		addField(fields, attr)
	}
	if !h.grouped {
		for _, attr := range attrs {
			addField(fields, attr)
		}
	}
	copy.attrs = make([]slog.Attr, 0, len(fields))
	for key, value := range fields {
		copy.attrs = append(copy.attrs, slog.String(key, value))
	}
	return &copy
}

func (h *handler) WithGroup(name string) slog.Handler {
	copy := *h
	copy.grouped = h.grouped || name != ""
	return &copy
}

func addField(fields map[string]string, attr slog.Attr) {
	switch attr.Key {
	case "component", "event", "store", "method", "phase", "reason", "status", "error_code", "error_type",
		"duration_ms", "queue_ms", "operations", "failed", "bytes", "attempt", "topic", "partition", "offset", "source_topic", "source_partition", "source_offset", "dropped", "suppressed":
	default:
		return
	}
	value := attr.Value.Resolve()
	var text string
	switch value.Kind() {
	case slog.KindString:
		text = value.String()
	case slog.KindInt64:
		text = strconv.FormatInt(value.Int64(), 10)
	case slog.KindUint64:
		text = strconv.FormatUint(value.Uint64(), 10)
	case slog.KindFloat64:
		text = strconv.FormatFloat(value.Float64(), 'f', -1, 64)
	case slog.KindBool:
		text = strconv.FormatBool(value.Bool())
	case slog.KindDuration:
		text = value.Duration().String()
	default:
		return
	}
	fields[attr.Key] = bounded(text, maxValueBytes)
}

func bounded(value string, limit int) string {
	if len(value) > limit {
		value = value[:limit]
		for !utf8.ValidString(value) && len(value) > 0 {
			value = value[:len(value)-1]
		}
	}
	return strings.ToValidUTF8(value, "?")
}

// ErrorType classifies an error without recording driver messages or user data.
func ErrorType(err error) string {
	if err == nil {
		return ""
	}
	for range 8 {
		next := errors.Unwrap(err)
		if next == nil {
			break
		}
		err = next
	}
	return fmt.Sprintf("%T", err)
}

type eventWindow struct {
	since      time.Time
	emitted    int
	suppressed uint64
}
type eventLimiter struct {
	mu      sync.Mutex
	windows map[string]eventWindow
}

func (l *eventLimiter) allow(key string, now time.Time) (bool, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.windows == nil {
		l.windows = make(map[string]eventWindow)
	}
	if _, ok := l.windows[key]; !ok && len(l.windows) >= 256 {
		key = "overflow"
	}
	window := l.windows[key]
	var suppressed uint64
	if window.since.IsZero() || now.Sub(window.since) >= 30*time.Second {
		suppressed = window.suppressed
		window = eventWindow{since: now}
	}
	allowed := window.emitted < 10
	if allowed {
		window.emitted++
	} else {
		window.suppressed++
	}
	l.windows[key] = window
	return allowed, suppressed
}

func (l *eventLimiter) pending() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var total uint64
	for key, window := range l.windows {
		total += window.suppressed
		window.suppressed = 0
		l.windows[key] = window
	}
	return total
}
