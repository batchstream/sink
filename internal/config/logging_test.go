package config

import (
	"strings"
	"testing"
	"time"
)

const loggingBase = "mode: engine\nstorage:\n  name: primary\n  driver: mongodb\n  mongodb:\n    uri: mongodb://localhost:27017\n"

func TestLoggingDefaults(t *testing.T) {
	loaded, err := Decode(strings.NewReader(loggingBase))
	if err != nil {
		t.Fatal(err)
	}
	cfg := loaded.Logging
	if cfg.Level != "warn" || !cfg.Console.Enabled || cfg.Console.Format != "json" || cfg.OTLP.Enabled || !cfg.OTLP.TLS || cfg.FailureBody {
		t.Fatalf("unexpected logging defaults: %+v", cfg)
	}
	if cfg.OTLP.QueueSize != 1024 || cfg.OTLP.BatchSize != 128 || cfg.OTLP.ExportTimeout != 3*time.Second || cfg.MaxBodyBytes != 16<<10 {
		t.Fatalf("unbounded/changed defaults: %+v", cfg)
	}
}

func TestLoggingRejectsInvalidSettings(t *testing.T) {
	cases := []string{
		"level: trace", "console: {format: xml}", "console: {enabled: false}", "file: {enabled: true}",
		"otlp: {enabled: true}", "otlp: {protocol: json}", "otlp: {endpoint: 'http://localhost:4317'}",
		"otlp: {endpoint: 'user:password@localhost:4317'}", "otlp: {endpoint: 'localhost:0'}",
		"otlp: {queue_size: 0}", "otlp: {queue_size: 8193}", "otlp: {queue_size: 1, batch_size: 2}",
		"otlp: {flush_interval: 0s}", "otlp: {export_timeout: 31s}", "otlp: {shutdown_timeout: 31s}",
		"labels: {arbitrary_field: value}", "labels: {service_name: spoof}",
		"labels: {environment: '" + strings.Repeat("x", 257) + "'}",
		"components: {unknown: debug}", "components: {kafka: trace}", "max_body_bytes: 128KiB", "max_body_bytes: 1",
	}
	for _, fragment := range cases {
		t.Run(fragment, func(t *testing.T) {
			_, err := Decode(strings.NewReader(loggingBase + "logging:\n  " + fragment + "\n"))
			if err == nil {
				t.Fatal("accepted invalid logging configuration")
			}
		})
	}
}

func TestLoggingExplicitOptions(t *testing.T) {
	yaml := loggingBase + `logging:
  level: error
  components: {kafka: debug}
  failure_body: true
  max_body_bytes: 32KiB
  console: {enabled: false, format: text}
  labels: {environment: production, cluster: eks}
  otlp:
    enabled: true
    protocol: http/protobuf
    endpoint: "[::1]:4318"
    tls: {enabled: false}
    queue_size: 256
    batch_size: 64
    flush_interval: 2s
    export_timeout: 4s
    shutdown_timeout: 6s
`
	loaded, err := Decode(strings.NewReader(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Logging.Console.Enabled || loaded.Logging.OTLP.TLS || !loaded.Logging.FailureBody || loaded.Logging.Components["kafka"] != "debug" || loaded.Logging.MaxBodyBytes != 32<<10 {
		t.Fatalf("lost explicit settings: %+v", loaded.Logging)
	}
}
