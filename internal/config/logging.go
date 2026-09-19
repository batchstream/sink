package config

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Logging struct {
	FailureBody  bool
	MaxBodyBytes int
	Level        string
	Console      LogConsole
	OTLP         LogOTLP
	Labels       map[string]string
}

type LogConsole struct {
	Enabled bool
	Format  string
}

type LogOTLP struct {
	Enabled         bool
	Protocol        string
	Endpoint        string
	TLS             bool
	QueueSize       int
	BatchSize       int
	FlushInterval   time.Duration
	ExportTimeout   time.Duration
	ShutdownTimeout time.Duration
}

type loggingFile struct {
	FailureBody  bool              `yaml:"failure_body"`
	MaxBodyBytes *byteSize         `yaml:"max_body_bytes"`
	Level        string            `yaml:"level"`
	Console      logConsoleFile    `yaml:"console"`
	OTLP         logOTLPFile       `yaml:"otlp"`
	Labels       map[string]string `yaml:"labels"`
}

type logConsoleFile struct {
	Enabled *bool  `yaml:"enabled"`
	Format  string `yaml:"format"`
}

type logOTLPFile struct {
	Enabled         bool           `yaml:"enabled"`
	Protocol        string         `yaml:"protocol"`
	Endpoint        string         `yaml:"endpoint"`
	TLS             logTLSFile     `yaml:"tls"`
	QueueSize       *int           `yaml:"queue_size"`
	BatchSize       *int           `yaml:"batch_size"`
	FlushInterval   *time.Duration `yaml:"flush_interval"`
	ExportTimeout   *time.Duration `yaml:"export_timeout"`
	ShutdownTimeout *time.Duration `yaml:"shutdown_timeout"`
}

type logTLSFile struct {
	Enabled *bool `yaml:"enabled"`
}

func resolveLogging(file loggingFile, v *validator) Logging {
	result := Logging{Level: valueOrDefault(file.Level, "warn"), Labels: file.Labels}
	result.FailureBody = file.FailureBody
	result.MaxBodyBytes = v.bytes("logging.max_body_bytes", file.MaxBodyBytes, 16<<10, 64<<10)
	if result.MaxBodyBytes < 1024 {
		v.reject(errors.New("logging.max_body_bytes must be at least 1KiB"))
	}
	switch result.Level {
	case "debug", "info", "warn", "error":
	default:
		v.reject(errors.New("logging.level must be debug, info, warn, or error"))
	}
	result.Console.Enabled = file.Console.Enabled == nil || *file.Console.Enabled
	result.Console.Format = valueOrDefault(file.Console.Format, "text")
	if result.Console.Format != "json" && result.Console.Format != "text" {
		v.reject(errors.New("logging.console.format must be json or text"))
	}
	result.OTLP = LogOTLP{
		Enabled: file.OTLP.Enabled, Protocol: valueOrDefault(file.OTLP.Protocol, "grpc"),
		Endpoint: file.OTLP.Endpoint, TLS: file.OTLP.TLS.Enabled == nil || *file.OTLP.TLS.Enabled,
		QueueSize:       v.bounded("logging.otlp.queue_size", file.OTLP.QueueSize, 1024, 8192),
		BatchSize:       v.bounded("logging.otlp.batch_size", file.OTLP.BatchSize, 128, 512),
		FlushInterval:   v.duration("logging.otlp.flush_interval", file.OTLP.FlushInterval, time.Second),
		ExportTimeout:   v.duration("logging.otlp.export_timeout", file.OTLP.ExportTimeout, 3*time.Second),
		ShutdownTimeout: v.duration("logging.otlp.shutdown_timeout", file.OTLP.ShutdownTimeout, 5*time.Second),
	}
	if result.OTLP.Protocol != "grpc" && result.OTLP.Protocol != "http/protobuf" {
		v.reject(errors.New("logging.otlp.protocol must be grpc or http/protobuf"))
	}
	if result.OTLP.BatchSize > result.OTLP.QueueSize {
		v.reject(errors.New("logging.otlp.batch_size must not exceed queue_size"))
	}
	if result.OTLP.ExportTimeout > 30*time.Second || result.OTLP.ShutdownTimeout > 30*time.Second || result.OTLP.FlushInterval > time.Minute {
		v.reject(errors.New("logging OTLP export/shutdown timeouts must not exceed 30s; flush_interval must not exceed 1m"))
	}
	if file.OTLP.Enabled || file.OTLP.Endpoint != "" {
		host, port, err := net.SplitHostPort(file.OTLP.Endpoint)
		number, numberErr := strconv.Atoi(port)
		if err != nil || host == "" || numberErr != nil || number < 1 || number > 65535 || strings.ContainsAny(host, "/@?#\\ \t\r\n") {
			v.reject(errors.New("logging.otlp.endpoint must be host:port without scheme, path, or credentials"))
		}
	}
	if !result.Console.Enabled && !result.OTLP.Enabled {
		v.reject(errors.New("logging requires console or OTLP output"))
	}
	for key, value := range file.Labels {
		switch key {
		case "environment", "cluster", "namespace", "pod", "node":
		default:
			v.reject(fmt.Errorf("logging.labels key %q is not supported", key))
		}
		if len(value) > 256 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n\t") {
			v.reject(errors.New("logging.labels values must be valid single-line UTF-8 of at most 256 bytes"))
		}
	}
	return result
}
