package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Load reads one strict YAML document. It never connects to storage or Kafka.
func Load(path string) (Config, error) {
	var empty Config
	file, err := os.Open(path)
	if err != nil {
		return empty, fmt.Errorf("read config file %q: %w", path, err)
	}
	defer file.Close()
	loaded, err := Decode(file)
	if err != nil {
		return empty, fmt.Errorf("config file %q: %w", path, err)
	}
	return loaded, nil
}

// Decode resolves defaults and validates the complete configuration before it
// becomes available to the application. Explicit zero limits are invalid.
func Decode(reader io.Reader) (Config, error) {
	var empty Config
	var file configFile
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	if err := decoder.Decode(&file); err != nil {
		return empty, fmt.Errorf("decode configuration: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return empty, fmt.Errorf("decode configuration: %w", err)
		}
		return empty, errors.New("multiple YAML documents are not supported")
	}
	loaded, err := resolve(file)
	if err != nil {
		return empty, err
	}
	return loaded, nil
}

func resolve(file configFile) (Config, error) {
	var loaded Config
	loaded.Mode = Mode(valueOrDefault(string(file.Mode), string(ModeServer)))
	if loaded.Mode != ModeServer && loaded.Mode != ModeWorker && loaded.Mode != ModeAll {
		return loaded, errors.New("mode must be server, worker, or all")
	}
	v := validator{}
	loaded.GRPC.Address = valueOrDefault(file.GRPC.Address, ":8080")
	loaded.GRPC.MaxReceiveMessageBytes = v.integer("grpc.max_receive_message_bytes", file.GRPC.MaxReceiveMessageBytes, 64<<20)
	loaded.GRPC.MaxSendMessageBytes = v.integer("grpc.max_send_message_bytes", file.GRPC.MaxSendMessageBytes, 64<<20)
	loaded.Prometheus.Address = strings.TrimSpace(file.Prometheus.Address)
	loaded.ShutdownTimeout = v.duration("shutdown_timeout", file.ShutdownTimeout, 15*time.Second)
	loaded.Service = resolveService(file.Service, loaded.GRPC, &v)
	if v.err != nil {
		return loaded, v.err
	}
	storages, err := resolveStorages(file.Storages, loaded.Service.Execution.MaxBytes)
	if err != nil {
		return loaded, err
	}
	loaded.Storages = storages
	if err := validateKafkaResources(loaded); err != nil {
		return loaded, err
	}
	return loaded, nil
}

// validator retains the first error while each section resolves its own fields.
type validator struct {
	err error
}

func (v *validator) reject(err error) {
	if v.err == nil {
		v.err = err
	}
}

func (v *validator) integer(name string, value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	if *value <= 0 {
		v.reject(fmt.Errorf("%s must be a positive integer", name))
		return fallback
	}
	return *value
}

func (v *validator) bounded(name string, value *int, fallback int, maximum int) int {
	result := v.integer(name, value, fallback)
	if result <= 0 || result > maximum {
		v.reject(fmt.Errorf("%s must be between 1 and %d", name, maximum))
	}
	return result
}

func (v *validator) duration(name string, value *time.Duration, fallback time.Duration) time.Duration {
	if value == nil {
		return fallback
	}
	if *value <= 0 {
		v.reject(fmt.Errorf("%s must be a positive duration", name))
		return fallback
	}
	return *value
}

func valueOrDefault(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func nonEmptyValues(raw []string) []string {
	values := make([]string, 0, len(raw))
	for _, entry := range raw {
		if value := strings.TrimSpace(entry); value != "" {
			values = append(values, value)
		}
	}
	return values
}
