package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liran/sink-go/uri"
	"github.com/liran/sink/internal/capacity"
	"gopkg.in/yaml.v3"
)

const MaxFileBytes = 4 << 20

// Load reads separate strict component and Store YAML documents. It never connects to storage or Kafka.
func Load(componentPath, storePath string) (Config, error) {
	var empty Config
	component, err := os.Open(componentPath)
	if err != nil {
		return empty, fmt.Errorf("read component config %q: %w", componentPath, err)
	}
	defer component.Close()
	var store io.Reader
	if storePath != "" {
		file, err := os.Open(storePath)
		if err != nil {
			return empty, fmt.Errorf("read Store config %q: %w", storePath, err)
		}
		defer file.Close()
		store = file
	}
	return Decode(component, store)
}

// Decode validates separate standard YAML documents before opening dependencies.
// Store is required for Engine/Worker and forbidden for Gateway.
func Decode(component io.Reader, store io.Reader) (Config, error) {
	var empty Config
	var file configFile
	if err := decodeDocument(component, &file); err != nil {
		return empty, fmt.Errorf("component config: %w", err)
	}
	if file.Mode != ModeGateway && file.Mode != ModeEngine && file.Mode != ModeWorker {
		return empty, errors.New("mode is required and must be gateway, engine, or worker")
	}
	if file.Mode == ModeGateway && store != nil {
		return empty, errors.New("gateway must not use --store-config")
	}
	if file.Mode != ModeGateway && store == nil {
		return empty, errors.New("engine and worker require --store-config")
	}
	var shared storeFile
	if store != nil {
		if err := decodeDocument(store, &shared); err != nil {
			return empty, fmt.Errorf("store config: %w", err)
		}
	}
	loaded, err := resolve(file, shared)
	if err != nil {
		return empty, err
	}
	return loaded, nil
}

func decodeDocument(reader io.Reader, target any) error {
	if reader == nil {
		return errors.New("configuration reader is required")
	}
	data, err := io.ReadAll(io.LimitReader(reader, MaxFileBytes+1))
	if err != nil {
		return fmt.Errorf("read configuration: %w", err)
	}
	if len(data) > MaxFileBytes {
		return errors.New("configuration exceeds 4 MiB")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode configuration: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("decode configuration: %w", err)
		}
		return errors.New("multiple YAML documents are not supported")
	}
	return nil
}

func resolve(file configFile, shared storeFile) (Config, error) {
	var loaded Config
	loaded.Mode = file.Mode
	if loaded.Mode != ModeWorker && loaded.Mode != ModeEngine && loaded.Mode != ModeGateway {
		return loaded, errors.New("mode is required and must be gateway, engine, or worker")
	}
	v := validator{}
	loaded.Logging = resolveLogging(file.Logging, &v)
	if file.Memory.MaxBytes != nil {
		loaded.Memory.MaxBytes = v.bytes("memory.max_bytes", file.Memory.MaxBytes, 256<<20, math.MaxInt)
		if loaded.Memory.MaxBytes < 1024 {
			v.reject(errors.New("memory.max_bytes must be at least 1KiB"))
		}
	}
	loaded.Memory.BurstPercent = v.bounded("memory.burst_percent", file.Memory.BurstPercent, capacity.DefaultBurstPercent, 99)
	loaded.Memory.WaitTimeout = v.duration("memory.wait_timeout", file.Memory.WaitTimeout, 2*time.Second)
	grpcFile := gRPCFile{}
	if file.GRPC != nil {
		grpcFile = *file.GRPC
	}
	loaded.GRPC.Address = valueOrDefault(grpcFile.Address, ":8080")
	loaded.GRPC.MaxReceiveMessageBytes = v.bytes("grpc.max_receive_message_bytes", grpcFile.MaxReceiveMessageBytes, 64<<20, math.MaxInt)
	loaded.GRPC.MaxSendMessageBytes = v.bytes("grpc.max_send_message_bytes", grpcFile.MaxSendMessageBytes, 64<<20, math.MaxInt)
	loaded.Prometheus.Enabled = file.Prometheus.Enabled
	loaded.Health.Address = valueOrDefault(file.Health.Address, ":8081")
	loaded.Prometheus.Address = valueOrDefault(file.Prometheus.Address, ":9090")
	loaded.ShutdownTimeout = v.duration("shutdown_timeout", file.ShutdownTimeout, 15*time.Second)
	loaded.Service = resolveService(file, loaded.GRPC, &v)
	if loaded.Mode == ModeWorker && (file.Memory.BurstPercent != nil || file.Memory.WaitTimeout != nil) {
		v.reject(errors.New("worker memory only accepts max_bytes"))
	}
	if v.err != nil {
		return loaded, v.err
	}
	if loaded.Mode == ModeGateway {
		if file.Execution != nil || file.Producer != nil || file.Consumer != nil || file.Batching != nil {
			return loaded, errors.New("gateway must not configure execution, producer, consumer or batching")
		}
		if file.Gateway == nil {
			return loaded, errors.New("forwarding configuration is required")
		}
		loaded.Gateway = resolveGateway(*file.Gateway, &v)
		return loaded, v.err
	}
	if file.Request != nil || file.Gateway != nil {
		return loaded, errors.New("request and forwarding require gateway mode")
	}
	if loaded.Mode == ModeEngine && file.Consumer != nil {
		return loaded, errors.New("consumer requires worker mode")
	}
	if loaded.Mode == ModeWorker && (file.Producer != nil || file.Batching != nil || file.GRPC != nil) {
		return loaded, errors.New("worker must not configure producer, batching or grpc")
	}
	name := shared.Name
	if name == "" || len(name) > 256 || !utf8.ValidString(name) || strings.TrimSpace(name) != name || strings.ContainsAny(name, "\x00\r\n\t") {
		return loaded, errors.New("store name must be a nonempty valid identity of at most 256 bytes")
	}
	configured, err := resolveStorage("storage", shared.Storage)
	if err != nil {
		return loaded, err
	}
	configured.Name = name
	if !uri.ValidStore(name) {
		return loaded, errors.New("store name must be a canonical lowercase identity")
	}
	configured.MongoDB.MaxConcurrentWrites = loaded.Service.Execution.MongoDB.MaxConcurrentWrites
	configured.MongoDB.MaxConcurrentGroups = loaded.Service.Execution.MongoDB.MaxConcurrentGroups
	configured.Kafka = resolveKafka("kafka", shared.Kafka, &v)
	configured.Kafka.Producer = resolveProducer(file.Producer, &v)
	configured.Kafka.Consumer = resolveConsumer(file.Consumer, &v)
	if file.Producer != nil && !configured.Kafka.Enabled {
		v.reject(errors.New("producer requires Kafka enabled in Store config"))
	}
	if loaded.Mode == ModeEngine && configured.Kafka.Topic.MaxRecordBytes > configured.Kafka.Producer.MaxBufferedBytes {
		v.reject(errors.New("producer.max_buffered_bytes must cover kafka.topic.max_record_bytes"))
	}
	if file.Execution != nil && file.Execution.MongoDB != (mongoExecutionFile{}) && configured.Driver != DriverMongoDB {
		v.reject(errors.New("execution.mongodb requires the mongodb driver"))
	}
	if v.err != nil {
		return loaded, v.err
	}
	loaded.Storage = configured
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
