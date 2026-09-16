package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigEngineDefaults(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Mode != ModeEngine || loaded.GRPC.Address != ":8080" {
		t.Fatalf("Load() = %#v", loaded)
	}
	if loaded.Prometheus.Address != "" {
		t.Fatalf("Load() Prometheus address = %q", loaded.Prometheus.Address)
	}
	configured := loaded.Storage
	if configured.Name != "primary" || configured.Driver != DriverMongoDB || configured.MongoDB.URI != "mongodb://mongodb:27017" {
		t.Fatalf("Load() storage = %#v", configured)
	}
	if loaded.Service.Request.MaxOperations != 1000 || loaded.Service.Merge.MaxAttempts != 3 || loaded.ShutdownTimeout != 15*time.Second {
		t.Fatalf("Load() service defaults = %#v", loaded)
	}
	if loaded.Service.Batching.MaxWait != 2*time.Millisecond ||
		loaded.Service.Batching.MaxOperations != 1000 || loaded.Service.Batching.MaxBytes != 16<<20 ||
		loaded.Service.Batching.Queue.MaxOperations != 10_000 || loaded.Service.Batching.Queue.MaxBytes != 128<<20 {
		t.Fatalf("Load() batching defaults = %#v", loaded)
	}
	if loaded.Service.Merge.Lua.Timeout != 100*time.Millisecond || loaded.Service.Merge.Lua.MaxSourceBytes != 64<<10 ||
		loaded.Service.Merge.Lua.MaxResultBytes != 16<<20 || loaded.Service.Merge.Lua.MaxCachedPrograms != 256 ||
		loaded.Service.Merge.Lua.MaxInstructions != 1_000_000 {
		t.Fatalf("Load() Lua defaults = %#v", loaded.Service.Merge.Lua)
	}
	if loaded.GRPC.MaxReceiveMessageBytes != 64<<20 || loaded.GRPC.MaxSendMessageBytes != 64<<20 {
		t.Fatalf("Load() gRPC limits = %#v", loaded)
	}
	if configured.Kafka.Enabled {
		t.Fatalf("Load() Kafka configuration = %#v", configured.Kafka)
	}
}

func TestLoadConfigDisablesKafkaByDefault(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
  kafka:
    brokers: [kafka:9092]
    topic:
      name: sink-mutations
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	configured := loaded.Storage.Kafka
	if configured.Enabled {
		t.Fatalf("Load() Kafka = %#v", configured)
	}
}

func TestLoadConfigBatchingSettings(t *testing.T) {
	path := writeConfig(t, `mode: engine
grpc:
  max_receive_message_bytes: 1048576
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
service:
  request:
    max_operations: 2000
  batching:
    max_wait: 5ms
    max_operations: 500
    max_bytes: 524288
    queue:
      max_operations: 2500
      max_bytes: 2097152
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Service.Batching.MaxWait != 5*time.Millisecond ||
		loaded.Service.Batching.MaxOperations != 500 || loaded.Service.Batching.MaxBytes != 524288 ||
		loaded.Service.Batching.Queue.MaxOperations != 2500 || loaded.Service.Batching.Queue.MaxBytes != 2097152 {
		t.Fatalf("Load() batching settings = %#v", loaded)
	}
}

func TestLoadConfigRejectsBatchingSwitch(t *testing.T) {
	for _, enabled := range []string{"true", "false"} {
		t.Run(enabled, func(t *testing.T) {
			contents := `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
service:
  batching:
    enabled: ` + enabled + "\n"
			path := writeConfig(t, contents)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), "field enabled not found in type config.batchingFile") {
				t.Fatalf("Load() accepted removed batching switch: %v", err)
			}
		})
	}
}

func TestLoadConfigRejectsUnsafeBatchingLimits(t *testing.T) {
	tests := []struct {
		name      string
		batching  string
		wantError string
	}{
		{
			name:      "batch exceeds service operation limit",
			batching:  "max_operations: 1001",
			wantError: "service.batching.max_operations cannot exceed service.request.max_operations",
		},
		{
			name:      "queue cannot hold one request",
			batching:  "queue:\n      max_operations: 999",
			wantError: "service.batching.queue.max_operations must cover one server request and one batch",
		},
		{
			name:      "byte queue cannot hold one gRPC message",
			batching:  "queue:\n      max_bytes: 1048576",
			wantError: "service.batching.queue.max_bytes must cover one gRPC request and one batch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents := `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
service:
  batching:
    ` + test.batching + "\n"
			path := writeConfig(t, contents)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Load() error = %v", err)
			}
		})
	}
}

func TestLoadConfigRejectsMultipleStorages(t *testing.T) {
	path := writeConfig(t, `
prometheus:
  address: ":9090"
storages:
  - name: mongo-main
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-main:27017
      metadata_field: __revision
  - name: mongo-archive
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-archive:27017
  - name: search-main
    driver: elasticsearch
    search:
      endpoints:
        - http://search-1:9200
        - http://search-2:9200
      api_key: test-api-key
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field storages not found") {
		t.Fatalf("plural storage configuration was accepted: %v", err)
	}
}

func TestLoadConfigOpenSearchBasicAuthentication(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: search-main
  driver: opensearch
  search:
    endpoints:
      - https://search:9200
    username: sink
    password: test-password
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	configured := loaded.Storage
	if configured.Driver != DriverOpenSearch || configured.Search.Username != "sink" || configured.Search.Password != "test-password" {
		t.Fatalf("Load() storage = %#v", configured)
	}
}

func TestLoadConfigRejectsConflictingSearchAuthentication(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: search-main
  driver: elasticsearch
  search:
    endpoints: [http://search:9200]
    username: sink
    password: test-password
    api_key: test-api-key
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() error = nil")
	}
}

func TestLoadConfigWorkerSettings(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
  kafka:
    enabled: true
    brokers:
      - kafka-1:9092
      - kafka-2:9092
    topic:
      name: sink-mutations
      partitions: 12
      replication_factor: 3
      retention: 48h
    consumer:
      group_id: sink-workers
      max_poll_records: 250
      retry:
        max_attempts: 4
        backoff: 20ms
        max_backoff: 200ms
    dead_letter:
      topic: sink-dead-letters
service:
  request:
    max_operations: 2000
  merge:
    max_attempts: 5
    lua:
      timeout: 250ms
      max_source_bytes: 32768
      max_result_bytes: 1048576
      max_cached_programs: 128
      max_instructions: 2000000
shutdown_timeout: 30s
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	configured := loaded.Storage
	if loaded.Mode != ModeWorker || len(configured.Kafka.Brokers) != 2 || configured.Kafka.Consumer.MaxPollRecords != 250 {
		t.Fatalf("Load() = %#v", loaded)
	}
	if configured.Kafka.DeadLetter.Topic != "sink-dead-letters" || configured.Kafka.Consumer.Retry.MaxAttempts != 4 || configured.Kafka.Consumer.Retry.Backoff != 20*time.Millisecond || configured.Kafka.Consumer.Retry.MaxBackoff != 200*time.Millisecond {
		t.Fatalf("Load() Kafka retry settings = %#v", configured.Kafka)
	}
	if configured.Kafka.Topic.Partitions != 12 ||
		configured.Kafka.Topic.ReplicationFactor != 3 || configured.Kafka.Topic.Retention != 48*time.Hour {
		t.Fatalf("Load() Kafka topic settings = %#v", configured.Kafka)
	}
	if loaded.Service.Request.MaxOperations != 2000 || loaded.Service.Merge.MaxAttempts != 5 || loaded.ShutdownTimeout != 30*time.Second {
		t.Fatalf("Load() service settings = %#v", loaded)
	}
	if loaded.Service.Merge.Lua.Timeout != 250*time.Millisecond || loaded.Service.Merge.Lua.MaxSourceBytes != 32768 ||
		loaded.Service.Merge.Lua.MaxResultBytes != 1048576 || loaded.Service.Merge.Lua.MaxCachedPrograms != 128 ||
		loaded.Service.Merge.Lua.MaxInstructions != 2_000_000 {
		t.Fatalf("Load() Lua settings = %#v", loaded.Service.Merge.Lua)
	}
}

func TestLoadConfigEngineAllowsKafkaWithoutConsumerGroup(t *testing.T) {
	path := writeConfig(t, `
mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
  kafka:
    enabled: true
    brokers: [kafka:9092]
    topic:
      name: sink-mutations
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	configured := loaded.Storage.Kafka
	if configured.Consumer.GroupID != "" || configured.DeadLetter.Topic != "sink-mutations.dlq" {
		t.Fatalf("Load() Kafka = %#v", configured)
	}
}

func TestLoadConfigRejectsNonPositiveLuaLimits(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
service:
  merge:
    lua:
      max_source_bytes: 0
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "service.merge.lua.max_source_bytes") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigRejectsDuplicateStorageNames(t *testing.T) {
	path := writeConfig(t, `
storages:
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-1:27017
  - name: primary
    driver: mongodb
    mongodb:
      uri: mongodb://mongo-2:27017
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field storages not found") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigRequiresAtLeastOneStorage(t *testing.T) {
	path := writeConfig(t, `
mode: engine

`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "require singular storage") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigRequiresStorageNameAndDriver(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: ""
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "storage.name") {
		t.Fatalf("Load() error = %v", err)
	}

	path = writeConfig(t, `mode: engine
storage:
  name: primary
  mongodb:
    uri: mongodb://mongodb:27017
`)
	_, err = Load(path)
	if err == nil || !strings.Contains(err.Error(), "storage.driver must be") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigRejectsBindings(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
    bindings: []
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field bindings not found") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigRejectsPartialKafkaConfiguration(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
  kafka:
    enabled: true
    brokers: [kafka:9092]
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "storage.kafka") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigWorkerRequiresGroupForEveryKafkaStore(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
  kafka:
    enabled: true
    brokers: [kafka:9092]
    topic:
      name: sink-mutations
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "storage.kafka.consumer.group_id") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigRejectsTopLevelKafkaConfiguration(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
kafka:
  brokers: [kafka:9092]
  topic:
    name: sink-mutations
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field kafka not found") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigWorkerRequiresAtLeastOneKafkaStore(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "worker requires storage.kafka.enabled") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
unexpected: true
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field unexpected not found") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigRejectsNonPositiveValues(t *testing.T) {
	path := writeConfig(t, `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
service:
  request:
    max_operations: 0
`)
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "service.request.max_operations must be a positive integer") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadConfigDefaultsWorkerDeadLetterTopic(t *testing.T) {
	path := writeConfig(t, `
mode: worker
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
  kafka:
    enabled: true
    brokers: [kafka:9092]
    topic:
      name: sink-mutations
    consumer:
      group_id: sink-workers
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	configured := loaded.Storage.Kafka
	if configured.DeadLetter.Topic != "sink-mutations.dlq" || configured.Consumer.MaxPollRecords != 500 ||
		configured.Consumer.Retry.MaxAttempts != 10 || configured.Consumer.Retry.Backoff != 100*time.Millisecond ||
		configured.Consumer.Retry.MaxBackoff != 10*time.Second ||
		configured.Topic.Partitions != 4 || configured.Topic.ReplicationFactor != 2 ||
		configured.Topic.Retention != 72*time.Hour {
		t.Fatalf("Kafka defaults = %#v", configured)
	}
}

func TestLoadConfigRejectsInvalidKafkaTopicSettings(t *testing.T) {
	tests := []struct {
		name      string
		setting   string
		wantError string
	}{
		{
			name:      "zero partitions",
			setting:   "partitions: 0",
			wantError: "storage.kafka.topic.partitions must be a positive integer",
		},
		{
			name:      "zero replication factor",
			setting:   "replication_factor: 0",
			wantError: "storage.kafka.topic.replication_factor must be a positive integer",
		},
		{
			name:      "zero retention",
			setting:   "retention: 0s",
			wantError: "storage.kafka.topic.retention must be a positive duration",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contents := `mode: engine
storage:
  name: primary
  driver: mongodb
  mongodb:
    uri: mongodb://mongodb:27017
  kafka:
    enabled: true
    brokers: [kafka:9092]
    topic:
      name: sink-mutations
      ` + test.setting + "\n"
			path := writeConfig(t, contents)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Load() error = %v", err)
			}
		})
	}
}

func TestLoadConfigDoesNotReadLegacyEnvironmentVariables(t *testing.T) {
	t.Setenv("SINK_MODE", "worker")
	t.Setenv("SINK_MONGODB_URI", "mongodb://legacy-environment:27017")
	path := writeConfig(t, `
mode: engine
storage:
  name: configured
  driver: mongodb
  mongodb:
    uri: mongodb://configured:27017
`)
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Mode != ModeEngine || loaded.Storage.MongoDB.URI != "mongodb://configured:27017" {
		t.Fatalf("Load() = %#v", loaded)
	}
}

func TestExampleConfigurationFilesLoad(t *testing.T) {
	paths := []string{
		"../../config.gateway.example.yaml",
		"../../config.engine.example.yaml",
		"../../config.worker.example.yaml",
		"../../examples/quickstart/engine.yaml", "../../examples/quickstart/worker.yaml", "../../examples/quickstart/gateway.yaml",
		"../../examples/kubernetes/sink.yaml",
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			_, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%q) error = %v", path, err)
			}
		})
	}
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte(contents), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
