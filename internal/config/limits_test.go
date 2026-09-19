package config

import (
	"strings"
	"testing"
	"time"
)

func TestExecutionBudgetsAreIndependentOfTransport(t *testing.T) {
	for _, input := range []string{
		"mode: engine\ngrpc: {max_send_message_bytes: 1MiB}\nexecution: {max_snapshot_bytes: 64MiB, max_output_bytes: 128MiB}\n",
		"mode: worker\nconsumer: {group_id: workers}\nexecution: {max_snapshot_bytes: 64MiB, max_output_bytes: 128MiB}\n",
	} {
		loaded, err := Decode(strings.NewReader(input), strings.NewReader(kafkaStore))
		if err != nil || loaded.Service.Execution.MaxSnapshotBytes != 64<<20 || loaded.Service.Execution.MaxOutputBytes != 128<<20 {
			t.Fatalf("execution depends on gRPC: %v", err)
		}
	}
}

func TestRejectInvalidRoleLimits(t *testing.T) {
	for _, fields := range []string{
		"execution: {max_snapshot_bytes: 0}", "execution: {max_output_bytes: -1}", "execution: {mongodb: {max_concurrent_groups: 0}}",
		"execution: {merge: {max_attempts: 0}}", "execution: {merge: {lua: {timeout: 0s}}}", "execution: {merge: {lua: {max_source_bytes: 0}}}",
		"batching: {max_wait: 0s}", "batching: {max_operations: 0}", "batching: {max_operations: 20, queue: {max_operations: 19}}",
		"batching: {queue: {max_bytes: 1MiB}}", "producer: {max_buffered_bytes: 2GiB}", "producer: {max_buffered_bytes: 1KiB}",
	} {
		if _, err := Decode(strings.NewReader("mode: engine\n"+fields), strings.NewReader(kafkaStore)); err == nil {
			t.Fatalf("invalid limits accepted: %s", fields)
		}
	}
	for _, fields := range []string{
		"processing_timeout: 21s", "max_poll_records: 0", "processing_timeout: 0s", "processing_timeout: 20",
		"retry: {max_backoff: 4611686018427387904ns}", "retry: {backoff: 2s, max_backoff: 1s}", "retry: {max_attempts: 0}",
	} {
		input := "mode: worker\nconsumer:\n  group_id: workers\n  " + fields
		if _, err := Decode(strings.NewReader(input), strings.NewReader(kafkaStore)); err == nil {
			t.Fatalf("invalid consumer accepted: %s", fields)
		}
	}
}

func TestKafkaSharedPolicyValidation(t *testing.T) {
	for _, kafka := range []string{
		"enabled: true", "enabled: true\n  brokers: [localhost:1]", "topic: {retention: 1ns}", "dead_letter: {retention: 1ns}",
		"partitions: 0", "replication_factor: 1\n  min_insync_replicas: 2", "max_record_bytes: 65MiB", "min_insync_replicas: 0",
		"enabled: true\n  brokers: [localhost:1]\n  topic: {name: a}\n  dead_letter: {name: a}",
		"topic: {partitions: 4}", "topic: {replication_factor: 2}", "topic: {min_insync_replicas: 1}", "topic: {max_record_bytes: 900KiB}",
		"dead_letter: {topic: a.dlq}", "dead_letter: {partitions: 4}",
		"consumer: {}", "producer: {}",
	} {
		if _, err := Decode(strings.NewReader("mode: engine"), strings.NewReader(minimalStorage+"kafka:\n  "+kafka)); err == nil {
			t.Fatalf("invalid Kafka policy accepted: %s", kafka)
		}
	}
	for _, test := range []struct{ component, store string }{
		{"mode: worker\nconsumer: {group_id: workers}", minimalStorage},
		{"mode: worker", kafkaStore}, {"mode: engine\nproducer: {}", minimalStorage},
	} {
		if _, err := Decode(strings.NewReader(test.component), strings.NewReader(test.store)); err == nil {
			t.Fatal("invalid role/Kafka combination accepted")
		}
	}
}

func TestKafkaSharedPolicyLoading(t *testing.T) {
	shared := minimalStorage + `kafka:
  enabled: true
  brokers: [localhost:9092]
  partitions: 8
  replication_factor: 3
  min_insync_replicas: 2
  max_record_bytes: 2MiB
  topic: {name: mutations, retention: 48h}
  dead_letter: {name: rejected, retention: 240h}
`
	for _, component := range []string{"mode: engine", "mode: worker\nconsumer: {group_id: workers}"} {
		loaded, err := Decode(strings.NewReader(component), strings.NewReader(shared))
		if err != nil {
			t.Fatal(err)
		}
		kafka := loaded.Storage.Kafka
		if kafka.Partitions != 8 || kafka.ReplicationFactor != 3 || kafka.MinInSyncReplicas != 2 || kafka.MaxRecordBytes != 2<<20 {
			t.Fatalf("shared Kafka policy not loaded for %s: %+v", component, kafka)
		}
		if kafka.Topic.Name != "mutations" || kafka.Topic.Retention != 48*time.Hour || kafka.DeadLetter.Name != "rejected" || kafka.DeadLetter.Retention != 240*time.Hour {
			t.Fatalf("independent Topic settings not loaded: %+v", kafka)
		}
	}
}
