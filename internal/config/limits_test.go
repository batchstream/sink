package config

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

const minimalStorage = `mode: engine
storage:
  name: primary
  database_id: test-database
  driver: mongodb
  mongodb:
    uri: mongodb://127.0.0.1:1
`

func TestDecodeDerivesLimitsFromTheirOwners(t *testing.T) {
	input := minimalStorage + `grpc:
  max_send_message_bytes: 2097152
  max_receive_message_bytes: 268435456
service:
  request:
    timeout: 1s
    max_operations: 15000
  execution:
    max_requests: 8
    max_bytes: 67108864
  publish:
    max_requests: 7
`
	loaded, err := Decode(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	service := loaded.Service
	if service.Request.MaxReadBytes != 1<<20 || service.Execution.Scan.MaxRequests != 4 ||
		service.Execution.Scan.MaxBytes != 32<<20 ||
		service.Execution.Scan.AdmissionWait != time.Second {
		t.Fatalf("dependent request and execution defaults: %+v", service)
	}
	if service.Batching.MaxOperations != 15000 || service.Batching.Queue.MaxOperations != 15000 || service.Batching.Queue.MaxBytes != 256<<20 {
		t.Fatalf("dependent batching defaults: %+v", service.Batching)
	}
	if service.Publish.MaxRequests != 7 || service.Publish.MaxBytes != 256<<20 {
		t.Fatalf("execution limits leaked into publication: %+v", service.Publish)
	}
	if service.Execution.Queue.MaxWait != time.Second || service.Execution.Queue.MaxRequests != 1024 || service.Execution.Queue.MaxBytes != 32<<20 {
		t.Fatalf("invalid direct admission defaults: %+v", service.Execution.Queue)
	}
}

func TestDecodeRejectsInvalidResourceLimitsWithoutReturningPartialConfig(t *testing.T) {
	cases := []struct {
		section string
		field   string
		values  []string
	}{
		{section: "request", field: "timeout", values: []string{"0s", "-1ms", "301s", "9223372036854775808ns", "30"}},
		{section: "request", field: "max_read_bytes", values: []string{"0", "-1", "33554433"}},
		{section: "execution", field: "max_requests", values: []string{"0", "-1", "10001"}},
		{section: "execution", field: "max_bytes", values: []string{"0", "-1", "18253611008"}},
		{section: "execution:\n    queue", field: "max_requests", values: []string{"0", "10001"}},
		{section: "execution:\n    queue", field: "max_bytes", values: []string{"0", "17GiB"}},
		{section: "execution:\n    queue", field: "max_wait", values: []string{"0s", "-1s", "30.001s"}},
		{section: "execution:\n    scan", field: "max_requests", values: []string{"0", "129"}},
		{section: "execution:\n    scan", field: "max_bytes", values: []string{"0", "268435457"}},
		{section: "execution:\n    scan", field: "admission_wait", values: []string{"0s", "-1s", "30.001s"}},
		{section: "publish", field: "max_requests", values: []string{"0", "-1", "10001"}},
		{section: "publish", field: "max_bytes", values: []string{"0", "-1", "18253611008"}},
	}
	for _, test := range cases {
		for _, value := range test.values {
			t.Run(test.section+"/"+test.field+"/"+value, func(t *testing.T) {
				indent := "    "
				if strings.Contains(test.section, "\n") {
					indent += "  "
				}
				input := fmt.Sprintf("%sservice:\n  %s:\n%s%s: %s\n", minimalStorage, test.section, indent, test.field, value)
				loaded, err := Decode(strings.NewReader(input))
				if err == nil || !reflect.ValueOf(loaded).IsZero() {
					t.Fatalf("invalid configuration escaped validation: config=%+v error=%v", loaded, err)
				}
			})
		}
	}
}

func TestDecodeRejectsRemovedConfigurationPaths(t *testing.T) {
	for _, field := range []string{"max_in_flight_requests: 10", "max_publish_bytes: 1024", "max_operations: 10", "lua: {}", "store_execution_bytes: {}"} {
		_, err := Decode(strings.NewReader(minimalStorage + "service:\n  " + field + "\n"))
		if err == nil || !strings.Contains(err.Error(), "field ") {
			t.Fatalf("old service field was silently accepted: %s, %v", field, err)
		}
	}
	_, err := Decode(strings.NewReader(minimalStorage + "shutdown_timeout_seconds: 15\n"))
	if err == nil || !strings.Contains(err.Error(), "shutdown_timeout_seconds") {
		t.Fatalf("old duration field was silently accepted: %v", err)
	}
}

func TestDecodeRejectsKafkaDurationTruncationAndOverflow(t *testing.T) {
	for _, fragment := range []string{
		"topic:\n      retention: 1ns",
		"dead_letter:\n      retention: 1ns",
		"consumer:\n      processing_timeout: 21s",
		"consumer:\n      retry:\n        max_backoff: 4611686018427387904ns",
		"consumer:\n      retry:\n        backoff: 2s\n        max_backoff: 1s",
	} {
		_, err := Decode(strings.NewReader(minimalStorage + "  kafka:\n    " + fragment + "\n"))
		if err == nil || !strings.Contains(err.Error(), "storage.kafka.") {
			t.Fatalf("invalid Kafka duration accepted: %s, %v", fragment, err)
		}
	}
}
