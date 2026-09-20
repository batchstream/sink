package config

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const minimalStorage = "name: primary\nstorage:\n  driver: mongodb\n  mongodb:\n    uri: mongodb://127.0.0.1:1\n"
const gatewayConfig = "mode: gateway\nforwarding:\n  routes: [{store: primary, target: '127.0.0.1:1', tls: {insecure: true}}]\n"
const kafkaStore = minimalStorage + "kafka:\n  enabled: true\n  brokers: [127.0.0.1:1]\n  topic: {name: mutations}\n"

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSharedStoreWithIndependentRoleSettings(t *testing.T) {
	storePath := writeConfig(t, kafkaStore)
	enginePath := writeConfig(t, "mode: engine\nproducer: {max_buffered_bytes: 8MiB}\n")
	workerPath := writeConfig(t, "mode: worker\nconsumer: {group_id: workers, max_poll_records: 2000}\n")
	engine, err := Load(enginePath, storePath)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := Load(workerPath, storePath)
	if err != nil {
		t.Fatal(err)
	}
	if engine.Storage.Name != worker.Storage.Name || engine.Storage.MongoDB.URI != worker.Storage.MongoDB.URI || !reflect.DeepEqual(engine.Storage.Kafka.Topic, worker.Storage.Kafka.Topic) {
		t.Fatal("shared identity or dependencies diverged")
	}
	if worker.Storage.Kafka.Consumer.MaxPollRecords != 2000 || engine.Storage.Kafka.Producer.MaxBufferedBytes != 8<<20 {
		t.Fatal("role tuning was not independent")
	}
	if worker.Storage.Kafka.Consumer.GroupID != "workers" || engine.Storage.Kafka.Consumer.GroupID != "" {
		t.Fatal("consumer leaked between roles")
	}
}

func TestComponentDefaults(t *testing.T) {
	engine, err := Decode(strings.NewReader("mode: engine\n"), strings.NewReader(minimalStorage))
	if err != nil {
		t.Fatal(err)
	}
	if engine.Service.Request.MaxOperations != 0 || engine.Service.Request.MaxReadBytes != 0 {
		t.Fatal("Engine acquired public request limits")
	}
	if engine.Service.Batching.MaxOperations != 32 || engine.Service.Batching.MaxWait != 2*time.Millisecond || engine.Service.Batching.Queue.MaxBytes != 128<<20 {
		t.Fatal("batch defaults changed")
	}
	if engine.Service.Merge.Lua.MaxResultBytes != 16<<20 {
		t.Fatal("execution defaults changed")
	}
	if engine.Storage.Kafka.Enabled || engine.Prometheus.Enabled || engine.Health.Address != ":8081" || engine.GRPC.MaxSendMessageBytes != 64<<20 {
		t.Fatal("process defaults changed")
	}
	gateway, err := Decode(strings.NewReader(gatewayConfig), nil)
	if err != nil || gateway.Service.Request.MaxOperations != 1000 {
		t.Fatalf("Gateway defaults: %v", err)
	}
	worker, err := Decode(strings.NewReader("mode: worker\nconsumer: {group_id: workers}\n"), strings.NewReader(kafkaStore))
	if err != nil {
		t.Fatal(err)
	}
	kafka := worker.Storage.Kafka
	if kafka.Partitions != 4 || kafka.ReplicationFactor != 2 || kafka.MinInSyncReplicas != 1 || kafka.MaxRecordBytes != 900<<10 || kafka.Topic.Retention != 72*time.Hour || kafka.DeadLetter.Retention != 720*time.Hour {
		t.Fatal("shared Kafka defaults changed")
	}
	if kafka.DeadLetter.Name != "mutations.dlq" || kafka.Consumer.MaxPollRecords != 500 || kafka.Consumer.ProcessingTimeout != 20*time.Second || kafka.Consumer.Retry.MaxAttempts != 10 {
		t.Fatal("consumer defaults changed")
	}
}

func TestStoreIdentityAndBackendValidation(t *testing.T) {
	for _, shared := range []string{
		"name: ''\nstorage: {driver: mongodb}", "name: INVALID\nstorage: {driver: mongodb}", "name: 'a/b'\nstorage: {driver: mongodb}",
		"name: primary\nstorage: {driver: unknown}", "name: primary\nstorage: {driver: mongodb}",
		"name: primary\nstorage: {driver: opensearch, search: {endpoints: []}}",
		"name: primary\nstorage: {driver: opensearch, search: {endpoints: [http://search:9200], username: test}}",
		"name: primary\nstorage: {driver: opensearch, search: {endpoints: [http://search:9200], username: test, password: test, api_key: test}}",
		minimalStorage + "consumer: {group_id: workers}\n", minimalStorage + "producer: {}\n",
		strings.Replace(minimalStorage, "uri: mongodb://127.0.0.1:1", "uri: mongodb://127.0.0.1:1\n    max_concurrent_writes: 0", 1),
	} {
		loaded, err := Decode(strings.NewReader("mode: engine\n"), strings.NewReader(shared))
		if err == nil || !reflect.ValueOf(loaded).IsZero() {
			t.Fatalf("invalid Store accepted: %v", err)
		}
	}
	for _, driver := range []string{"elasticsearch", "opensearch"} {
		shared := "name: search\nstorage:\n  driver: " + driver + "\n  search: {endpoints: [http://search:9200], username: test, password: test}\n"
		loaded, err := Decode(strings.NewReader("mode: engine\n"), strings.NewReader(shared))
		if err != nil || loaded.Storage.Search.Username != "test" {
			t.Fatalf("search configuration: %v", err)
		}
		_, err = Decode(strings.NewReader("mode: engine\nexecution:\n  mongodb: {max_concurrent_writes: 3}\n"), strings.NewReader(shared))
		if err == nil {
			t.Fatal("irrelevant MongoDB tuning accepted")
		}
	}
}

func TestStrictDocumentsAndRequiredPaths(t *testing.T) {
	cases := []struct {
		component, store string
		want             string
	}{
		{"mode: engine", "", "--store-config"}, {"mode: worker", "", "--store-config"}, {gatewayConfig, minimalStorage, "must not use"},
		{"mode: server", "", "mode is required"}, {"", "", "EOF"},
		{"mode: engine\nunknown: true", minimalStorage, "field unknown"},
		{"mode: engine\nmode: worker", minimalStorage, "already defined"},
		{"mode: engine\n---\nmode: engine", minimalStorage, "multiple YAML"},
		{"mode: engine", minimalStorage + "---\nname: other", "multiple YAML"},
		{"mode: engine", minimalStorage + "name: other", "already defined"},
		{strings.Repeat(" ", MaxFileBytes+1), "", "exceeds 4 MiB"},
		{"mode: engine", strings.Repeat(" ", MaxFileBytes+1), "exceeds 4 MiB"},
	}
	for _, test := range cases {
		var store io.Reader
		if test.store != "" {
			store = strings.NewReader(test.store)
		}
		loaded, err := Decode(strings.NewReader(test.component), store)
		if err == nil || !strings.Contains(err.Error(), test.want) || !reflect.ValueOf(loaded).IsZero() {
			t.Fatalf("want %q, got %v", test.want, err)
		}
	}
	if _, err := Load("/nonexistent/component", ""); err == nil {
		t.Fatal("missing component accepted")
	}
	if _, err := Load(writeConfig(t, "mode: engine"), "/nonexistent/store"); err == nil {
		t.Fatal("missing Store accepted")
	}
}

func TestConfigurationExamples(t *testing.T) {
	for _, mode := range []string{"gateway", "engine", "worker"} {
		storePath := ""
		if mode != "gateway" {
			storePath = "../../configs/stores/primary.yaml"
		}
		if _, err := Load("../../configs/"+mode+".yaml", storePath); err != nil {
			t.Fatal(err)
		}
	}
}
