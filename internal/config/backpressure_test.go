package config

import (
	"fmt"
	"strings"
	"testing"

	"github.com/batchstream/sink/internal/backpressure"
)

func TestStoreConcurrencyCeilingIndependentOfRoleAndMemory(t *testing.T) {
	for _, mode := range []string{"engine", "worker"} {
		base := "mode: " + mode + "\n"
		if mode == "worker" {
			base += "consumer: {group_id: workers}\n"
		}
		loaded, err := Decode(strings.NewReader(base), strings.NewReader(kafkaStore))
		if err != nil || loaded.Service.StoreMaxConcurrent != backpressure.DefaultMaxConcurrent {
			t.Fatalf("%s default: %+v %v", mode, loaded.Service, err)
		}
		for _, maximum := range []int{-1, 0, 1, 128, 4096, 4097} {
			storeConfig := fmt.Sprintf("name: primary\nmax_concurrent: %d\n", maximum)
			store := strings.Replace(kafkaStore, "name: primary\n", storeConfig, 1)
			loaded, err := Decode(strings.NewReader(base), strings.NewReader(store))
			valid := maximum >= 1 && maximum <= 4096
			if valid && (err != nil || loaded.Service.StoreMaxConcurrent != maximum) || !valid && err == nil {
				t.Fatalf("%s maximum=%d: %v", mode, maximum, err)
			}
		}
	}
}

func TestAdmissionQueueIndependentOfBatchOperationLimits(t *testing.T) {
	component := "mode: engine\nbatching: {max_operations: 32, queue: {max_operations: 4000}}\nexecution: {queue: {max_tasks: 7, max_bytes: 256MiB}}\n"
	loaded, err := Decode(strings.NewReader(component), strings.NewReader(minimalStorage))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Service.Admission.MaxTasks != 7 || loaded.Service.Admission.MaxBytes != 256<<20 || loaded.Service.Batching.Queue.MaxOperations != 4000 {
		t.Fatalf("queue task and operation limits were conflated: %+v", loaded.Service)
	}
	defaults, err := Decode(strings.NewReader("mode: engine"), strings.NewReader(minimalStorage))
	if err != nil || defaults.Service.Admission.MaxTasks != 10000 || defaults.Service.Admission.MaxBytes != 128<<20 {
		t.Fatalf("admission defaults: %+v %v", defaults.Service.Admission, err)
	}
	for _, queue := range []string{"max_tasks: 0", "max_tasks: -1", "max_bytes: 0", "max_bytes: 1MiB", "max_operations: 10"} {
		input := "mode: engine\nexecution: {queue: {" + queue + "}}\n"
		if _, err := Decode(strings.NewReader(input), strings.NewReader(minimalStorage)); err == nil {
			t.Fatalf("invalid admission queue accepted: %s", queue)
		}
	}
	worker := "mode: worker\nconsumer: {group_id: workers}\nexecution: {queue: {max_tasks: 10}}\n"
	if _, err := Decode(strings.NewReader(worker), strings.NewReader(kafkaStore)); err == nil {
		t.Fatal("Engine admission queue accepted by Worker")
	}
}
