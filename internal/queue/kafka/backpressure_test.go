package kafka

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/backpressure"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/queue"
	"github.com/batchstream/sink/internal/service"
	"github.com/batchstream/sink/internal/storage"
	"github.com/batchstream/sink/internal/storage/memory"
	"github.com/batchstream/sink/internal/worker"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type overloadStore struct {
	storage.Storage
	calls []time.Time
}

func (s *overloadStore) Write(ctx context.Context, req storage.WriteRequest) (storage.WriteResponse, error) {
	s.calls = append(s.calls, time.Now())
	if len(s.calls) == 1 {
		response := storage.WriteResponse{Results: make([]storage.WriteResult, len(req.Operations))}
		for index := range response.Results {
			response.Results[index].Status = storage.WriteStatusFailed
			response.Results[index].Err = storage.ResourceExhaustedError(errors.New("backend overloaded"))
		}
		return response, nil
	}
	return s.Storage.Write(ctx, req)
}

func admissionProcessor(t *testing.T, backend storage.Storage, controller *backpressure.Controller) *worker.Processor {
	t.Helper()
	luaOptions := merge.LuaOptions{}
	lua, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	options := service.Options{Storage: backend, Lua: lua, BoundStore: "primary", Admission: controller}
	server, err := service.New(options)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatal(err)
	}
	return processor
}

func TestWorkerAdmissionKeepsOverloadedRetryUncommitted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := backpressure.Options{Store: "primary", Role: "worker", MaxConcurrent: 1}
		controller, err := backpressure.New(opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := controller.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		backend := &overloadStore{Storage: memory.New()}
		processor := admissionProcessor(t, backend, controller)
		w := &Worker{admission: controller, handler: processor, maxRetryAttempts: 3, retryBackoff: time.Nanosecond, maxRetryBackoff: time.Nanosecond}
		mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":1}`)
		mutations := []queue.Mutation{mutation}
		processing, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		results := w.handleWithRetry(processing, mutations)
		cancel()
		if len(backend.calls) != 1 || !errors.Is(results[0], context.DeadlineExceeded) {
			t.Fatalf("retry bypassed cooldown: calls=%d results=%v", len(backend.calls), results)
		}
		record := &kgo.Record{Topic: "mutations", Partition: 0, Offset: 4}
		records := []*kgo.Record{record}
		ready, _, retained := resolvedPrefixes(records, results)
		if len(ready) != 0 || len(retained) != 1 {
			t.Fatal("admission timeout committed or quarantined unresolved work")
		}
		results = w.handleWithRetry(t.Context(), mutations)
		if len(backend.calls) != 2 || results[0] != nil || backend.calls[1].Sub(backend.calls[0]) < 100*time.Millisecond {
			t.Fatalf("Store did not recover after cooldown: calls=%v results=%v", backend.calls, results)
		}
	})
}

func TestWorkerPausesBeforePollWithoutSpendingProcessingTimeoutOrHealth(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "admission", "admission.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	options := backpressure.Options{Store: "primary", Role: "worker", MaxConcurrent: 1}
	controller, err := backpressure.New(options)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := controller.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer permit.Release()
	processor := admissionProcessor(t, memory.New(), controller)
	handler := &capacitySplitHandler{processor: processor, calls: make(chan int, 10)}
	opts := WorkerOptions{Admission: controller, Brokers: cluster.ListenAddrs(), Store: "primary", Topic: "admission", GroupID: "admission", DeadLetterTopic: "admission.dlq", Handler: handler, MaxPollRecords: 1, ProcessingTimeout: 100 * time.Millisecond}
	w, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":1}`)
	payload, err := queue.MarshalMutation(mutation)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "admission", Value: payload}
	if err := w.client.ProduceSync(t.Context(), record).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case <-handler.calls:
		t.Fatal("worker dispatched without Store capacity")
	case <-time.After(300 * time.Millisecond):
	}
	if err := w.Ping(t.Context()); err != nil {
		t.Fatalf("admission waiting made worker unhealthy: %v", err)
	}
	admin := kadm.NewClient(w.client)
	offsets, err := admin.FetchOffsets(t.Context(), "admission")
	if err == nil && offsets["admission"][0].At > 0 {
		t.Fatal("paused poll advanced source offsets")
	}
	permit.Release()
	select {
	case count := <-handler.calls:
		if count != 1 {
			t.Fatal("poll boundary changed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not resume")
	}
	waitRecovery(t, func() bool {
		offsets, err := admin.FetchOffsets(t.Context(), "admission")
		return err == nil && offsets["admission"][0].At == 1
	})
	ends, err := admin.ListEndOffsets(t.Context(), "admission.dlq")
	if err != nil || ends["admission.dlq"][0].Offset != 0 {
		t.Fatal("admission caused a dead letter")
	}
}
