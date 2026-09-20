package kafka

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/capacity"
	"github.com/batchstream/sink/internal/merge"
	"github.com/batchstream/sink/internal/queue"
	"github.com/batchstream/sink/internal/service"
	"github.com/batchstream/sink/internal/storage/memory"
	"github.com/batchstream/sink/internal/testuri"
	"github.com/batchstream/sink/internal/worker"
	"github.com/liran/sink-go/uri"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type capacitySplitHandler struct {
	processor *worker.Processor
	calls     chan int
}

func (h *capacitySplitHandler) HandleBatch(ctx context.Context, mutations []queue.Mutation) []error {
	results := h.processor.HandleBatch(ctx, mutations)
	select {
	case h.calls <- len(mutations):
	default:
	}
	return results
}

func TestWorkerPausesForMemoryPressureAndCommitsCollectedPoll(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "review-capacity", "review-capacity.dlq"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	luaOpts := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOpts)
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	// The admitted poll is processed whole after memory pressure recovers.
	coreOpts := service.Options{BoundStore: "primary", Storage: store, Lua: engine}
	core, err := service.New(coreOpts)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := worker.NewProcessor(core)
	if err != nil {
		t.Fatal(err)
	}
	handler := &capacitySplitHandler{processor: processor, calls: make(chan int, 100)}
	pressure := &pressureCounter{}
	pressure.pages.Store(90)
	memoryOpts := capacity.Options{Bytes: int64(100 * os.Getpagesize()), HighPercent: 80, LowPercent: 70, Files: pressure}
	guard, err := capacity.New(memoryOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts := WorkerOptions{Memory: guard, Brokers: cluster.ListenAddrs(), Store: "primary", Topic: "review-capacity", GroupID: "review-capacity", DeadLetterTopic: "review-capacity.dlq", Handler: handler, MaxPollRecords: 2, MaxRetryAttempts: 1, RetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond}
	w, err := NewWorker(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var records []*kgo.Record
	for _, id := range []string{"a", "b"} {
		mutation := reliabilityPut(sink.WriteMode_WRITE_MODE_UPSERT, `{"value":"`+strings.Repeat("x", 600)+`"}`)
		kind := uri.StringKey(id)
		key := kind
		mutation.Write.Address.Uri = testuri.WithKey(mutation.Write.Address.GetUri(), key)
		payload, err := queue.MarshalMutation(mutation)
		if err != nil {
			t.Fatal(err)
		}
		record := &kgo.Record{Topic: "review-capacity", Value: payload}
		records = append(records, record)
	}
	if err := w.client.ProduceSync(t.Context(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() { cancel(); <-done }()
	// No handler call or offset commit may occur while intake is paused.
	select {
	case <-handler.calls:
		t.Fatal("Worker polled above its high watermark")
	case <-time.After(2 * capacity.SampleInterval):
	}
	// Replace the filesystem only while no concurrent counter read can observe it.
	// The fake counter below is backed by an atomic value for subsequent samples.
	pressure.pages.Store(60)
	select {
	case size := <-handler.calls:
		if size != 2 {
			t.Fatalf("unexpected poll size %d", size)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not process poll")
	}
	admin := kadm.NewClient(w.client)
	waitRecovery(t, func() bool {
		offsets, err := admin.FetchOffsets(t.Context(), "review-capacity")
		return err == nil && offsets["review-capacity"][0].At == 2
	})
	ends, err := admin.ListEndOffsets(t.Context(), "review-capacity.dlq")
	if err != nil {
		t.Fatal(err)
	}
	if ends["review-capacity.dlq"][0].Offset != 0 {
		t.Fatal("valid records were quarantined")
	}
}

// pressureCounter supplies changing OS readings without a test-only function hook.
type pressureCounter struct{ pages atomic.Int64 }

func (p *pressureCounter) Open(name string) (fs.File, error) {
	files := fstest.MapFS{"proc/self/statm": {Data: []byte(fmt.Sprintf("100 %d", p.pages.Load()))}}
	return files.Open(name)
}
