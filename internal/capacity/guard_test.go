package capacity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestWatermarksOnlyGateNewWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		files := fstest.MapFS{}
		opts := Options{Bytes: int64(100 * os.Getpagesize()), HighPercent: 80, LowPercent: 70, Files: files}
		guard, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		set := func(pages int) {
			files["proc/self/statm"] = &fstest.MapFile{Data: []byte(fmt.Sprintf("100 %d", pages))}
			time.Sleep(SampleInterval)
		}
		set(79)
		admitted, err := guard.Admit(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		set(80)
		if _, err := guard.Admit(t.Context()); !errors.Is(err, ErrBusy) {
			t.Fatalf("high watermark admitted new work: %v", err)
		}
		if _, err := guard.Admit(admitted); err != nil {
			t.Fatalf("admitted work was interrupted: %v", err)
		}
		set(75)
		if !guard.Blocked() {
			t.Fatal("recovered before the low watermark")
		}
		set(70)
		if _, err := guard.Admit(t.Context()); err != nil {
			t.Fatalf("low watermark did not recover: %v", err)
		}
		if admitted.Err() != nil {
			t.Fatal("pressure canceled an existing request")
		}
		canceled, cancel := context.WithCancel(admitted)
		cancel()
		if _, err := guard.Admit(canceled); err == nil {
			t.Fatal("caller cancellation ignored")
		}
	})
}

func TestWatermarkWaitRecoveryAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		files := fstest.MapFS{"proc/self/statm": {Data: []byte("100 90")}}
		opts := Options{Bytes: int64(100 * os.Getpagesize()), HighPercent: 80, LowPercent: 70, Files: files}
		guard, err := New(opts)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		ctx, cancel := context.WithCancel(t.Context())
		go func() { done <- guard.Wait(ctx) }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("pressure did not pause polling")
		default:
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		go func() { done <- guard.Wait(t.Context()) }()
		synctest.Wait()
		guard.mu.Lock()
		files["proc/self/statm"] = &fstest.MapFile{Data: []byte("100 69")}
		guard.mu.Unlock()
		time.Sleep(SampleInterval)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestConcurrentAdmissionAndMetrics(t *testing.T) {
	files := fstest.MapFS{"proc/self/statm": {Data: []byte("100 90")}}
	opts := Options{Bytes: int64(100 * os.Getpagesize()), HighPercent: 80, LowPercent: 70, Files: files, Role: "engine", Store: "primary", Source: "configured"}
	guard, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	var calls sync.WaitGroup
	for range 32 {
		calls.Go(func() {
			if _, err := guard.Admit(t.Context()); !errors.Is(err, ErrBusy) {
				t.Errorf("unexpected admission: %v", err)
			}
		})
	}
	calls.Wait()
	registry := prometheus.NewRegistry()
	registry.MustRegister(guard)
	metrics, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, family := range metrics {
		found[family.GetName()] = true
	}
	if len(found) != 7 {
		t.Fatalf("obsolete or unexpected memory metrics: %v", found)
	}
	for _, name := range []string{"sink_memory_limit_bytes", "sink_memory_used_bytes", "sink_memory_watermark_bytes", "sink_memory_pressure", "sink_memory_admitted_total", "sink_memory_rejected_total", "sink_memory_limit_source_info"} {
		if !found[name] {
			t.Fatalf("missing metric %s", name)
		}
	}
}

func TestMemoryCounterFallbackAndValidation(t *testing.T) {
	for _, raw := range []string{"", "100 invalid", "100 -1", "100 9223372036854775807"} {
		files := fstest.MapFS{"proc/self/statm": {Data: []byte(raw)}}
		used, source := processUsage(files)
		if source != "go_runtime" || used <= 0 {
			t.Fatalf("invalid fallback: %d %s", used, source)
		}
	}
	for _, opts := range []Options{{Bytes: 1024, HighPercent: 80, LowPercent: 80}, {Bytes: 1024, HighPercent: 100, LowPercent: 70}, {Bytes: 512, HighPercent: 80, LowPercent: 70}, {Bytes: 1024, HighPercent: 80, LowPercent: 0}} {
		if _, err := New(opts); err == nil {
			t.Fatal("invalid memory configuration accepted")
		}
	}
}
