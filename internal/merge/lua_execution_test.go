package merge

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/liran/sink/internal/storage"
)

func TestLuaExecutionQueueHonorsCancellation(t *testing.T) {
	tests := []struct {
		name      string
		compiling bool
		deadline  bool
	}{
		{name: "merge canceled"},
		{name: "merge deadline", deadline: true},
		{name: "compile canceled", compiling: true},
		{name: "compile deadline", compiling: true, deadline: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				options := LuaOptions{}
				engine, err := NewLuaEngine(options)
				if err != nil {
					t.Fatal(err)
				}
				program := Program{Source: []byte(`return function(current, incoming) return incoming end`)}
				merger, err := engine.Compile(t.Context(), program)
				if err != nil {
					t.Fatal(err)
				}
				for range cap(engine.executions) {
					engine.executions <- struct{}{}
				}
				ctx, cancel := context.WithCancel(t.Context())
				if test.deadline {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), time.Millisecond)
				}
				defer cancel()
				finished := make(chan error, 1)
				document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{}`)}
				request := Request{Incoming: document}
				go func() {
					if test.compiling {
						uncached := Program{Source: []byte(`return function(current, incoming) return {ok=true} end`)}
						_, err := engine.Compile(ctx, uncached)
						finished <- err
						return
					}
					_, err := merger.Merge(ctx, request)
					finished <- err
				}()
				synctest.Wait()
				select {
				case err := <-finished:
					t.Fatalf("execution bypassed occupied CPU slots: %v", err)
				default:
				}
				if test.deadline {
					time.Sleep(time.Millisecond)
				} else {
					cancel()
				}
				want := ErrExecutionDeadline
				if test.compiling {
					want = context.Canceled
					if test.deadline {
						want = context.DeadlineExceeded
					}
				}
				if err := <-finished; !errors.Is(err, want) {
					t.Fatalf("queued cancellation: %v", err)
				}
				if len(engine.executions) != cap(engine.executions) {
					t.Fatal("canceled waiter released another execution's slot")
				}
				for range cap(engine.executions) {
					<-engine.executions
				}
				if _, err := merger.Merge(t.Context(), request); err != nil {
					t.Fatal(err)
				}
				if len(engine.executions) != 0 {
					t.Fatal("successful execution retained a CPU slot")
				}
			})
		})
	}
}

func TestLuaExecutionBudgetStartsAfterCPUQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		options := LuaOptions{Timeout: 10 * time.Millisecond}
		engine, err := NewLuaEngine(options)
		if err != nil {
			t.Fatal(err)
		}
		program := Program{Source: []byte(`return function(current, incoming) return incoming end`)}
		merger, err := engine.Compile(t.Context(), program)
		if err != nil {
			t.Fatal(err)
		}
		for range cap(engine.executions) {
			engine.executions <- struct{}{}
		}
		finished := make(chan error, 1)
		go func() {
			document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{}`)}
			request := Request{Incoming: document}
			_, err := merger.Merge(t.Context(), request)
			finished <- err
		}()
		synctest.Wait()
		time.Sleep(2 * options.Timeout)
		<-engine.executions
		if err := <-finished; err != nil {
			t.Fatalf("CPU queue consumed the execution budget: %v", err)
		}
		if len(engine.executions) != cap(engine.executions)-1 {
			t.Fatal("execution did not release its CPU slot")
		}
		for range cap(engine.executions) - 1 {
			<-engine.executions
		}
	})
}

func TestLuaExecutionSlotsReleaseAfterFailures(t *testing.T) {
	options := LuaOptions{MaxInstructions: 1000, MaxResultBytes: 1024}
	engine, err := NewLuaEngine(options)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`return function(current, incoming) error("failure") end`,
		`return function(current, incoming) while true do end end`,
		`return function(current, incoming) return 1 end`,
		`return function(current, incoming) return {value=string.pack("c2048", "")} end`,
	} {
		program := Program{Source: []byte(source)}
		merger, err := engine.Compile(t.Context(), program)
		if err != nil {
			t.Fatal(err)
		}
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: []byte(`{}`)}
		request := Request{Incoming: document}
		if _, err := merger.Merge(t.Context(), request); err == nil {
			t.Fatal("invalid execution succeeded")
		}
		if len(engine.executions) != 0 {
			t.Fatal("failed execution retained a CPU slot")
		}
	}
	invalid := Program{Source: []byte(`return 42`)}
	if _, err := engine.Compile(t.Context(), invalid); !errors.Is(err, ErrInvalidProgram) {
		t.Fatalf("invalid chunk succeeded: %v", err)
	}
	if len(engine.executions) != 0 {
		t.Fatal("failed chunk validation retained a CPU slot")
	}
}
