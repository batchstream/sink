package service

import (
	"context"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
)

func TestCanceledReturnReleasesCandidateBeforeSibling(t *testing.T) {
	backend := newHeldReadStorage(t)
	server := completionServer(t, backend)
	options := capacity.Options{Bytes: 192 << 10, BurstPercent: 10, WaitTimeout: 20 * time.Millisecond}
	pool, err := capacity.New(options)
	if err != nil {
		t.Fatal(err)
	}
	server.server.memory = pool
	firstScope, secondScope := pool.NewScope(), pool.NewScope()
	defer firstScope.Release()
	defer secondScope.Release()
	firstContext, cancel := context.WithCancel(capacity.WithScope(t.Context(), firstScope))
	defer cancel()
	secondContext := capacity.WithScope(t.Context(), secondScope)
	source := []byte(`return function(current, incoming) local value = "x"; for i=1,15 do value=value..value end; return {value=value} end`)
	first, second := completionMerge("canceled", 1), completionMerge("healthy", 1)
	first.GetMerge().GetLuaProgram().Source = source
	second.GetMerge().GetLuaProgram().Source = source
	first.ReturnDocument = true
	mode := sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED
	firstCall := completionWriteCall(firstContext, mode, first)
	secondCall := completionWriteCall(secondContext, mode, second)
	calls := []*batchCall[*sink.WriteRequest, *sink.WriteResponse]{firstCall, secondCall}
	done := make(chan struct{})
	go func() { server.executeWrites(t.Context(), calls); close(done) }()
	awaitCompletion(t, backend.entered)
	cancel()
	backend.unblock()
	awaitCompletion(t, done)
	firstResult, secondResult := awaitCompletion(t, firstCall.result), awaitCompletion(t, secondCall.result)
	t.Logf("canceled: %v error=%v", firstResult.response, firstResult.err)
	t.Logf("healthy: %v error=%v", secondResult.response, secondResult.err)
	if secondResult.err != nil || secondResult.response.GetResults()[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatal("discarded candidate reservation poisoned healthy sibling")
	}
}
