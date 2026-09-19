package service

import (
	"context"
	"testing"
	"time"
)

func TestExecutionContextSurvivesOneCallerButCancelsForAll(t *testing.T) {
	first, cancelFirst := context.WithCancel(t.Context())
	second, cancelSecond := context.WithCancel(t.Context())
	defer cancelFirst()
	defer cancelSecond()
	calls := []*batchCall[int, int]{{ctx: first}, {ctx: second}}
	ctx, cleanup := batchExecutionContext(t.Context(), calls, time.Second)
	defer cleanup()
	cancelFirst()
	select {
	case <-ctx.Done():
		t.Fatal("one caller cancelled the other caller's work")
	case <-time.After(20 * time.Millisecond):
	}
	cancelSecond()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("all cancelled callers retained execution")
	}
}
