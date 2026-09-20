package service

import (
	"context"
	"testing"
	"time"
)

func TestBatchWithoutCallerDeadlineHasNoSyntheticDeadline(t *testing.T) {
	short, cancelShort := context.WithTimeout(context.Background(), time.Hour)
	defer cancelShort()
	unbounded, cancelUnbounded := context.WithCancel(context.Background())
	defer cancelUnbounded()
	calls := []*batchCall[int, int]{{ctx: short}, {ctx: unbounded}}
	ctx, release := batchExecutionContext(context.Background(), calls, 0)
	defer release()
	if deadline, ok := ctx.Deadline(); ok {
		t.Fatalf("invented deadline: %v", deadline)
	}
	cancelShort()
	if ctx.Err() != nil {
		t.Fatal("one canceled caller stopped shared work")
	}
	cancelUnbounded()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("all callers canceled but shared work survived")
	}
}

func TestBatchInheritsLatestCallerDeadline(t *testing.T) {
	first, cancelFirst := context.WithTimeout(context.Background(), time.Minute)
	defer cancelFirst()
	last, cancelLast := context.WithTimeout(context.Background(), time.Hour)
	defer cancelLast()
	calls := []*batchCall[int, int]{{ctx: first}, {ctx: last}}
	ctx, cancel := batchExecutionContext(context.Background(), calls, 0)
	defer cancel()
	want, _ := last.Deadline()
	got, ok := ctx.Deadline()
	if !ok || !got.Equal(want) {
		t.Fatalf("deadline = %v, want %v", got, want)
	}
}
