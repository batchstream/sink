package service

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Expire immediately after the second Err snapshot. This places cancellation
// between the parent check and the wait-context check after an admission wakeup.
type admissionExpiringContext struct {
	context.Context
	mu      sync.Mutex
	done    chan struct{}
	err     error
	expired bool
	checks  int
}

func (c *admissionExpiringContext) Done() <-chan struct{} { return c.done }

func (c *admissionExpiringContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.expired {
		return c.err
	}
	c.checks++
	if c.checks == 2 {
		c.expired = true
		close(c.done)
	}
	return nil
}

func TestAdmissionPreservesCancellationAfterWakeup(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pool := completionServer(t, memory.New()).server.admissionPool
				pool.maxInFlightBytes = 10
				busy := admissionRequest{encodedBytes: 3}
				_, releaseBusy, err := pool.admitRequest(t.Context(), busy)
				if err != nil {
					t.Fatal(err)
				}
				defer releaseBusy()
				other := admissionRequest{encodedBytes: 1}
				_, releaseOther, err := pool.admitRequest(t.Context(), other)
				if err != nil {
					t.Fatal(err)
				}
				ctx := &admissionExpiringContext{Context: t.Context(), done: make(chan struct{}), err: cause}
				waiting := admissionRequest{encodedBytes: 8, wait: true}
				finished := make(chan error, 1)
				go func() {
					_, release, err := pool.admitRequest(ctx, waiting)
					if release != nil {
						release()
					}
					finished <- err
				}()
				synctest.Wait()
				releaseOther()
				if err := <-finished; status.Code(err) != status.FromContextError(cause).Code() {
					t.Fatalf("caller cancellation became capacity rejection: %v", err)
				}
				if pool.inFlightBytes != 3 || pool.inFlightRequests != 1 || len(pool.admissionWaiters) != 0 {
					t.Fatal("canceled waiter leaked admission state")
				}
				followup := admissionRequest{encodedBytes: 7}
				_, release, err := pool.admitRequest(t.Context(), followup)
				if status.Code(err) != codes.OK {
					t.Fatalf("canceled waiter blocked available capacity: %v", err)
				}
				release()
			})
		})
	}
}
