package backpressure

import (
	"container/list"
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readWaiter struct {
	bytes    int
	turn     chan struct{}
	enqueued time.Time
}

// AdmitRead waits in a shared FIFO for Query and Count. Only the caller's context
// ends waiting; cooldown timers wake the head without imposing a new deadline.
// Waiting payloads remain bounded separately from executing Store work.
func (c *Controller) AdmitRead(ctx context.Context, requestBytes int) (context.Context, *Permit, error) {
	if err := ctx.Err(); err != nil {
		return ctx, nil, status.FromContextError(err).Err()
	}
	if requestBytes < 0 {
		return ctx, nil, status.Error(codes.InvalidArgument, "negative read admission size")
	}
	if c == nil {
		return ctx, nil, nil
	}
	if permit, ok := ctx.Value(permitKey{}).(*Permit); ok && permit.controller == c {
		return ctx, nil, nil
	}
	c.mu.Lock()
	if c.readWaiters.Len() == 0 {
		permit, _, _ := c.tryAcquireLocked(time.Now())
		if permit != nil {
			c.mu.Unlock()
			if err := ctx.Err(); err != nil {
				permit.Release()
				return ctx, nil, status.FromContextError(err).Err()
			}
			return permit.Context(ctx), permit, nil
		}
	}
	if c.readWaiters.Len() >= c.maxQueuedRequests || requestBytes > c.maxQueuedBytes-c.queuedBytes {
		c.observed.rejected++
		c.observed.queueRejected++
		c.mu.Unlock()
		return ctx, nil, ErrQueueFull
	}
	waiter := &readWaiter{bytes: requestBytes, turn: make(chan struct{}), enqueued: time.Now()}
	entry := c.readWaiters.PushBack(waiter)
	c.queuedBytes += requestBytes
	c.demand = true
	if entry == c.readWaiters.Front() {
		close(waiter.turn)
	}
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		c.mu.Lock()
		c.finishReadWaitLocked(entry, false)
		c.mu.Unlock()
		return ctx, nil, status.FromContextError(ctx.Err()).Err()
	case <-waiter.turn:
	}
	for {
		c.mu.Lock()
		if err := ctx.Err(); err != nil {
			c.finishReadWaitLocked(entry, false)
			c.mu.Unlock()
			return ctx, nil, status.FromContextError(err).Err()
		}
		permit, changed, delay := c.tryAcquireLocked(time.Now())
		if permit != nil {
			c.finishReadWaitLocked(entry, true)
			c.mu.Unlock()
			if err := ctx.Err(); err != nil {
				permit.Release()
				return ctx, nil, status.FromContextError(err).Err()
			}
			return permit.Context(ctx), permit, nil
		}
		c.mu.Unlock()
		if err := wait(ctx, changed, delay); err != nil {
			c.mu.Lock()
			c.finishReadWaitLocked(entry, false)
			c.mu.Unlock()
			return ctx, nil, status.FromContextError(err).Err()
		}
	}
}

func (c *Controller) finishReadWaitLocked(entry *list.Element, admitted bool) {
	waiter := entry.Value.(*readWaiter)
	head := entry == c.readWaiters.Front()
	c.readWaiters.Remove(entry)
	c.queuedBytes -= waiter.bytes
	outcome := "canceled"
	if admitted {
		c.observed.queueAdmitted++
		outcome = "admitted"
	} else {
		c.observed.queueCanceled++
	}
	c.observed.queueDuration.WithLabelValues(outcome).Observe(time.Since(waiter.enqueued).Seconds())
	if next := c.readWaiters.Front(); head && next != nil {
		close(next.Value.(*readWaiter).turn)
	}
}
