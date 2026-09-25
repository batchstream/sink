package backpressure

import (
	"container/list"
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reservation charges retained request bytes across collection and admission.
// Moving a request into a batch never releases and reacquires its budget.
type Reservation struct {
	controller *Controller
	bytes      int
	ticket     *Ticket
	released   bool
}

// Ticket is one ready execution, whether it contains one RPC or a whole batch.
// Polling it never blocks a batcher's cancellation/completion event loop.
type Ticket struct {
	controller   *Controller
	entry        *list.Element
	turn         chan struct{}
	reservations []*Reservation
	enqueued     time.Time
}

func (c *Controller) Reserve(requestBytes int) (*Reservation, error) {
	if requestBytes < 0 {
		return nil, status.Error(codes.InvalidArgument, "negative admission size")
	}
	reservation := &Reservation{controller: c, bytes: requestBytes}
	if c == nil {
		return reservation, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if requestBytes > c.maxQueuedBytes-c.bufferedBytes {
		c.rejectQueueLocked()
		return nil, ErrQueueFull
	}
	c.bufferedBytes += requestBytes
	return reservation, nil
}

func (r *Reservation) Release() {
	if r == nil || r.controller == nil {
		return
	}
	c := r.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	r.releaseLocked()
}

func (r *Reservation) releaseLocked() {
	if r.released {
		return
	}
	r.released = true
	r.controller.bufferedBytes -= r.bytes
	if r.ticket != nil {
		r.controller.queuedBytes -= r.bytes
		r.ticket = nil
	}
}

// Enqueue transfers existing byte reservations into one FIFO task. A full task
// queue leaves ownership with the producer; changed wakes a batcher to retry.
// It does not count such retries as rejected business requests.
func (c *Controller) Enqueue(reservations []*Reservation) (*Ticket, <-chan struct{}) {
	ticket := &Ticket{controller: c, reservations: reservations, turn: make(chan struct{}), enqueued: time.Now()}
	if c == nil {
		return ticket, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waiters.Len() >= c.maxQueuedTasks {
		return nil, c.changed
	}
	for _, reservation := range reservations {
		if reservation.controller != c || reservation.ticket != nil {
			panic("admission reservation has a different owner")
		}
		if !reservation.released {
			reservation.ticket = ticket
			c.queuedBytes += reservation.bytes
		}
	}
	ticket.entry = c.waiters.PushBack(ticket)
	if c.waiters.Len()+c.inFlight > c.limit {
		c.demand = true
	}
	if ticket.entry == c.waiters.Front() {
		close(ticket.turn)
	}
	return ticket, nil
}

// Poll grants a permit only to the FIFO head. Timers only wake that head; they
// neither execute work nor impose an admission deadline.
func (t *Ticket) Poll() (*Permit, <-chan struct{}, time.Duration) {
	c := t.controller
	if c == nil {
		permit := &Permit{}
		return permit, nil, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.entry == nil {
		panic("admission ticket was already settled")
	}
	if t.entry != c.waiters.Front() {
		return nil, t.turn, 0
	}
	permit, changed, delay := c.tryAcquireLocked(time.Now())
	if permit != nil {
		t.finishLocked(true)
	}
	return permit, changed, delay
}

func (t *Ticket) Cancel() {
	if t == nil || t.controller == nil {
		return
	}
	c := t.controller
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.entry != nil {
		t.finishLocked(false)
	}
}

func (t *Ticket) finishLocked(admitted bool) {
	c := t.controller
	head := t.entry == c.waiters.Front()
	c.waiters.Remove(t.entry)
	t.entry = nil
	for _, reservation := range t.reservations {
		reservation.releaseLocked()
	}
	t.reservations = nil
	outcome := "canceled"
	if admitted {
		c.observed.queueAdmitted++
		outcome = "admitted"
	} else {
		c.observed.queueCanceled++
	}
	c.observed.queueDuration.WithLabelValues(outcome).Observe(time.Since(t.enqueued).Seconds())
	if next := c.waiters.Front(); head && next != nil {
		close(next.Value.(*Ticket).turn)
	}
	c.signal()
}

func (c *Controller) rejectQueueLocked() {
	c.observed.rejected++
	c.observed.queueRejected++
}

// Admit reuses an execution's permit, or waits in the same FIFO as ready
// batches. Only the caller's original context ends admission waiting.
func (c *Controller) Admit(ctx context.Context, requestBytes int) (context.Context, *Permit, error) {
	if err := ctx.Err(); err != nil {
		return ctx, nil, status.FromContextError(err).Err()
	}
	if requestBytes < 0 {
		return ctx, nil, status.Error(codes.InvalidArgument, "negative admission size")
	}
	if c == nil {
		return ctx, nil, nil
	}
	if permit, ok := ctx.Value(permitKey{}).(*Permit); ok && permit.controller == c {
		return ctx, nil, nil
	}
	if permit, _, _ := c.TryAcquire(); permit != nil {
		if err := ctx.Err(); err != nil {
			permit.Release()
			return ctx, nil, status.FromContextError(err).Err()
		}
		return permit.Context(ctx), permit, nil
	}
	reservation, err := c.Reserve(requestBytes)
	if err != nil {
		return ctx, nil, err
	}
	defer reservation.Release()
	reservations := []*Reservation{reservation}
	ticket, _ := c.Enqueue(reservations)
	if ticket == nil {
		c.mu.Lock()
		c.rejectQueueLocked()
		c.mu.Unlock()
		return ctx, nil, ErrQueueFull
	}
	defer ticket.Cancel()
	for {
		if err := ctx.Err(); err != nil {
			return ctx, nil, status.FromContextError(err).Err()
		}
		permit, changed, delay := ticket.Poll()
		if permit != nil {
			if err := ctx.Err(); err != nil {
				permit.Release()
				return ctx, nil, status.FromContextError(err).Err()
			}
			return permit.Context(ctx), permit, nil
		}
		if err := wait(ctx, changed, delay); err != nil {
			return ctx, nil, status.FromContextError(err).Err()
		}
	}
}
