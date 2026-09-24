package backpressure

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestBatchTicketCountsTasksNotOperationsAndTransfersBytes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 1, 1, 100)
		held, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		reservations := make([]*Reservation, 0, 100)
		for range 100 {
			reservation, err := c.Reserve(1)
			if err != nil {
				t.Fatal(err)
			}
			reservations = append(reservations, reservation)
		}
		if c.bufferedBytes != 100 || c.queuedBytes != 0 {
			t.Fatal("collection did not retain its shared byte budget")
		}
		ticket, _ := c.Enqueue(reservations)
		if ticket == nil || c.waiters.Len() != 1 || c.bufferedBytes != 100 || c.queuedBytes != 100 {
			t.Fatal("batch transfer changed bytes or counted its members as tasks")
		}
		if _, err := c.Reserve(1); err != ErrQueueFull {
			t.Fatal("collection and admission did not share one byte budget")
		}
		reservations[0].Release()
		reservations[0].Release()
		if c.bufferedBytes != 99 || c.queuedBytes != 99 {
			t.Fatal("canceled batch member did not release exactly its own bytes")
		}
		held.Release()
		if permit, _, _ := c.TryAcquire(); permit != nil {
			permit.Release()
			t.Fatal("unqueued work overtook a ready batch")
		}
		permit, _, _ := ticket.Poll()
		if permit == nil || c.waiters.Len() != 0 || c.queuedBytes != 0 || c.bufferedBytes != 0 {
			t.Fatal("dispatch did not atomically release waiting reservations")
		}
		ticket.Cancel()
		for _, reservation := range reservations {
			reservation.Release()
		}
		permit.Release()
		if c.inFlight != 0 || c.bufferedBytes != 0 {
			t.Fatal("settlement released capacity twice")
		}
	})
}

func TestFullAdmissionQueueBackpressuresProducerWithoutRejection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 1, 1, 32)
		held, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		first, err := c.Reserve(16)
		if err != nil {
			t.Fatal(err)
		}
		second, err := c.Reserve(16)
		if err != nil {
			t.Fatal(err)
		}
		firstReservations := []*Reservation{first}
		secondReservations := []*Reservation{second}
		ticket, _ := c.Enqueue(firstReservations)
		for range 1000 {
			next, _ := c.Enqueue(secondReservations)
			if next != nil || c.observed.rejected != 0 || c.bufferedBytes != 32 {
				t.Fatal("full queue rejected or released already accepted upstream work")
			}
		}
		_, changed := c.Enqueue(secondReservations)
		ticket.Cancel()
		select {
		case <-changed:
		default:
			t.Fatal("producer lost the queue-capacity wakeup")
		}
		next, _ := c.Enqueue(secondReservations)
		if next == nil || c.queuedBytes != 16 || c.bufferedBytes != 16 {
			t.Fatal("upstream reservation could not transfer after queue capacity freed")
		}
		next.Cancel()
		if c.waiters.Len() != 0 || c.bufferedBytes != 0 || c.queuedBytes != 0 {
			t.Fatal("canceled producer leaked waiting capacity")
		}
	})
}

func TestBatchAndDirectAdmissionShareFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 1, 4, 64)
		held, err := c.Acquire(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer held.Release()
		reservation, err := c.Reserve(16)
		if err != nil {
			t.Fatal(err)
		}
		reservations := []*Reservation{reservation}
		batch, _ := c.Enqueue(reservations)
		done := make(chan *Permit, 1)
		go func() {
			_, permit, err := c.Admit(t.Context(), 16)
			if err != nil {
				t.Error(err)
			}
			done <- permit
		}()
		synctest.Wait()
		held.Release()
		synctest.Wait()
		select {
		case permit := <-done:
			permit.Release()
			t.Fatal("direct request overtook queued batch")
		default:
		}
		permit, _, _ := batch.Poll()
		if permit == nil {
			t.Fatal("FIFO head failed to obtain free capacity")
		}
		permit.Release()
		next := <-done
		next.Release()
		if c.bufferedBytes != 0 || c.waiters.Len() != 0 || c.inFlight != 0 {
			t.Fatal("mixed admission leaked state")
		}
	})
}

func TestReservationReleaseRacesWithTicketCancellation(t *testing.T) {
	c := readController(t, 1, 100, 100)
	for range 100 {
		reservation, err := c.Reserve(1)
		if err != nil {
			t.Fatal(err)
		}
		reservations := []*Reservation{reservation}
		ticket, _ := c.Enqueue(reservations)
		var group sync.WaitGroup
		group.Go(reservation.Release)
		group.Go(ticket.Cancel)
		group.Go(ticket.Cancel)
		group.Wait()
	}
	if c.bufferedBytes != 0 || c.queuedBytes != 0 || c.waiters.Len() != 0 {
		t.Fatal("concurrent cancellation leaked queue ownership")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, permit, err := c.Admit(ctx, 1); err == nil || permit != nil {
		t.Fatal("canceled request received a permit")
	}
}

func TestUncontendedQueuedBatchesDoNotInventGrowthDemand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := readController(t, 64, 10, 64)
		if err := c.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		for range 32 {
			reservation, err := c.Reserve(1)
			if err != nil {
				t.Fatal(err)
			}
			reservations := []*Reservation{reservation}
			ticket, _ := c.Enqueue(reservations)
			permit, _, _ := ticket.Poll()
			if permit == nil {
				t.Fatal("uncontended batch unexpectedly waited")
			}
			sample := c.begin(read, 1)
			time.Sleep(20 * time.Millisecond)
			c.observe(sample, 20*time.Millisecond, healthy)
			permit.Release()
			time.Sleep(time.Second)
		}
		if c.limit != initialConcurrent || c.observed.increases != 0 {
			t.Fatal("merely passing through the queue inflated Store concurrency")
		}
	})
}
